import std/[times, algorithm, strutils, streams, options, sequtils, random, os]
import chronos
import chronos/timer as ctimer
import chronos/threadsync
import taskpools
import leveldbstatic as leveldb

import ./errors
import ./cid
import ./blocks as blk
import ./merkle
import ./manifest
import ./repostore
import ./serialization as ser
import ./sharding
import ./ioutils
import ./blockmap
import ./chunker

const
  DatasetMetadataPrefix = "dataset_metadata:"
  DatasetBlocksPrefix = "dataset_blocks:"
  BlockmapsPrefix = "blockmaps:"
  ManifestsPrefix = "manifests:"
  MerkleBackendConfigKey = "blockstore_config:merkle_backend"
  BlockBackendConfigKey = "blockstore_config:block_backend"
  BlockmapBackendConfigKey = "blockstore_config:blockmap_backend"

  DeletionBatchSize = 100
  DefaultDeletionPoolSize = 2

let DefaultDeletionWorkerInterval* = ctimer.milliseconds(100)

type
  DeletionTask = object
    path: string
    cidStr: string
    size: uint64

  DeletionBatchResult = object
    deletedCids: seq[string]
    totalFreed: uint64
    count: int

  BlockBackend* = enum
    bbSharded
    bbPacked

  DatasetMetadata = object
    treeCid: string
    manifestCid: string
    lastAccess: uint64
    size: uint64
    blockCount: int
    chunkSize: uint32
    treeId: string
    merkleBackend: uint8
    blockBackend: uint8
    blockmapBackend: uint8

  BlockRef = object
    blockCid: string
    proof: MerkleProof

  BlockRefSimple = object
    blockCid: string

  Dataset* = ref object
    treeCid*: Cid
    manifestCid*: Cid
    blockCount*: int
    chunkSize: uint32
    totalSize: uint64
    treeId: string
    repo: RepoStore
    db: LevelDb
    blockmapBackend: BlockmapBackend
    blockmapSeq: seq[byte]
    blockmapPending: seq[int]
    blockmapFile: FileBlockmap
    merkleBackend: MerkleBackend
    merkleReader: MerkleReader
    treesDir: string
    blockBackend: BlockBackend
    dataDir: string
    dataFile: File

  PendingDeletionWorker* = ref object
    store: DatasetStore
    running: bool
    task: Future[void]
    interval: ctimer.Duration

  DatasetStore* = ref object
    repo*: RepoStore
    db: LevelDb
    blockHashConfig*: BlockHashConfig
    merkleBackend*: MerkleBackend
    blockBackend*: BlockBackend
    blockmapBackend*: BlockmapBackend
    ioMode*: IOMode
    syncBatchSize*: int
    treesDir*: string
    dataDir*: string
    blockmapsDir*: string
    deletionWorker: PendingDeletionWorker
    deletionPool: Taskpool
    ownsDeletionPool: bool

  DatasetBuilder* = ref object
    chunkSize: uint32
    filename: Option[string]
    mimetype: Option[string]
    blockHashConfig: BlockHashConfig
    merkleBuilder: MerkleTreeBuilder
    blockCids: seq[Cid]
    streamingBuilder: StreamingMerkleBuilder
    merkleStorage: MerkleStorage
    treeId: string
    merkleBackend: MerkleBackend
    blockIndex: int
    totalSize: uint64
    store: DatasetStore
    blockBackend: BlockBackend
    blockmapBackend: BlockmapBackend
    ioMode: IOMode
    writeHandle: WriteHandle

  LruIterator* = ref object
    datasets: seq[Cid]
    index: int

proc hasBlock(dataset: Dataset, index: int): bool {.inline.} =
  case dataset.blockmapBackend
  of bmLevelDb:
    blockmapGet(dataset.blockmapSeq, index)
  of bmFile:
    dataset.blockmapFile.get(index.uint64)

proc markBlock(dataset: Dataset, index: int, value: bool): BResult[void] {.inline.} =
  case dataset.blockmapBackend
  of bmLevelDb:
    blockmapSet(dataset.blockmapSeq, index, value)
    ok()
  of bmFile:
    if value:
      ?dataset.blockmapFile.set(index.uint64)
    else:
      ?dataset.blockmapFile.clear(index.uint64)
    ok()

proc close*(dataset: Dataset) =
  case dataset.blockmapBackend
  of bmLevelDb:
    discard
  of bmFile:
    if dataset.blockmapFile != nil:
      dataset.blockmapFile.close()
  if dataset.dataFile != nil:
    dataset.dataFile.close()
  if dataset.merkleReader != nil:
    dataset.merkleReader.close()

proc writeDatasetMetadata(s: Stream, m: DatasetMetadata) {.gcsafe.} =
  ser.writeString(s, m.treeCid)
  ser.writeString(s, m.manifestCid)
  ser.writeUint64(s, m.lastAccess)
  ser.writeUint64(s, m.size)
  ser.writeUint64(s, m.blockCount.uint64)
  ser.writeUint32(s, m.chunkSize)
  ser.writeString(s, m.treeId)
  ser.writeUint8(s, m.merkleBackend)
  ser.writeUint8(s, m.blockBackend)
  ser.writeUint8(s, m.blockmapBackend)

proc readDatasetMetadata(s: Stream): BResult[DatasetMetadata] {.gcsafe.} =
  var m: DatasetMetadata
  m.treeCid = ?ser.readString(s)
  m.manifestCid = ?ser.readString(s)
  m.lastAccess = ser.readUint64(s)
  m.size = ser.readUint64(s)
  m.blockCount = ser.readUint64(s).int
  m.chunkSize = ser.readUint32(s)
  m.treeId = ?ser.readString(s)
  m.merkleBackend = ser.readUint8(s)
  m.blockBackend = ser.readUint8(s)
  m.blockmapBackend = ser.readUint8(s)
  ok(m)

proc serializeDatasetMetadata(m: DatasetMetadata): BResult[seq[byte]] =
  ser.toBytes(m, writeDatasetMetadata)

proc deserializeDatasetMetadata(data: openArray[byte]): BResult[DatasetMetadata] =
  ser.fromBytesResult(data, readDatasetMetadata)

proc writeBlockRefSimple(s: Stream, r: BlockRefSimple) {.gcsafe.} =
  ser.writeString(s, r.blockCid)

proc readBlockRefSimple(s: Stream): BResult[BlockRefSimple] {.gcsafe.} =
  var r: BlockRefSimple
  r.blockCid = ?ser.readString(s)
  ok(r)

proc serializeBlockRefSimple(r: BlockRefSimple): BResult[seq[byte]] =
  ser.toBytes(r, writeBlockRefSimple)

proc deserializeBlockRefSimple(data: openArray[byte]): BResult[BlockRefSimple] =
  ser.fromBytesResult(data, readBlockRefSimple)

proc writeMerkleProofNode(s: Stream, node: MerkleProofNode) {.gcsafe.} =
  s.write(node.hash)
  ser.writeUint32(s, node.level.uint32)

proc readMerkleProofNode(s: Stream): BResult[MerkleProofNode] {.gcsafe.} =
  var node: MerkleProofNode
  let bytesRead = s.readData(addr node.hash[0], HashSize)
  if bytesRead != HashSize:
    return err(ioError("Failed to read " & $HashSize & " bytes, got " & $bytesRead))
  node.level = ser.readUint32(s).int
  ok(node)

proc writeStreamingMerkleProof(s: Stream, proof: MerkleProof) {.gcsafe.} =
  ser.writeUint64(s, proof.index)
  ser.writeUint64(s, proof.path.len.uint64)
  for node in proof.path:
    writeMerkleProofNode(s, node)
  ser.writeUint64(s, proof.leafCount)

proc readStreamingMerkleProof(s: Stream): BResult[MerkleProof] {.gcsafe.} =
  var proof: MerkleProof
  proof.index = ser.readUint64(s)
  let pathLen = ser.readUint64(s).int
  proof.path = newSeq[MerkleProofNode](pathLen)
  for i in 0 ..< pathLen:
    proof.path[i] = ?readMerkleProofNode(s)
  proof.leafCount = ser.readUint64(s)
  ok(proof)

proc writeBlockRef(s: Stream, r: BlockRef) {.gcsafe.} =
  ser.writeString(s, r.blockCid)
  writeStreamingMerkleProof(s, r.proof)

proc readBlockRef(s: Stream): BResult[BlockRef] {.gcsafe.} =
  var r: BlockRef
  r.blockCid = ?ser.readString(s)
  r.proof = ?readStreamingMerkleProof(s)
  ok(r)

proc serializeBlockRef(r: BlockRef): BResult[seq[byte]] =
  ser.toBytes(r, writeBlockRef)

proc deserializeBlockRef(data: openArray[byte]): BResult[BlockRef] =
  ser.fromBytesResult(data, readBlockRef)

proc datasetMetadataKey(treeCid: string): string {.inline.} =
  DatasetMetadataPrefix & treeCid

proc datasetBlockKey(treeId: string, index: int): string {.inline.} =
  DatasetBlocksPrefix & treeId & ":" & align($index, 10, '0')

proc blockmapKey(treeCid: string): string {.inline.} =
  BlockmapsPrefix & treeCid

proc manifestKey(manifestCid: string): string {.inline.} =
  ManifestsPrefix & manifestCid

proc generateTreeId(): string =
  var r = initRand()
  result = ""
  for i in 0 ..< 16:
    result.add(char(r.rand(25) + ord('a')))

proc finalizeTreeFile(store: DatasetStore, tempPath: string, treeCid: Cid): BResult[void] =
  let finalPath = getShardedPath(store.treesDir, treeCid, ".tree")
  ?atomicRename(tempPath, finalPath)
  ok()

proc finalizeDataFile(store: DatasetStore, tempPath: string, treeCid: Cid): BResult[void] =
  let finalPath = getShardedPath(store.dataDir, treeCid, ".data")
  ?atomicRename(tempPath, finalPath)
  ok()

proc finalizeBlockmapFile(store: DatasetStore, tempPath: string, treeCid: Cid): BResult[void] =
  let finalPath = getBlockmapPath(store.blockmapsDir, treeCid)
  ?atomicRename(tempPath, finalPath)
  ok()

proc deleteBatchWorker(tasks: ptr seq[DeletionTask], result: ptr DeletionBatchResult,
                       signal: ThreadSignalPtr) {.gcsafe.} =
  result[].deletedCids = @[]
  result[].totalFreed = 0
  result[].count = 0

  for task in tasks[]:
    if fileExists(task.path):
      try:
        removeFile(task.path)
        result[].deletedCids.add(task.cidStr)
        result[].totalFreed += task.size
        result[].count += 1
      except OSError:
        discard
    else:
      # TODO: cnanakos: file already gone??? still mark as deleted
      result[].deletedCids.add(task.cidStr)
      result[].count += 1

  discard signal.fireSync()

proc processPendingDeletions*(store: DatasetStore): Future[BResult[int]] {.async.} =
  if store.deletionPool == nil:
    return ok(0)

  let pendingResult = store.repo.getPendingDeletions(DeletionBatchSize)
  if pendingResult.isErr:
    return err(pendingResult.error)

  let pending = pendingResult.value
  if pending.len == 0:
    return ok(0)

  var locks: seq[(string, CidLock)] = @[]
  for (cidStr, _) in pending:
    let cl = await store.repo.acquireCidLock(cidStr)
    locks.add((cidStr, cl))

  proc releaseAllLocks() =
    for (cidStr, cl) in locks:
      store.repo.releaseCidLock(cl, cidStr)

  var
    tasksPtr = cast[ptr seq[DeletionTask]](alloc0(sizeof(seq[DeletionTask])))
    resultPtr = cast[ptr DeletionBatchResult](alloc0(sizeof(DeletionBatchResult)))

  tasksPtr[] = @[]
  for (cidStr, pd) in pending:
    tasksPtr[].add(DeletionTask(path: pd.blockPath, cidStr: cidStr, size: pd.size))

  let signalResult = ThreadSignalPtr.new()
  if signalResult.isErr:
    dealloc(tasksPtr)
    dealloc(resultPtr)
    releaseAllLocks()
    return err(ioError("Failed to create thread signal"))

  let signal = signalResult.get()

  store.deletionPool.spawn deleteBatchWorker(tasksPtr, resultPtr, signal)

  try:
    await signal.wait()
  except AsyncError as e:
    discard signal.close()
    dealloc(tasksPtr)
    dealloc(resultPtr)
    releaseAllLocks()
    return err(ioError("Async error waiting for deletion: " & e.msg))
  except CancelledError:
    discard signal.close()
    dealloc(tasksPtr)
    dealloc(resultPtr)
    releaseAllLocks()
    return err(ioError("Deletion cancelled"))

  discard signal.close()

  let
    totalFreed = resultPtr[].totalFreed
    deletedCids = resultPtr[].deletedCids
    count = resultPtr[].count

  dealloc(tasksPtr)
  dealloc(resultPtr)

  releaseAllLocks()

  if totalFreed > 0:
    store.repo.decreaseUsed(totalFreed)

  if deletedCids.len > 0:
    ?store.repo.removePendingDeletionsBatch(deletedCids)

  return ok(count)

proc deletionWorkerLoop(worker: PendingDeletionWorker) {.async.} =
  while worker.running:
    discard await worker.store.processPendingDeletions()
    await sleepAsync(worker.interval)

proc startDeletionWorker*(store: DatasetStore, interval: ctimer.Duration = DefaultDeletionWorkerInterval) =
  if store.deletionWorker != nil and store.deletionWorker.running:
    return

  store.deletionWorker = PendingDeletionWorker(
    store: store,
    running: true,
    interval: interval
  )
  store.deletionWorker.task = deletionWorkerLoop(store.deletionWorker)

proc stopDeletionWorker*(store: DatasetStore) {.async.} =
  if store.deletionWorker == nil or not store.deletionWorker.running:
    return

  store.deletionWorker.running = false
  if store.deletionWorker.task != nil:
    try:
      await store.deletionWorker.task
    except CancelledError:
      discard
  store.deletionWorker = nil

proc newDatasetStore*(dbPath: string, blocksDir: string, quota: uint64 = 0,
                      blockHashConfig: BlockHashConfig = defaultBlockHashConfig(),
                      merkleBackend: MerkleBackend = mbPacked,
                      blockBackend: BlockBackend = bbSharded,
                      blockmapBackend: BlockmapBackend = bmLevelDb,
                      ioMode: IOMode = ioDirect,
                      syncBatchSize: int = 0,
                      pool: Taskpool = nil): BResult[DatasetStore] =
  var db: LevelDb
  try:
    db = leveldb.open(dbPath)
  except LevelDbException as e:
    return err(databaseError(e.msg))

  try:
    let existingConfig = db.get(MerkleBackendConfigKey)
    if existingConfig.isSome:
      let storedBackend = MerkleBackend(parseInt(existingConfig.get()))
      if storedBackend != merkleBackend:
        db.close()
        return err(backendMismatchError(
          "Repository merkle backend was " & $storedBackend & " but " & $merkleBackend & " was requested"))
    else:
      db.put(MerkleBackendConfigKey, $ord(merkleBackend))
  except LevelDbException as e:
    db.close()
    return err(databaseError(e.msg))
  except ValueError as e:
    db.close()
    return err(databaseError("Invalid merkle backend config: " & e.msg))

  try:
    let existingConfig = db.get(BlockBackendConfigKey)
    if existingConfig.isSome:
      let storedBackend = BlockBackend(parseInt(existingConfig.get()))
      if storedBackend != blockBackend:
        db.close()
        return err(backendMismatchError(
          "Repository block backend was " & $storedBackend & " but " & $blockBackend & " was requested"))
    else:
      db.put(BlockBackendConfigKey, $ord(blockBackend))
  except LevelDbException as e:
    db.close()
    return err(databaseError(e.msg))
  except ValueError as e:
    db.close()
    return err(databaseError("Invalid block backend config: " & e.msg))

  try:
    let existingConfig = db.get(BlockmapBackendConfigKey)
    if existingConfig.isSome:
      let storedBackend = BlockmapBackend(parseInt(existingConfig.get()))
      if storedBackend != blockmapBackend:
        db.close()
        return err(backendMismatchError(
          "Repository blockmap backend was " & $storedBackend & " but " & $blockmapBackend & " was requested"))
    else:
      db.put(BlockmapBackendConfigKey, $ord(blockmapBackend))
  except LevelDbException as e:
    db.close()
    return err(databaseError(e.msg))
  except ValueError as e:
    db.close()
    return err(databaseError("Invalid blockmap backend config: " & e.msg))

  let
    parentDir = parentDir(dbPath)
    treesDir = parentDir / "trees"
    dataDir = parentDir / "data"
    blockmapsDir = parentDir / "blockmaps"

  if merkleBackend == mbPacked:
    let shardResult = initShardDirectories(treesDir)
    if shardResult.isErr:
      db.close()
      return err(shardResult.error)
    cleanupTmpDir(treesDir)

  if blockBackend == bbPacked:
    let shardResult = initShardDirectories(dataDir)
    if shardResult.isErr:
      db.close()
      return err(shardResult.error)
    cleanupTmpDir(dataDir)

  if blockmapBackend == bmFile:
    let shardResult = initShardDirectories(blockmapsDir)
    if shardResult.isErr:
      db.close()
      return err(shardResult.error)
    cleanupTmpDir(blockmapsDir)

  let repo = ?newRepoStore(blocksDir, db, quota, ioMode, syncBatchSize)

  var
    ownsDeletionPool = false
    deletionPool: Taskpool
  if pool == nil:
    deletionPool = Taskpool.new(numThreads = DefaultDeletionPoolSize)
    ownsDeletionPool = true
  else:
    deletionPool = pool

  var store = DatasetStore(
    repo: repo,
    db: db,
    blockHashConfig: blockHashConfig,
    merkleBackend: merkleBackend,
    blockBackend: blockBackend,
    blockmapBackend: blockmapBackend,
    ioMode: ioMode,
    syncBatchSize: syncBatchSize,
    treesDir: treesDir,
    dataDir: dataDir,
    blockmapsDir: blockmapsDir,
    deletionPool: deletionPool,
    ownsDeletionPool: ownsDeletionPool
  )

  store.startDeletionWorker()

  ok(store)

proc closeAsync*(store: DatasetStore) {.async.} =
  await store.stopDeletionWorker()
  # cnanakos: We intentionally don't call deletionPool.shutdown() here.
  # Taskpools uses global static variables that can conflict with other
  # taskpool instances (e.g., nimble). The pool threads will be cleaned
  # up when the process exits afterall or the caller will handle the taskpools.
  store.repo.close()
  store.db.close()

proc close*(store: DatasetStore) =
  waitFor store.closeAsync()

proc used*(store: DatasetStore): uint64 =
  store.repo.used()

proc quota*(store: DatasetStore): uint64 =
  store.repo.quota()

proc getRepo*(store: DatasetStore): RepoStore =
  store.repo

proc getManifest*(store: DatasetStore, manifestCid: Cid): Future[BResult[Option[Manifest]]] {.async.} =
  try:
    let
      key = manifestKey($manifestCid)
      valueOpt= store.db.get(key)
    if valueOpt.isNone:
      return ok(none(Manifest))

    let manifest = ?decodeManifest(cast[seq[byte]](valueOpt.get))
    return ok(some(manifest))
  except LevelDbException as e:
    return err(databaseError(e.msg))

proc getDataset*(store: DatasetStore, treeCid: Cid): Future[BResult[Option[Dataset]]] {.async.} =
  try:
    let
      metaKey = datasetMetadataKey($treeCid)
      metaValueOpt = store.db.get(metaKey)
    if metaValueOpt.isNone:
      return ok(none(Dataset))

    let
      meta = ?deserializeDatasetMetadata(cast[seq[byte]](metaValueOpt.get))
      manifestCid = ?cidFromString(meta.manifestCid)
      merkleBackend = MerkleBackend(meta.merkleBackend)
      blockBackend = BlockBackend(meta.blockBackend)
      blockmapBackend = BlockmapBackend(meta.blockmapBackend)

    var
      blockmapSeq: seq[byte]
      blockmapFile: FileBlockmap = nil

    case blockmapBackend
    of bmLevelDb:
      let
        bmKey = blockmapKey($treeCid)
        bmValueOpt = store.db.get(bmKey)
      if bmValueOpt.isSome and bmValueOpt.get().len > 0:
        blockmapSeq = cast[seq[byte]](bmValueOpt.get)
      else:
        blockmapSeq = newBlockmap(meta.blockCount)
    of bmFile:
      let bmPath = getBlockmapPath(store.blockmapsDir, treeCid)
      blockmapFile = ?newFileBlockmap(bmPath, forWriting = true)

    var merkleReader: MerkleReader = nil
    case merkleBackend
    of mbEmbeddedProofs:
      discard
    of mbLevelDb:
      let storage = newLevelDbMerkleStorage(store.db, MerkleTreePrefix & meta.treeId)
      merkleReader = newMerkleReader(storage)
    of mbPacked:
      let
        treePath = getShardedPath(store.treesDir, treeCid, ".tree")
        storage = ?newPackedMerkleStorage(treePath, forWriting = false)
      merkleReader = newMerkleReader(storage)

    var dataFile: File = nil
    case blockBackend
    of bbSharded:
      discard
    of bbPacked:
      let dataPath = getShardedPath(store.dataDir, treeCid, ".data")
      dataFile = open(dataPath, fmRead)

    var dataset = Dataset(
      treeCid: treeCid,
      manifestCid: manifestCid,
      blockCount: meta.blockCount,
      chunkSize: meta.chunkSize,
      totalSize: meta.size,
      treeId: meta.treeId,
      repo: store.repo,
      db: store.db,
      blockmapBackend: blockmapBackend,
      blockmapSeq: blockmapSeq,
      blockmapFile: blockmapFile,
      merkleBackend: merkleBackend,
      merkleReader: merkleReader,
      treesDir: store.treesDir,
      blockBackend: blockBackend,
      dataDir: store.dataDir,
      dataFile: dataFile
    )

    return ok(some(dataset))
  except LevelDbException as e:
    return err(databaseError(e.msg))
  except IOError as e:
    return err(ioError(e.msg))

proc createDataset*(store: DatasetStore, manifest: Manifest): Future[BResult[Dataset]] {.async.} =
  let
    manifestCid = ?manifest.toCid()
    treeCid = ?cidFromBytes(manifest.treeCid)
    blockCount = manifest.blocksCount()

  let existingOpt = ?await store.getDataset(treeCid)
  if existingOpt.isSome:
    return ok(existingOpt.get)

  let
    now = epochTime().uint64
    treeId = generateTreeId()

  var
    blockmapSeq: seq[byte] = @[]
    blockmapFile: FileBlockmap = nil

  case store.blockmapBackend
  of bmLevelDb:
    blockmapSeq = newBlockmap(blockCount)
  of bmFile:
    let bmPath = getBlockmapPath(store.blockmapsDir, treeCid)
    blockmapFile = ?newFileBlockmap(bmPath, forWriting = true)

  let meta = DatasetMetadata(
    treeCid: $treeCid,
    manifestCid: $manifestCid,
    lastAccess: now,
    size: manifest.datasetSize,
    blockCount: blockCount,
    chunkSize: manifest.blockSize,
    treeId: treeId,
    merkleBackend: uint8(store.merkleBackend),
    blockBackend: uint8(store.blockBackend),
    blockmapBackend: uint8(store.blockmapBackend)
  )

  let
    manifestBytes = ?encodeManifest(manifest)
    metaBytes = ?serializeDatasetMetadata(meta)

  try:
    let batch = newBatch()
    batch.put(datasetMetadataKey($treeCid), cast[string](metaBytes))
    if store.blockmapBackend == bmLevelDb:
      batch.put(blockmapKey($treeCid), cast[string](blockmapSeq))
    batch.put(manifestKey($manifestCid), cast[string](manifestBytes))
    store.db.write(batch)

    var dataset = Dataset(
      treeCid: treeCid,
      manifestCid: manifestCid,
      blockCount: blockCount,
      chunkSize: manifest.blockSize,
      totalSize: manifest.datasetSize,
      treeId: treeId,
      repo: store.repo,
      db: store.db,
      blockmapBackend: store.blockmapBackend,
      blockmapSeq: blockmapSeq,
      blockmapFile: blockmapFile,
      merkleBackend: store.merkleBackend,
      blockBackend: store.blockBackend
    )

    return ok(dataset)
  except LevelDbException as e:
    return err(databaseError(e.msg))

proc deleteDataset*(store: DatasetStore, manifestCid: Cid): Future[BResult[void]] {.async.} =
  let manifestOpt = ?await store.getManifest(manifestCid)
  if manifestOpt.isNone:
    return err(datasetNotFoundError())

  let
    manifest = manifestOpt.get
    treeCid = ?cidFromBytes(manifest.treeCid)
    treeCidStr = $treeCid

  var treeId: string
  try:
    let
      metaKey = datasetMetadataKey(treeCidStr)
      metaValueOpt = store.db.get(metaKey)
    if metaValueOpt.isSome:
      let meta = ?deserializeDatasetMetadata(cast[seq[byte]](metaValueOpt.get))
      treeId = meta.treeId
    else:
      treeId = ""
  except LevelDbException as e:
    return err(databaseError(e.msg))

  try:
    let batch = newBatch()
    batch.delete(manifestKey($manifestCid))
    batch.delete(datasetMetadataKey(treeCidStr))
    batch.delete(blockmapKey(treeCidStr))
    store.db.write(batch)
  except LevelDbException as e:
    return err(databaseError(e.msg))

  if treeId.len > 0:
    const batchSize = 1000
    let prefix = DatasetBlocksPrefix & treeId & ":"

    while true:
      var batch: seq[(string, Cid)] = @[]
      try:
        for key, value in store.db.iter():
          if not key.startsWith(prefix):
            if key > prefix:
              break
            continue

          let blockRefSimpleResult = deserializeBlockRefSimple(cast[seq[byte]](value))
          if blockRefSimpleResult.isOk:
            let blockCidResult = cidFromString(blockRefSimpleResult.value.blockCid)
            if blockCidResult.isOk:
              batch.add((key, blockCidResult.value))
              if batch.len >= batchSize:
                break
          else:
            let blockRefResult = deserializeBlockRef(cast[seq[byte]](value))
            if blockRefResult.isOk:
              let blockCidResult = cidFromString(blockRefResult.value.blockCid)
              if blockCidResult.isOk:
                batch.add((key, blockCidResult.value))
                if batch.len >= batchSize:
                  break
      except LevelDbException as e:
        return err(databaseError(e.msg))

      if batch.len == 0:
        break

      if store.blockBackend == bbSharded:
        for (_, c) in batch:
          discard store.repo.releaseBlock(c)

      try:
        let dbBatch = newBatch()
        for (key, _) in batch:
          dbBatch.delete(key)
        store.db.write(dbBatch)
      except LevelDbException as e:
        return err(databaseError(e.msg))

  if store.merkleBackend == mbPacked:
    let treePath = getShardedPathStr(store.treesDir, treeCidStr, ".tree")
    if fileExists(treePath):
      removeFile(treePath)

  if store.blockBackend == bbPacked:
    let dataPath = getShardedPathStr(store.dataDir, treeCidStr, ".data")
    if fileExists(dataPath):
      removeFile(dataPath)

  if store.blockmapBackend == bmFile:
    let bmPath = getBlockmapPathStr(store.blockmapsDir, treeCidStr)
    if fileExists(bmPath):
      removeFile(bmPath)

  return ok()

proc startDataset*(store: DatasetStore, chunkSize: uint32, filename: Option[string] = none(string)): BResult[DatasetBuilder] =
  ?validateChunkSize(chunkSize)

  let treeId = generateTreeId()

  var builder = DatasetBuilder(
    chunkSize: chunkSize,
    filename: filename,
    mimetype: none(string),
    blockHashConfig: store.blockHashConfig,
    merkleBackend: store.merkleBackend,
    blockBackend: store.blockBackend,
    blockmapBackend: store.blockmapBackend,
    ioMode: store.ioMode,
    treeId: treeId,
    blockIndex: 0,
    blockCids: @[],
    totalSize: 0,
    store: store
  )

  case store.merkleBackend
  of mbEmbeddedProofs:
    builder.merkleBuilder = newMerkleTreeBuilder()
  of mbLevelDb:
    builder.merkleStorage = newLevelDbMerkleStorage(store.db, MerkleTreePrefix & treeId)
    builder.streamingBuilder = newStreamingMerkleBuilder(builder.merkleStorage)
  of mbPacked:
    let treePath = getTmpPath(store.treesDir, treeId, ".tree")
    builder.merkleStorage = ?newPackedMerkleStorage(treePath, forWriting = true)
    builder.streamingBuilder = newStreamingMerkleBuilder(builder.merkleStorage)

  case store.blockBackend
  of bbSharded:
    discard
  of bbPacked:
    let
      dataPath = getTmpPath(store.dataDir, treeId, ".data")
      handleResult = ioutils.openForWrite(dataPath, store.ioMode, chunkSize.int, syncNone())
    if handleResult.isErr:
      return err(handleResult.error)
    builder.writeHandle = handleResult.value

  ok(builder)

proc lru*(store: DatasetStore): BResult[LruIterator] =
  var datasets: seq[(Cid, uint64)] = @[]

  try:
    for key, value in store.db.iter():
      if not key.startsWith(DatasetMetadataPrefix):
        continue

      let metaResult = deserializeDatasetMetadata(cast[seq[byte]](value))
      if metaResult.isOk:
        let
          cidStr = key[DatasetMetadataPrefix.len .. ^1]
          cidResult = cidFromString(cidStr)
        if cidResult.isOk:
          datasets.add((cidResult.value, metaResult.value.lastAccess))

    datasets.sort(proc(a, b: (Cid, uint64)): int = cmp(a[1], b[1]))

    ok(LruIterator(
      datasets: datasets.mapIt(it[0]),
      index: 0
    ))
  except LevelDbException as e:
    err(databaseError(e.msg))

proc filterPresent*(dataset: Dataset, indices: openArray[int]): seq[int] =
  result = @[]
  for i in indices:
    if dataset.hasBlock(i):
      result.add(i)

proc getBlockmapRanges*(dataset: Dataset): seq[BlockRange] =
  case dataset.blockmapBackend
  of bmLevelDb:
    result = @[]
    var inRange = false
    var rangeStart: uint64 = 0
    for i in 0 ..< dataset.blockCount:
      let present = blockmapGet(dataset.blockmapSeq, i)
      if present and not inRange:
        rangeStart = i.uint64
        inRange = true
      elif not present and inRange:
        result.add(BlockRange(start: rangeStart, count: i.uint64 - rangeStart))
        inRange = false
    if inRange:
      result.add(BlockRange(start: rangeStart, count: dataset.blockCount.uint64 - rangeStart))
  of bmFile:
    result = dataset.blockmapFile.toRanges()

proc completed*(dataset: Dataset): int =
  case dataset.blockmapBackend
  of bmLevelDb:
    blockmapCountOnes(dataset.blockmapSeq)
  of bmFile:
    dataset.blockmapFile.countOnes().int

proc touch(dataset: Dataset): BResult[void] =
  let now = epochTime().uint64

  try:
    let key = datasetMetadataKey($dataset.treeCid)
    let valueOpt = dataset.db.get(key)
    if valueOpt.isNone:
      return ok()

    var meta = ?deserializeDatasetMetadata(cast[seq[byte]](valueOpt.get))
    meta.lastAccess = now
    let metaBytes = ?serializeDatasetMetadata(meta)
    dataset.db.put(key, cast[string](metaBytes))

    ok()
  except LevelDbException as e:
    err(databaseError(e.msg))

proc saveBlockmap(dataset: Dataset): BResult[void] =
  case dataset.blockmapBackend
  of bmLevelDb:
    try:
      dataset.db.put(blockmapKey($dataset.treeCid), cast[string](dataset.blockmapSeq))
      ok()
    except LevelDbException as e:
      err(databaseError(e.msg))
  of bmFile:
    dataset.blockmapFile.flush()
    ok()

proc putBlock*(dataset: Dataset, b: blk.Block, index: int, proof: MerkleProof): Future[BResult[void]] {.async.} =
  if index < 0 or index >= dataset.blockCount:
    return err(invalidBlockError())

  if dataset.hasBlock(index):
    return ok()

  if proof.leafCount != uint64(dataset.blockCount):
    return err(invalidProofError())

  let (_, synced) = ?await dataset.repo.putBlock(b)

  let key = datasetBlockKey(dataset.treeId, index)

  var blockRefBytes: seq[byte]
  case dataset.merkleBackend
  of mbEmbeddedProofs:
    let blockRef = BlockRef(blockCid: $b.cid, proof: proof)
    blockRefBytes = ?serializeBlockRef(blockRef)
  of mbLevelDb, mbPacked:
    let blockRef = BlockRefSimple(blockCid: $b.cid)
    blockRefBytes = ?serializeBlockRefSimple(blockRef)

  try:
    dataset.db.put(key, cast[string](blockRefBytes))

    # Only update the blockmap when blocks are synced (or
    # we've disabled consistency) or we may report blocks
    # we don't have.
    if synced or dataset.repo.syncBatchSize == 0:
      for index in dataset.blockmapPending:
        ?dataset.markBlock(index, true)

      ?dataset.saveBlockmap()
      dataset.blockmapPending = @[]
    else:
      dataset.blockmapPending.add(index)

    return ok()
  except LevelDbException as e:
    return err(databaseError(e.msg))

proc getBlock*(dataset: Dataset, index: int): Future[BResult[Option[(blk.Block, MerkleProof)]]] {.async.} =
  if index < 0 or index >= dataset.blockCount:
    return err(invalidBlockError())

  if not dataset.hasBlock(index):
    return ok(none((blk.Block, MerkleProof)))

  try:
    let
      key = datasetBlockKey(dataset.treeId, index)
      valueOpt = dataset.db.get(key)
    if valueOpt.isNone:
      return ok(none((blk.Block, MerkleProof)))

    var
      blockCid: Cid
      proof: MerkleProof

    case dataset.merkleBackend
    of mbEmbeddedProofs:
      let blockRef = ?deserializeBlockRef(cast[seq[byte]](valueOpt.get))
      blockCid = ?cidFromString(blockRef.blockCid)
      proof = blockRef.proof
    of mbLevelDb, mbPacked:
      let blockRef = ?deserializeBlockRefSimple(cast[seq[byte]](valueOpt.get))
      blockCid = ?cidFromString(blockRef.blockCid)
      if dataset.merkleReader.isNil:
        return err(merkleTreeError("Merkle reader not initialized"))
      try:
        proof = ?dataset.merkleReader.getProof(uint64(index))
      except CatchableError as e:
        return err(merkleTreeError("Failed to get proof: " & e.msg))
      except Exception as e:
        return err(merkleTreeError("Failed to get proof: " & e.msg))

    var b: blk.Block
    case dataset.blockBackend
    of bbSharded:
      let blockOpt = ?await dataset.repo.getBlock(blockCid)
      if blockOpt.isNone:
        return ok(none((blk.Block, MerkleProof)))
      b = blockOpt.get
    of bbPacked:
      let
        offset = int64(index) * int64(dataset.chunkSize)
        isLastBlock = (index == dataset.blockCount - 1)
      let blockSize = if isLastBlock:
        let remainder = dataset.totalSize mod dataset.chunkSize.uint64
        if remainder == 0: dataset.chunkSize.int else: remainder.int
      else:
        dataset.chunkSize.int

      var data = newSeq[byte](blockSize)
      dataset.dataFile.setFilePos(offset)
      let bytesRead = dataset.dataFile.readBytes(data, 0, blockSize)
      if bytesRead != blockSize:
        return err(ioError("Failed to read complete block from packed file"))

      b = blk.fromCidUnchecked(blockCid, data)

    return ok(some((b, proof)))
  except LevelDbException as e:
    return err(databaseError(e.msg))
  except IOError as e:
    return err(ioError("Failed to read block: " & e.msg))
  except CatchableError as e:
    return err(merkleTreeError("Failed to get proof: " & e.msg))

proc setFilename*(builder: DatasetBuilder, filename: string) =
  builder.filename = some(filename)

proc setMimetype*(builder: DatasetBuilder, mimetype: string) =
  builder.mimetype = some(mimetype)

proc newChunker*(builder: DatasetBuilder, pool: Taskpool): BResult[AsyncChunker] =
  if builder.filename.isSome:
    return err(invalidOperationError("Cannot use newChunker when filename is set. Use chunkFile instead."))
  let config = newChunkerConfig(builder.chunkSize.int)
  ok(newAsyncChunker(pool, config))

proc chunkFile*(builder: DatasetBuilder, pool: Taskpool): Future[BResult[AsyncChunkStream]] {.async.} =
  if builder.filename.isNone:
    return err(invalidOperationError("Cannot use chunkFile without filename. Use newChunker instead."))
  let config = newChunkerConfig(builder.chunkSize.int)
  let chunker = newAsyncChunker(pool, config)
  return await chunker.chunkFile(builder.filename.get())

proc addBlock*(builder: DatasetBuilder, b: blk.Block): Future[BResult[int]] {.async.} =
  let
    blockSize = b.data.len.uint64
    index = builder.blockIndex

  case builder.merkleBackend
  of mbEmbeddedProofs:
    builder.merkleBuilder.addBlock(b.data)
    builder.blockCids.add(b.cid)
  of mbLevelDb, mbPacked:
    let leafHash = builder.blockHashConfig.hashFunc(b.data)
    ?builder.streamingBuilder.addLeaf(leafHash)

    let
      key = datasetBlockKey(builder.treeId, index)
      blockRef = BlockRefSimple(blockCid: $b.cid)
      blockRefBytes = ?serializeBlockRefSimple(blockRef)
    try:
      builder.store.db.put(key, cast[string](blockRefBytes))
    except LevelDbException as e:
      return err(databaseError("Failed to write block ref: " & e.msg))

  case builder.blockBackend
  of bbSharded:
    discard ?await builder.store.repo.putBlock(b)
  of bbPacked:
    let writeResult = builder.writeHandle.writeBlock(b.data)
    if writeResult.isErr:
      return err(writeResult.error)

  builder.blockIndex += 1
  builder.totalSize += blockSize

  return ok(index)

proc finalize*(builder: DatasetBuilder): Future[BResult[Dataset]] {.async.} =
  let blockCount = builder.blockIndex

  if blockCount == 0:
    return err(invalidBlockError())

  var treeCid: Cid
  case builder.merkleBackend
  of mbEmbeddedProofs:
    builder.merkleBuilder.buildTree()
    treeCid = ?builder.merkleBuilder.rootCid()
  of mbLevelDb, mbPacked:
    try:
      let rootHash = ?builder.streamingBuilder.finalize()
      treeCid = ?rootToCid(rootHash, builder.blockHashConfig.hashCode, builder.blockHashConfig.treeCodec)
    except CatchableError as e:
      return err(merkleTreeError("Failed to finalize: " & e.msg))
    except Exception as e:
      return err(merkleTreeError("Failed to finalize: " & e.msg))

  var merkleReader: MerkleReader = nil
  if builder.merkleBackend == mbPacked:
    ?builder.merkleStorage.close()
    let tempTreePath = getTmpPath(builder.store.treesDir, builder.treeId, ".tree")
    ?builder.store.finalizeTreeFile(tempTreePath, treeCid)
    let
      finalTreePath = getShardedPath(builder.store.treesDir, treeCid, ".tree")
      storage = ?newPackedMerkleStorage(finalTreePath, forWriting = false)
    merkleReader = newMerkleReader(storage)
  elif builder.merkleBackend == mbLevelDb:
    merkleReader = newMerkleReader(builder.merkleStorage)

  var dataFile: File = nil
  if builder.blockBackend == bbPacked:
    let finalizeResult = builder.writeHandle.finalize(builder.totalSize.int64)
    if finalizeResult.isErr:
      return err(finalizeResult.error)
    builder.writeHandle.close()

    let tempDataPath = getTmpPath(builder.store.dataDir, builder.treeId, ".data")
    ?builder.store.finalizeDataFile(tempDataPath, treeCid)
    let finalDataPath = getShardedPath(builder.store.dataDir, treeCid, ".data")
    try:
      dataFile = open(finalDataPath, fmRead)
    except IOError as e:
      return err(ioError("Failed to open data file for reading: " & e.msg))

  let manifest = newManifest(
    treeCid.toBytes(),
    builder.chunkSize,
    builder.totalSize,
    builder.filename,
    builder.mimetype
  )

  let
    manifestCid = ?manifest.toCid()
    now = epochTime().uint64

  #TODO cnanakos: maybe use a variant Dataset object for these?
  var
    blockmapSeq: seq[byte] = @[]
    blockmapFile: FileBlockmap = nil
    tempBmPath: string = ""

  case builder.blockmapBackend
  of bmLevelDb:
    blockmapSeq = newBlockmap(blockCount)
  of bmFile:
    tempBmPath = getTmpPath(builder.store.blockmapsDir, builder.treeId, ".blkmap")
    blockmapFile = ?newFileBlockmap(tempBmPath, forWriting = true)

  let meta = DatasetMetadata(
    treeCid: $treeCid,
    manifestCid: $manifestCid,
    lastAccess: now,
    size: builder.totalSize,
    blockCount: blockCount,
    chunkSize: builder.chunkSize,
    treeId: builder.treeId,
    merkleBackend: uint8(builder.merkleBackend),
    blockBackend: uint8(builder.blockBackend),
    blockmapBackend: uint8(builder.blockmapBackend)
  )

  let
    manifestBytes = ?encodeManifest(manifest)
    metaBytes = ?serializeDatasetMetadata(meta)

  try:
    let batch = newBatch()
    batch.put(datasetMetadataKey($treeCid), cast[string](metaBytes))
    batch.put(manifestKey($manifestCid), cast[string](manifestBytes))
    builder.store.db.write(batch)

    case builder.merkleBackend
    of mbEmbeddedProofs:
      const batchSize = 1024
      for chunkStart in countup(0, blockCount - 1, batchSize):
        let
          chunkEnd = min(chunkStart + batchSize, blockCount)
          dbBatch = newBatch()

        for index in chunkStart ..< chunkEnd:
          let
            proof = ?builder.merkleBuilder.getProof(index)
            blockRef = BlockRef(
              blockCid: $builder.blockCids[index],
              proof: proof
            )
            blockRefBytes = ?serializeBlockRef(blockRef)
          dbBatch.put(datasetBlockKey(builder.treeId, index), cast[string](blockRefBytes))

        builder.store.db.write(dbBatch)
    of mbLevelDb, mbPacked:
      discard

    case builder.blockmapBackend
    of bmLevelDb:
      for index in 0 ..< blockCount:
        blockmapSet(blockmapSeq, index, true)
    of bmFile:
      ?blockmapFile.setAll(blockCount.uint64)

    case builder.blockmapBackend
    of bmLevelDb:
      builder.store.db.put(blockmapKey($treeCid), cast[string](blockmapSeq))
    of bmFile:
      ?blockmapFile.finalize(blockCount.uint64)
      blockmapFile.close()
      ?builder.store.finalizeBlockmapFile(tempBmPath, treeCid)
      let finalBmPath = getBlockmapPath(builder.store.blockmapsDir, treeCid)
      blockmapFile = ?newFileBlockmap(finalBmPath, forWriting = false)

    var dataset = Dataset(
      treeCid: treeCid,
      manifestCid: manifestCid,
      blockCount: blockCount,
      chunkSize: builder.chunkSize,
      totalSize: builder.totalSize,
      treeId: builder.treeId,
      repo: builder.store.repo,
      db: builder.store.db,
      blockmapBackend: builder.blockmapBackend,
      blockmapSeq: blockmapSeq,
      blockmapFile: blockmapFile,
      merkleBackend: builder.merkleBackend,
      merkleReader: merkleReader,
      treesDir: builder.store.treesDir,
      blockBackend: builder.blockBackend,
      dataDir: builder.store.dataDir,
      dataFile: dataFile
    )

    return ok(dataset)
  except LevelDbException as e:
    return err(databaseError(e.msg))

proc next*(iter: LruIterator): Option[Cid] =
  if iter.index < iter.datasets.len:
    result = some(iter.datasets[iter.index])
    inc iter.index
  else:
    result = none(Cid)

iterator items*(iter: LruIterator): Cid =
  for c in iter.datasets:
    yield c
