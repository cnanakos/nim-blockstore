import std/[os, locks, atomics, strutils, times, options, tables]
when defined(posix):
  import std/posix
import chronos
import chronos/asyncsync
import leveldbstatic as leveldb

import ./errors
import ./cid
import ./blocks as blk
import ./serialization
import ./sharding
import ./ioutils

export PendingDeletion, BlockInfo
export IOMode

const
  BlockInfoPrefix = "block_info:"
  PendingDeletionsPrefix = "pending_deletions:"
  UsedKey = "repo_metadata:used"

type
  SyncWorker* = ref object
    mutex: Lock
    cond: Cond
    running: Atomic[bool]
    thread: Thread[pointer]
    blocksDir: string

  CidLock* = ref object
    lock*: AsyncLock
    waiters*: int

  RepoStore* = ref object
    blocksDir: string
    db: LevelDb
    quota: uint64
    used: Atomic[uint64]
    ioMode: IOMode
    syncBatchSize: int
    syncWorker: SyncWorker
    writeCount: Atomic[int]
    cidLocks: Table[string, CidLock]

when defined(linux):
  proc syncfs(fd: cint): cint {.importc, header: "<unistd.h>".}

proc doSync(blocksDir: string) =
  when defined(linux):
    let fd = posix.open(blocksDir.cstring, O_RDONLY)
    if fd >= 0:
      discard syncfs(fd)
      discard posix.close(fd)
  elif defined(posix):
    proc sync() {.importc, header: "<unistd.h>".}
    sync()
  else:
    discard

proc syncWorkerLoop(workerPtr: pointer) {.thread, nimcall.} =
  let worker = cast[SyncWorker](workerPtr)
  while true:
    acquire(worker.mutex)
    while worker.running.load():
      wait(worker.cond, worker.mutex)
      if not worker.running.load():
        break
      release(worker.mutex)
      doSync(worker.blocksDir)
      acquire(worker.mutex)
    release(worker.mutex)
    doSync(worker.blocksDir)
    break

proc newSyncWorker*(blocksDir: string): SyncWorker =
  result = SyncWorker(blocksDir: blocksDir)
  initLock(result.mutex)
  initCond(result.cond)
  result.running.store(true)
  createThread(result.thread, syncWorkerLoop, cast[pointer](result))

proc triggerSync*(worker: SyncWorker) =
  signal(worker.cond)

proc stopSyncWorker*(worker: SyncWorker) =
  worker.running.store(false)
  signal(worker.cond)
  joinThread(worker.thread)
  deinitCond(worker.cond)
  deinitLock(worker.mutex)

proc calculateUsedFromDb(db: LevelDb): uint64 =
  result = 0
  for key, value in db.iter():
    if key.startsWith(BlockInfoPrefix):
      let infoResult = deserializeBlockInfo(cast[seq[byte]](value))
      if infoResult.isOk:
        result += infoResult.value.size.uint64
    elif key.startsWith(PendingDeletionsPrefix):
      let pdResult = deserializePendingDeletion(cast[seq[byte]](value))
      if pdResult.isOk:
        result += pdResult.value.size

proc newRepoStore*(blocksDir: string, db: LevelDb, quota: uint64 = 0,
                   ioMode: IOMode = ioDirect,
                   syncBatchSize: int = 0): BResult[RepoStore] =
  ?initShardDirectories(blocksDir)

  var used: uint64 = 0
  try:
    let usedBytesOpt = db.get(UsedKey)
    if usedBytesOpt.isSome and usedBytesOpt.get.len > 0:
      let usedResult = deserializeUint64(cast[seq[byte]](usedBytesOpt.get))
      if usedResult.isOk:
        used = usedResult.value
      else:
        used = calculateUsedFromDb(db)
    else:
      used = calculateUsedFromDb(db)
      let usedBytes = ?serializeUint64(used)
      db.put(UsedKey, cast[string](usedBytes))
  except LevelDbException as e:
    return err(databaseError(e.msg))

  var syncWorker: SyncWorker = nil
  if ioMode == ioBuffered and syncBatchSize > 1:
    syncWorker = newSyncWorker(blocksDir)

  var store = RepoStore(
    blocksDir: blocksDir,
    db: db,
    quota: quota,
    ioMode: ioMode,
    syncBatchSize: syncBatchSize,
    syncWorker: syncWorker
  )
  store.used.store(used)

  ok(store)

proc close*(store: RepoStore) =
  if store.syncWorker != nil:
    stopSyncWorker(store.syncWorker)

proc acquireCidLock*(store: RepoStore, cidStr: string): Future[CidLock] {.async.} =
  var cl: CidLock

  if cidStr in store.cidLocks:
    cl = store.cidLocks[cidStr]
    cl.waiters += 1
  else:
    cl = CidLock(lock: newAsyncLock(), waiters: 1)
    store.cidLocks[cidStr] = cl

  await cl.lock.acquire()
  return cl

proc releaseCidLock*(store: RepoStore, cl: CidLock, cidStr: string) =
  cl.lock.release()

  cl.waiters -= 1
  if cl.waiters == 0:
    store.cidLocks.del(cidStr)

proc used*(store: RepoStore): uint64 {.inline.} =
  store.used.load()

proc decreaseUsed*(store: RepoStore, size: uint64) {.inline.} =
  discard store.used.fetchSub(size)

proc quota*(store: RepoStore): uint64 {.inline.} =
  store.quota

proc wouldExceedQuota*(store: RepoStore, size: uint64): bool {.inline.} =
  if store.quota == 0:
    return false
  store.used() + size > store.quota

proc blocksDir*(store: RepoStore): string {.inline.} =
  store.blocksDir

proc getBlockPath(store: RepoStore, c: Cid): string {.inline.} =
  getShardedPath(store.blocksDir, c)

proc blockInfoKey(cidStr: string): string {.inline.} =
  BlockInfoPrefix & cidStr

proc pendingDeletionKey(cidStr: string): string {.inline.} =
  PendingDeletionsPrefix & cidStr

proc hasBlock*(store: RepoStore, c: Cid): BResult[bool] {.raises: [].} =
  let key = blockInfoKey($c)
  try:
    let valueOpt = store.db.get(key)
    ok(valueOpt.isSome)
  except LevelDbException as e:
    err(databaseError(e.msg))
  except CatchableError as e:
    err(databaseError(e.msg))

proc incrementRefCount(store: RepoStore, cidStr: string): BResult[void] =
  let key = blockInfoKey(cidStr)
  try:
    let valueOpt = store.db.get(key)
    if valueOpt.isSome:
      let infoResult = deserializeBlockInfo(cast[seq[byte]](valueOpt.get))
      if infoResult.isOk:
        var info = infoResult.value
        info.refCount += 1
        let infoBytes = ?serializeBlockInfo(info)
        store.db.put(key, cast[string](infoBytes))
    ok()
  except LevelDbException as e:
    err(databaseError(e.msg))
  except Exception as e:
    err(databaseError(e.msg))

proc putBlock*(store: RepoStore, b: blk.Block): Future[BResult[bool]] {.async.} =
  let cidStr = $b.cid
  let blockPath = store.getBlockPath(b.cid)
  let blockSize = b.data.len.uint64

  let hasIt = ?store.hasBlock(b.cid)
  if hasIt:
    ?store.incrementRefCount(cidStr)
    return ok(false)

  let cl = await store.acquireCidLock(cidStr)
  defer: store.releaseCidLock(cl, cidStr)

  let hasIt2 = ?store.hasBlock(b.cid)
  if hasIt2:
    ?store.incrementRefCount(cidStr)
    return ok(false)

  let fileExisted = fileExists(blockPath)

  var newUsed: uint64
  if fileExisted:
    newUsed = store.used.load()
  else:
    if store.wouldExceedQuota(blockSize):
      return err(quotaExceededError())

    case store.ioMode
    of ioDirect:
      let writeResult = writeBlockToFile(blockPath, b.data, ioDirect)
      if writeResult.isErr:
        return err(writeResult.error)

    of ioBuffered:
      if store.syncBatchSize == 0:
        let writeResult = writeBlockToFile(blockPath, b.data, ioBuffered)
        if writeResult.isErr:
          return err(writeResult.error)

      elif store.syncBatchSize == 1:
        let fileResult = writeBlockBuffered(blockPath, b.data)
        if fileResult.isErr:
          return err(fileResult.error)
        let syncResult = syncAndCloseFile(fileResult.value)
        if syncResult.isErr:
          return err(syncResult.error)

      else:
        let writeResult = writeBlockToFile(blockPath, b.data, ioBuffered)
        if writeResult.isErr:
          return err(writeResult.error)
        let count = store.writeCount.fetchAdd(1) + 1
        if count mod store.syncBatchSize == 0:
          store.syncWorker.triggerSync()

    newUsed = store.used.fetchAdd(blockSize) + blockSize

  let info = BlockInfo(size: b.data.len, refCount: 1)
  let
    infoBytes = ?serializeBlockInfo(info)
    usedBytes = ?serializeUint64(newUsed)
  try:
    store.db.put(blockInfoKey(cidStr), cast[string](infoBytes))
    store.db.put(UsedKey, cast[string](usedBytes))
    ok(not fileExisted)
  except LevelDbException as e:
    err(databaseError(e.msg))
  except Exception as e:
    err(databaseError(e.msg))

proc getBlock*(store: RepoStore, c: Cid): Future[BResult[Option[blk.Block]]] {.async.} =
  let blockPath = store.getBlockPath(c)

  let hasIt = ?store.hasBlock(c)
  if not hasIt:
    return ok(none(blk.Block))

  if not fileExists(blockPath):
    return ok(none(blk.Block))

  var data: seq[byte]
  try:
    data = cast[seq[byte]](readFile(blockPath))
  except IOError as e:
    return err(ioError(e.msg))

  let b = ?blk.newBlock(data)
  if b.cid != c:
    return err(cidError("Block CID mismatch"))

  ok(some(b))

proc getBlockUnchecked*(store: RepoStore, c: Cid): Future[BResult[Option[blk.Block]]] {.async.} =
  let blockPath = store.getBlockPath(c)

  let hasIt = ?store.hasBlock(c)
  if not hasIt:
    return ok(none(blk.Block))

  if not fileExists(blockPath):
    return ok(none(blk.Block))

  var data: seq[byte]
  try:
    data = cast[seq[byte]](readFile(blockPath))
  except IOError as e:
    return err(ioError(e.msg))

  ok(some(blk.fromCidUnchecked(c, data)))

proc releaseBlock*(store: RepoStore, c: Cid): BResult[bool] =
  let cidStr = $c
  let blockPath = store.getBlockPath(c)
  let key = blockInfoKey(cidStr)

  try:
    let valueOpt = store.db.get(key)
    if valueOpt.isNone:
      return ok(false)

    var info = ?deserializeBlockInfo(cast[seq[byte]](valueOpt.get))
    if info.refCount == 0:
      return err(databaseError("Block ref_count already 0"))

    info.refCount -= 1

    if info.refCount == 0:
      let blockSize = info.size.uint64
      let pd = PendingDeletion(
        queuedAt: epochTime().uint64,
        blockPath: blockPath,
        size: blockSize
      )
      let pdBytes = ?serializePendingDeletion(pd)
      store.db.delete(key)
      store.db.put(pendingDeletionKey(cidStr), cast[string](pdBytes))
      return ok(true)
    else:
      let infoBytes = ?serializeBlockInfo(info)
      store.db.put(key, cast[string](infoBytes))
      return ok(false)

  except LevelDbException as e:
    err(databaseError(e.msg))

proc getPendingDeletions*(store: RepoStore, limit: int): BResult[seq[(string, PendingDeletion)]] =
  var entries: seq[(string, PendingDeletion)] = @[]
  try:
    for key, value in store.db.iter():
      if not key.startsWith(PendingDeletionsPrefix):
        continue
      let cidStr = key[PendingDeletionsPrefix.len .. ^1]
      let pdResult = deserializePendingDeletion(cast[seq[byte]](value))
      if pdResult.isOk:
        entries.add((cidStr, pdResult.value))
        if entries.len >= limit:
          break
    ok(entries)
  except LevelDbException as e:
    err(databaseError(e.msg))

proc pendingDeletionsCount*(store: RepoStore): BResult[int] =
  var count = 0
  try:
    for key, _ in store.db.iter():
      if key.startsWith(PendingDeletionsPrefix):
        inc count
    ok(count)
  except LevelDbException as e:
    err(databaseError(e.msg))

proc deletePendingBlock*(store: RepoStore, c: Cid, blockPath: string, size: uint64): Future[BResult[bool]] {.async.} =
  let hasIt = ?store.hasBlock(c)
  if hasIt:
    return ok(false)

  if fileExists(blockPath):
    try:
      removeFile(blockPath)
      discard store.used.fetchSub(size)
    except OSError as e:
      return err(ioError(e.msg))

  ok(true)

proc removePendingDeletion*(store: RepoStore, cidStr: string): BResult[void] =
  try:
    store.db.delete(pendingDeletionKey(cidStr))
    ok()
  except LevelDbException as e:
    err(databaseError(e.msg))

proc removePendingDeletionsBatch*(store: RepoStore, cidStrs: seq[string]): BResult[void] =
  if cidStrs.len == 0:
    return ok()
  let currentUsed = store.used.load()
  let usedBytes = ?serializeUint64(currentUsed)
  try:
    let batch = newBatch()
    for cidStr in cidStrs:
      batch.delete(pendingDeletionKey(cidStr))
    batch.put(UsedKey, cast[string](usedBytes))
    store.db.write(batch)
    ok()
  except LevelDbException as e:
    err(databaseError(e.msg))

proc totalSize*(store: RepoStore): BResult[uint64] =
  var total: uint64 = 0
  try:
    for key, value in store.db.iter():
      if key.startsWith(BlockInfoPrefix):
        let infoResult = deserializeBlockInfo(cast[seq[byte]](value))
        if infoResult.isOk:
          total += infoResult.value.size.uint64
    ok(total)
  except LevelDbException as e:
    err(databaseError(e.msg))
