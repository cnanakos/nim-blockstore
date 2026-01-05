import std/[sets, options, memfiles, bitops, os]
when defined(posix):
  import std/posix
import results
import libp2p/multicodec
import leveldbstatic as leveldb
import ./errors
import ./cid
import ./sha256

const
  HashSize* = 32
  MerkleTreePrefix* = "merkle:"
  MetadataKey = ":meta"

  PackedMagic = 0x534B4C4D'u32
  PackedVersion = 4'u32
  HeaderSize = 17
  EntrySize = 32

type
  MerkleBackend* = enum
    mbEmbeddedProofs
    mbLevelDb
    mbPacked

  MerkleHash* = array[HashSize, byte]

  MerkleStorage* = ref object of RootObj

  LevelDbMerkleStorage* = ref object of MerkleStorage
    db: LevelDb
    treeId: string

  PackedMerkleStorage* = ref object of MerkleStorage
    path: string
    file: File
    memFile: MemFile
    leafCount: uint64
    numLevels: int
    levelFiles: seq[File]
    readOnly: bool

  StreamingMerkleBuilder* = ref object
    frontier: seq[Option[MerkleHash]]
    pendingIndices: seq[uint64]
    leafCount: uint64
    storage: MerkleStorage

  MerkleReader* = ref object
    storage: MerkleStorage

  MerkleProofNode* = object
    hash*: MerkleHash
    level*: int

  MerkleProof* = object
    index*: uint64
    path*: seq[MerkleProofNode]
    leafCount*: uint64

  MerkleTreeBuilder* = ref object
    leaves: seq[array[32, byte]]
    tree: seq[seq[array[32, byte]]]
    built: bool

method putHash*(s: MerkleStorage, level: int, index: uint64, hash: MerkleHash): BResult[void] {.base, raises: [].}
method getHash*(s: MerkleStorage, level: int, index: uint64): Option[MerkleHash] {.base, raises: [].}
method setMetadata*(s: MerkleStorage, leafCount: uint64, numLevels: int): BResult[void] {.base, raises: [].}
method getMetadata*(s: MerkleStorage): tuple[leafCount: uint64, numLevels: int] {.base, raises: [].}
method close*(s: MerkleStorage): BResult[void] {.base, gcsafe, raises: [].}
method flush*(s: MerkleStorage) {.base, gcsafe, raises: [].}

proc computeNumLevels*(leafCount: uint64): int =
  if leafCount == 0: return 0
  if leafCount == 1: return 1
  fastLog2(leafCount - 1) + 2

proc nodesAtLevel*(leafCount: uint64, level: int): uint64 =
  if leafCount == 0: return 0
  if level == 0: return leafCount
  if level >= 64:
    return if level == 64: 1 else: 0
  let
    shifted = leafCount shr level
    mask = (1'u64 shl level) - 1
  if (leafCount and mask) > 0: shifted + 1 else: shifted

proc nodesBeforeLevel*(leafCount: uint64, level: int): uint64 =
  result = 0
  for l in 0 ..< level:
    result += nodesAtLevel(leafCount, l)

proc nodePosition*(leafCount: uint64, level: int, index: uint64): uint64 =
  nodesBeforeLevel(leafCount, level) + index

proc hashConcat*(left, right: MerkleHash): MerkleHash =
  var combined: array[64, byte]
  copyMem(addr combined[0], unsafeAddr left[0], 32)
  copyMem(addr combined[32], unsafeAddr right[0], 32)
  sha256Hash(combined)

method putHash*(s: MerkleStorage, level: int, index: uint64, hash: MerkleHash): BResult[void] {.base, raises: [].} =
  err(ioError("putHash not implemented"))

method getHash*(s: MerkleStorage, level: int, index: uint64): Option[MerkleHash] {.base, raises: [].} =
  none(MerkleHash)

method setMetadata*(s: MerkleStorage, leafCount: uint64, numLevels: int): BResult[void] {.base, raises: [].} =
  err(ioError("setMetadata not implemented"))

method getMetadata*(s: MerkleStorage): tuple[leafCount: uint64, numLevels: int] {.base, raises: [].} =
  (0'u64, 0)

method close*(s: MerkleStorage): BResult[void] {.base, gcsafe, raises: [].} =
  ok()

method flush*(s: MerkleStorage) {.base, gcsafe, raises: [].} =
  discard

proc levelDbKey(treeId: string, level: int, index: uint64): string =
  MerkleTreePrefix & treeId & ":L" & $level & ":I" & $index

proc levelDbMetaKey(treeId: string): string =
  MerkleTreePrefix & treeId & MetadataKey

proc newLevelDbMerkleStorage*(db: LevelDb, treeId: string): LevelDbMerkleStorage =
  LevelDbMerkleStorage(db: db, treeId: treeId)

method putHash*(s: LevelDbMerkleStorage, level: int, index: uint64, hash: MerkleHash): BResult[void] {.raises: [].} =
  let key = levelDbKey(s.treeId, level, index)
  try:
    s.db.put(key, cast[string](@hash))
    ok()
  except CatchableError as e:
    err(databaseError(e.msg))
  except Exception as e:
    err(databaseError(e.msg))

method getHash*(s: LevelDbMerkleStorage, level: int, index: uint64): Option[MerkleHash] {.raises: [].} =
  let key = levelDbKey(s.treeId, level, index)
  try:
    let valueOpt = s.db.get(key)
    if valueOpt.isNone or valueOpt.get.len != HashSize:
      return none(MerkleHash)
    var hash: MerkleHash
    copyMem(addr hash[0], unsafeAddr valueOpt.get[0], HashSize)
    some(hash)
  except CatchableError:
    none(MerkleHash)
  except Exception:
    none(MerkleHash)

method setMetadata*(s: LevelDbMerkleStorage, leafCount: uint64, numLevels: int): BResult[void] {.raises: [].} =
  let key = levelDbMetaKey(s.treeId)
  var data: array[9, byte]
  copyMem(addr data[0], unsafeAddr leafCount, 8)
  var nl = numLevels.uint8
  copyMem(addr data[8], unsafeAddr nl, 1)
  try:
    s.db.put(key, cast[string](@data))
    ok()
  except CatchableError as e:
    err(databaseError(e.msg))
  except Exception as e:
    err(databaseError(e.msg))

method getMetadata*(s: LevelDbMerkleStorage): tuple[leafCount: uint64, numLevels: int] {.raises: [].} =
  let key = levelDbMetaKey(s.treeId)
  try:
    let valueOpt = s.db.get(key)
    if valueOpt.isNone or valueOpt.get.len < 9:
      return (0'u64, 0)
    var
      leafCount: uint64
      numLevels: uint8
    copyMem(addr leafCount, unsafeAddr valueOpt.get[0], 8)
    copyMem(addr numLevels, unsafeAddr valueOpt.get[8], 1)
    (leafCount, numLevels.int)
  except CatchableError:
    (0'u64, 0)
  except Exception:
    (0'u64, 0)

proc levelTempPath(basePath: string, level: int): string =
  basePath & ".L" & $level & ".tmp"

proc newPackedMerkleStorage*(path: string, forWriting: bool = false): BResult[PackedMerkleStorage] =
  var storage = PackedMerkleStorage(
    path: path,
    levelFiles: @[]
  )

  if forWriting:
    storage.readOnly = false
    storage.leafCount = 0
    storage.numLevels = 0
    storage.file = syncio.open(path, fmReadWrite)

    var header: array[HeaderSize, byte]
    var magic = PackedMagic
    var version = PackedVersion
    copyMem(addr header[0], addr magic, 4)
    copyMem(addr header[4], addr version, 4)
    let written = storage.file.writeBuffer(addr header[0], HeaderSize)
    if written != HeaderSize:
      storage.file.close()
      return err(ioError("Failed to write packed merkle header"))
  else:
    storage.readOnly = true
    storage.memFile = memfiles.open(path, mode = fmRead)

    let
      data = cast[ptr UncheckedArray[byte]](storage.memFile.mem)
      fileSize = storage.memFile.size

    if fileSize < HeaderSize:
      storage.memFile.close()
      return err(ioError("File too small for header"))

    var
      magic: uint32
      version: uint32
      nl: uint8
    copyMem(addr magic, addr data[0], 4)
    copyMem(addr version, addr data[4], 4)
    copyMem(addr storage.leafCount, addr data[8], 8)
    copyMem(addr nl, addr data[16], 1)
    storage.numLevels = nl.int

    if magic != PackedMagic:
      storage.memFile.close()
      return err(ioError("Invalid packed merkle file magic"))
    if version != PackedVersion:
      storage.memFile.close()
      return err(ioError("Unsupported packed merkle file version: " & $version))

  ok(storage)

method putHash*(s: PackedMerkleStorage, level: int, index: uint64, hash: MerkleHash): BResult[void] {.raises: [].} =
  if s.readOnly:
    return err(ioError("Storage is read-only"))

  try:
    if level == 0:
      let offset = HeaderSize + index.int64 * EntrySize
      s.file.setFilePos(offset)
      let written = s.file.writeBuffer(unsafeAddr hash[0], HashSize)
      if written != HashSize:
        return err(ioError("Failed to write hash at level 0"))
    else:
      while s.levelFiles.len < level:
        let tempPath = levelTempPath(s.path, s.levelFiles.len + 1)
        s.levelFiles.add(syncio.open(tempPath, fmReadWrite))
      let f = s.levelFiles[level - 1]
      f.setFilePos(index.int64 * HashSize)
      let written = f.writeBuffer(unsafeAddr hash[0], HashSize)
      if written != HashSize:
        return err(ioError("Failed to write hash at level " & $level))
    ok()
  except CatchableError as e:
    err(ioError(e.msg))
  except Exception as e:
    err(ioError(e.msg))

method getHash*(s: PackedMerkleStorage, level: int, index: uint64): Option[MerkleHash] {.raises: [].} =
  if s.leafCount == 0:
    return none(MerkleHash)

  if index >= nodesAtLevel(s.leafCount, level):
    return none(MerkleHash)

  let
    position = nodePosition(s.leafCount, level, index)
    offset = HeaderSize + position.int64 * EntrySize

  try:
    if s.readOnly:
      if offset + HashSize > s.memFile.size:
        return none(MerkleHash)
      let data = cast[ptr UncheckedArray[byte]](s.memFile.mem)
      var hash: MerkleHash
      copyMem(addr hash[0], addr data[offset], HashSize)
      return some(hash)
    else:
      s.file.setFilePos(offset)
      var hash: MerkleHash
      let bytesRead = s.file.readBuffer(addr hash[0], HashSize)
      if bytesRead != HashSize:
        return none(MerkleHash)
      return some(hash)
  except CatchableError:
    none(MerkleHash)
  except Exception:
    none(MerkleHash)

method setMetadata*(s: PackedMerkleStorage, leafCount: uint64, numLevels: int): BResult[void] {.raises: [].} =
  if s.readOnly:
    return ok()
  s.leafCount = leafCount
  s.numLevels = numLevels

  try:
    s.file.setFilePos(8)
    var
      lc = leafCount
      nl = numLevels.uint8
    var written = s.file.writeBuffer(addr lc, 8)
    if written != 8:
      return err(ioError("Failed to write leaf count"))
    written = s.file.writeBuffer(addr nl, 1)
    if written != 1:
      return err(ioError("Failed to write num levels"))
    ok()
  except CatchableError as e:
    err(ioError(e.msg))
  except Exception as e:
    err(ioError(e.msg))

method getMetadata*(s: PackedMerkleStorage): tuple[leafCount: uint64, numLevels: int] {.raises: [].} =
  (s.leafCount, s.numLevels)

method close*(s: PackedMerkleStorage): BResult[void] {.gcsafe, raises: [].} =
  try:
    if s.readOnly:
      s.memFile.close()
    else:
      s.file.setFilePos(0, fspEnd)
      var buffer: array[4096, byte]
      for i, levelFile in s.levelFiles:
        flushFile(levelFile)
        levelFile.setFilePos(0)
        while true:
          let bytesRead = levelFile.readBuffer(addr buffer[0], buffer.len)
          if bytesRead == 0:
            break
          let written = s.file.writeBuffer(addr buffer[0], bytesRead)
          if written != bytesRead:
            return err(ioError("Failed to write level " & $(i + 1) & " data"))
        levelFile.close()
        removeFile(levelTempPath(s.path, i + 1))
      s.levelFiles = @[]

      flushFile(s.file)
      when defined(posix):
        if fsync(s.file.getFileHandle().cint) != 0:
          return err(ioError("fsync failed"))
      s.file.close()
    ok()
  except CatchableError as e:
    err(ioError(e.msg))
  except Exception as e:
    err(ioError(e.msg))

method flush*(s: PackedMerkleStorage) {.gcsafe.} =
  if not s.readOnly:
    flushFile(s.file)
    for levelFile in s.levelFiles:
      flushFile(levelFile)

proc newStreamingMerkleBuilder*(storage: MerkleStorage): StreamingMerkleBuilder =
  StreamingMerkleBuilder(
    frontier: @[],
    pendingIndices: @[],
    leafCount: 0,
    storage: storage
  )

proc addLeaf*(builder: StreamingMerkleBuilder, hash: MerkleHash): BResult[void] {.raises: [].} =
  try:
    var
      current = hash
      level = 0
      index = builder.leafCount

    while builder.frontier.len <= level:
      builder.frontier.add(none(MerkleHash))
      builder.pendingIndices.add(0'u64)

    while level < builder.frontier.len and builder.frontier[level].isSome:
      let
        sibling = builder.frontier[level].get()
        siblingIndex = builder.pendingIndices[level]

      let r1 = builder.storage.putHash(level, siblingIndex, sibling)
      if r1.isErr:
        return err(r1.error)
      let r2 = builder.storage.putHash(level, siblingIndex + 1, current)
      if r2.isErr:
        return err(r2.error)

      current = hashConcat(sibling, current)
      builder.frontier[level] = none(MerkleHash)

      level += 1
      index = index shr 1

      while builder.frontier.len <= level:
        builder.frontier.add(none(MerkleHash))
        builder.pendingIndices.add(0'u64)

    builder.frontier[level] = some(current)
    builder.pendingIndices[level] = index
    builder.leafCount += 1
    ok()
  except CatchableError as e:
    err(ioError(e.msg))
  except Exception as e:
    err(ioError(e.msg))

proc finalize*(builder: StreamingMerkleBuilder): BResult[MerkleHash] {.raises: [].} =
  if builder.leafCount == 0:
    return err(merkleTreeError("Cannot finalize empty tree"))

  let numLevels = computeNumLevels(builder.leafCount)

  var
    current: Option[MerkleHash] = none(MerkleHash)
    currentIndex: uint64 = 0
    currentLevel: int = 0

  for level in 0 ..< builder.frontier.len:
    if builder.frontier[level].isSome:
      let
        hash = builder.frontier[level].get()
        index = builder.pendingIndices[level]

      ?builder.storage.putHash(level, index, hash)

      if current.isNone:
        current = some(hash)
        currentIndex = index
        currentLevel = level
      else:
        while currentLevel < level:
          currentLevel += 1
          currentIndex = currentIndex shr 1
          ?builder.storage.putHash(currentLevel, currentIndex, current.get())

        let combined = hashConcat(hash, current.get())
        current = some(combined)
        currentIndex = index shr 1
        currentLevel = level + 1

        ?builder.storage.putHash(currentLevel, currentIndex, current.get())

    elif current.isSome and currentLevel == level:
      currentLevel += 1
      currentIndex = currentIndex shr 1
      ?builder.storage.putHash(currentLevel, currentIndex, current.get())

  if current.isNone:
    return err(merkleTreeError("Failed to compute root"))

  ?builder.storage.setMetadata(builder.leafCount, numLevels)
  ?builder.storage.putHash(numLevels - 1, 0, current.get())
  builder.storage.flush()

  ok(current.get())

proc leafCount*(builder: StreamingMerkleBuilder): uint64 =
  builder.leafCount

proc newMerkleReader*(storage: MerkleStorage): MerkleReader =
  MerkleReader(storage: storage)

proc close*(reader: MerkleReader) =
  if reader.storage != nil:
    discard reader.storage.close()

proc root*(reader: MerkleReader): Option[MerkleHash] =
  let (leafCount, numLevels) = reader.storage.getMetadata()
  if numLevels == 0:
    return none(MerkleHash)
  reader.storage.getHash(numLevels - 1, 0)

proc leafCount*(reader: MerkleReader): uint64 =
  reader.storage.getMetadata().leafCount

proc getProof*(reader: MerkleReader, index: uint64): BResult[MerkleProof] =
  let (leafCount, numLevels) = reader.storage.getMetadata()

  if index >= leafCount:
    return err(invalidBlockError())

  var
    path: seq[MerkleProofNode] = @[]
    idx = index

  for level in 0 ..< numLevels - 1:
    let
      siblingIdx = idx xor 1
      maxIdx = nodesAtLevel(leafCount, level)

    if siblingIdx < maxIdx:
      let siblingOpt = reader.storage.getHash(level, siblingIdx)
      if siblingOpt.isSome:
        path.add(MerkleProofNode(hash: siblingOpt.get(), level: level))

    idx = idx shr 1

  ok(MerkleProof(
    index: index,
    path: path,
    leafCount: leafCount
  ))

proc newMerkleTreeBuilder*(): MerkleTreeBuilder =
  MerkleTreeBuilder(
    leaves: @[],
    tree: @[],
    built: false
  )

proc addBlock*(builder: MerkleTreeBuilder, blockData: openArray[byte]) =
  if builder.built:
    raise newException(Defect, "Cannot add blocks after tree has been built")
  builder.leaves.add(sha256Hash(blockData))

proc buildTree*(builder: MerkleTreeBuilder) =
  if builder.built or builder.leaves.len == 0:
    return

  builder.tree = @[]
  builder.tree.add(builder.leaves)

  var currentLevel = builder.leaves
  while currentLevel.len > 1:
    var
      nextLevel: seq[array[32, byte]] = @[]
      i = 0
    while i < currentLevel.len:
      if i + 1 < currentLevel.len:
        nextLevel.add(hashConcat(currentLevel[i], currentLevel[i + 1]))
      else:
        nextLevel.add(currentLevel[i])
      i += 2
    builder.tree.add(nextLevel)
    currentLevel = nextLevel

  builder.built = true

proc root*(builder: MerkleTreeBuilder): Option[array[32, byte]] =
  if not builder.built or builder.tree.len == 0:
    return none(array[32, byte])
  some(builder.tree[^1][0])

proc rootCid*(builder: MerkleTreeBuilder): BResult[Cid] =
  if not builder.built:
    return err(merkleTreeError("Tree not built. Call buildTree() first"))

  let rootOpt = builder.root()
  if rootOpt.isNone:
    return err(merkleTreeError("Failed to compute merkle root"))

  let mh = ?wrap(Sha256Code, rootOpt.get())
  newCidV1(LogosStorageTree, mh)

proc blockCount*(builder: MerkleTreeBuilder): int =
  builder.leaves.len

proc getProof*(builder: MerkleTreeBuilder, index: int): BResult[MerkleProof] =
  if index < 0 or index >= builder.leaves.len:
    return err(invalidBlockError())

  if not builder.built:
    return err(merkleTreeError("Tree not built. Call buildTree() first"))

  var
    path: seq[MerkleProofNode] = @[]
    idx = index

  for level in 0 ..< builder.tree.len - 1:
    let siblingIdx = if (idx mod 2) == 0: idx + 1 else: idx - 1
    if siblingIdx < builder.tree[level].len:
      path.add(MerkleProofNode(hash: builder.tree[level][siblingIdx], level: level))
    idx = idx div 2

  ok(MerkleProof(
    index: index.uint64,
    path: path,
    leafCount: builder.leaves.len.uint64
  ))

proc verify*(proof: MerkleProof, root: MerkleHash, leafHash: MerkleHash): bool =
  var
    current = leafHash
    idx = proof.index
    currentLevel = 0

  for node in proof.path:
    while currentLevel < node.level:
      idx = idx shr 1
      currentLevel += 1

    if (idx and 1) == 0:
      current = hashConcat(current, node.hash)
    else:
      current = hashConcat(node.hash, current)
    idx = idx shr 1
    currentLevel += 1

  current == root

proc verify*(proof: MerkleProof, root: openArray[byte], data: openArray[byte]): BResult[bool] =
  if root.len != 32:
    return err(invalidProofError())

  var rootHash: MerkleHash
  copyMem(addr rootHash[0], unsafeAddr root[0], 32)

  var
    currentHash = sha256Hash(data)
    idx = proof.index
    currentLevel = 0

  for node in proof.path:
    while currentLevel < node.level:
      idx = idx shr 1
      currentLevel += 1

    if (idx and 1) == 0:
      currentHash = hashConcat(currentHash, node.hash)
    else:
      currentHash = hashConcat(node.hash, currentHash)
    idx = idx shr 1
    currentLevel += 1

  ok(currentHash == rootHash)

proc rootToCid*(root: MerkleHash, hashCode: MultiCodec, treeCodec: MultiCodec): BResult[Cid] =
  let mh = ?wrap(hashCode, root)
  newCidV1(treeCodec, mh)

proc rootToCid*(root: MerkleHash): BResult[Cid] =
  rootToCid(root, Sha256Code, LogosStorageTree)

proc collectLeavesUnderNode(nodeIdx: int, levelSize: int, totalLeaves: int, leaves: var HashSet[int])

proc getRequiredLeafIndices*(start: int, count: int, totalLeaves: int): HashSet[int] =
  result = initHashSet[int]()

  var have = initHashSet[int]()
  for i in start ..< start + count:
    have.incl(i)

  var levelSize = totalLeaves

  while levelSize > 1:
    var
      nextHave = initHashSet[int]()
      processedPairs = initHashSet[int]()

    for idx in have:
      let pairIdx = idx div 2

      if pairIdx in processedPairs:
        continue
      processedPairs.incl(pairIdx)

      let
        leftIdx = pairIdx * 2
        rightIdx = pairIdx * 2 + 1

        haveLeft = leftIdx in have
        haveRight = rightIdx < levelSize and rightIdx in have

      if haveLeft and not haveRight and rightIdx < levelSize:
        collectLeavesUnderNode(rightIdx, levelSize, totalLeaves, result)
      elif not haveLeft and haveRight:
        collectLeavesUnderNode(leftIdx, levelSize, totalLeaves, result)

      nextHave.incl(pairIdx)

    levelSize = (levelSize + 1) div 2
    have = nextHave

proc collectLeavesUnderNode(nodeIdx: int, levelSize: int, totalLeaves: int, leaves: var HashSet[int]) =
  var
    currentSize = levelSize
    levelsToLeaves = 0
  while currentSize < totalLeaves:
    currentSize = currentSize * 2
    inc levelsToLeaves

  let
    leavesPerNode = 1 shl levelsToLeaves
    startLeaf = nodeIdx * leavesPerNode
    endLeaf = min((nodeIdx + 1) * leavesPerNode, totalLeaves)

  for leafIdx in startLeaf ..< endLeaf:
    leaves.incl(leafIdx)
