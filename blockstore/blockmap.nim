import std/[os, bitops, memfiles, posix]
import results

import ./errors
import ./sharding
import ./cid

proc newBlockmap*(size: int): seq[byte] =
  let byteCount = (size + 7) div 8
  newSeq[byte](byteCount)

proc blockmapGet*(blockmap: seq[byte], index: int): bool =
  if index < 0:
    return false
  let
    byteIdx = index div 8
    bitIdx = index mod 8
  if byteIdx >= blockmap.len:
    return false
  (blockmap[byteIdx] and (1'u8 shl bitIdx)) != 0

proc blockmapSet*(blockmap: var seq[byte], index: int, value: bool) =
  if index < 0:
    return
  let
    byteIdx = index div 8
    bitIdx = index mod 8
  if byteIdx >= blockmap.len:
    return
  if value:
    blockmap[byteIdx] = blockmap[byteIdx] or (1'u8 shl bitIdx)
  else:
    blockmap[byteIdx] = blockmap[byteIdx] and not (1'u8 shl bitIdx)

proc blockmapCountOnes*(blockmap: seq[byte]): int =
  result = 0
  for b in blockmap:
    result += countSetBits(b)

const
  BlockmapMagic = 0x424D4150'u32
  BlockmapVersion = 1'u8
  BlocksPerChunk* = 1024 * 1024
  GrowthChunk = 1024 * 1024
  HeaderSize = 24
  ChunkEmpty* = 0x00'u8
  ChunkFull* = 0xFF'u8
  ChunkPartial* = 0x01'u8

type
  BlockmapBackend* = enum
    bmLevelDb
    bmFile

  FileBlockmap* = ref object
    path: string
    file: MemFile
    fileSize: int
    maxIndex: uint64
    indexSize: uint32
    readOnly: bool

  BlockRange* = object
    start*: uint64
    count*: uint64

proc headerMaxIndex(mem: pointer): ptr uint64 {.inline.} =
  cast[ptr uint64](cast[uint](mem) + 8)

proc headerIndexSize(mem: pointer): ptr uint32 {.inline.} =
  cast[ptr uint32](cast[uint](mem) + 16)

proc indexOffset(): int {.inline.} =
  HeaderSize

proc bitmapOffset(indexSize: uint32): int {.inline.} =
  HeaderSize + indexSize.int

proc chunkIndexPtr(bm: FileBlockmap, chunkIdx: uint32): ptr uint8 {.inline.} =
  if chunkIdx >= bm.indexSize:
    return nil
  cast[ptr uint8](cast[uint](bm.file.mem) + indexOffset().uint + chunkIdx.uint)

proc bitmapBytePtr(bm: FileBlockmap, byteIdx: uint64): ptr uint8 {.inline.} =
  let offset = bitmapOffset(bm.indexSize).uint64 + byteIdx
  if offset.int >= bm.fileSize:
    return nil
  cast[ptr uint8](cast[uint](bm.file.mem) + offset.uint)

proc getChunkState*(bm: FileBlockmap, chunkIdx: uint32): uint8 =
  let p = bm.chunkIndexPtr(chunkIdx)
  if p == nil:
    return ChunkEmpty
  p[]

proc setChunkState(bm: FileBlockmap, chunkIdx: uint32, state: uint8) =
  let p = bm.chunkIndexPtr(chunkIdx)
  if p != nil:
    p[] = state

proc neededFileSize(blockIndex: uint64, currentIndexSize: uint32): tuple[fileSize: int, indexSize: uint32] =
  let chunkIdx = (blockIndex div BlocksPerChunk).uint32 + 1
  let newIndexSize = max(currentIndexSize, chunkIdx)
  let byteIdx = blockIndex div 8
  let bitmapEnd = bitmapOffset(newIndexSize) + byteIdx.int + 1
  let fileSize = ((bitmapEnd + GrowthChunk - 1) div GrowthChunk) * GrowthChunk
  (fileSize, newIndexSize)

proc growFile(bm: FileBlockmap, newSize: int, newIndexSize: uint32): BResult[void] =
  if bm.readOnly:
    return err(ioError("Cannot grow read-only blockmap"))

  var oldBitmapData: seq[byte] = @[]
  let oldIndexSize = bm.indexSize
  let oldBitmapOffset = bitmapOffset(oldIndexSize)
  let newBitmapOffset = bitmapOffset(newIndexSize)

  if newIndexSize > oldIndexSize and oldIndexSize > 0:
    let bitmapSize = bm.fileSize - oldBitmapOffset
    if bitmapSize > 0:
      oldBitmapData = newSeq[byte](bitmapSize)
      copyMem(addr oldBitmapData[0], cast[pointer](cast[uint](bm.file.mem) + oldBitmapOffset.uint), bitmapSize)

  bm.file.close()

  try:
    let fd = posix.open(bm.path.cstring, O_RDWR)
    if fd < 0:
      return err(ioError("Failed to open file for truncate"))
    if ftruncate(fd, newSize.Off) != 0:
      discard posix.close(fd)
      return err(ioError("Failed to truncate file"))
    discard posix.close(fd)
  except OSError as e:
    return err(ioError("Failed to grow file: " & e.msg))

  try:
    bm.file = memfiles.open(bm.path, fmReadWrite, mappedSize = newSize)
  except OSError as e:
    return err(ioError("Failed to remap file: " & e.msg))

  bm.fileSize = newSize
  headerIndexSize(bm.file.mem)[] = newIndexSize

  for i in oldIndexSize ..< newIndexSize:
    let p = cast[ptr uint8](cast[uint](bm.file.mem) + indexOffset().uint + i.uint)
    p[] = ChunkEmpty

  if oldBitmapData.len > 0:
    copyMem(cast[pointer](cast[uint](bm.file.mem) + newBitmapOffset.uint), addr oldBitmapData[0], oldBitmapData.len)
    if newBitmapOffset > oldBitmapOffset:
      let gapSize = min(newBitmapOffset - oldBitmapOffset, oldBitmapData.len)
      zeroMem(cast[pointer](cast[uint](bm.file.mem) + oldBitmapOffset.uint), gapSize)

  bm.indexSize = newIndexSize
  ok()

proc ensureCapacity(bm: FileBlockmap, blockIndex: uint64): BResult[void] =
  let (neededSize, neededIndexSize) = neededFileSize(blockIndex, bm.indexSize)
  if neededSize <= bm.fileSize and neededIndexSize <= bm.indexSize:
    return ok()
  ?bm.growFile(max(neededSize, bm.fileSize), max(neededIndexSize, bm.indexSize))
  ok()

proc get*(bm: FileBlockmap, index: uint64): bool {.inline.} =
  if index >= bm.maxIndex:
    return false

  let chunkIdx = (index div BlocksPerChunk).uint32
  let chunkState = bm.getChunkState(chunkIdx)

  if chunkState == ChunkEmpty:
    return false
  if chunkState == ChunkFull:
    return true

  let byteIdx = index div 8
  let bitIdx = index mod 8
  let p = bm.bitmapBytePtr(byteIdx)
  if p == nil:
    return false
  (p[] and (1'u8 shl bitIdx)) != 0

proc set*(bm: FileBlockmap, index: uint64): BResult[void] =
  if bm.readOnly:
    return err(ioError("Cannot write to read-only blockmap"))

  ?bm.ensureCapacity(index)

  let chunkIdx = (index div BlocksPerChunk).uint32
  let chunkState = bm.getChunkState(chunkIdx)

  if chunkState == ChunkFull:
    return ok()

  let byteIdx = index div 8
  let bitIdx = index mod 8
  let p = bm.bitmapBytePtr(byteIdx)
  if p != nil:
    p[] = p[] or (1'u8 shl bitIdx)

  if chunkState == ChunkEmpty:
    bm.setChunkState(chunkIdx, ChunkPartial)

  if index + 1 > bm.maxIndex:
    bm.maxIndex = index + 1
    headerMaxIndex(bm.file.mem)[] = bm.maxIndex

  ok()

proc clear*(bm: FileBlockmap, index: uint64): BResult[void] =
  if bm.readOnly:
    return err(ioError("Cannot write to read-only blockmap"))

  if index >= bm.maxIndex:
    return ok()

  let chunkIdx = (index div BlocksPerChunk).uint32
  let chunkState = bm.getChunkState(chunkIdx)

  if chunkState == ChunkEmpty:
    return ok()

  let byteIdx = index div 8
  let bitIdx = index mod 8
  let p = bm.bitmapBytePtr(byteIdx)
  if p != nil:
    p[] = p[] and not (1'u8 shl bitIdx)

  if chunkState == ChunkFull:
    bm.setChunkState(chunkIdx, ChunkPartial)

  ok()

proc countChunkBits(bm: FileBlockmap, chunkIdx: uint32): int =
  let startBlock = chunkIdx.uint64 * BlocksPerChunk
  let endBlock = min(startBlock + BlocksPerChunk, bm.maxIndex)
  if startBlock >= endBlock:
    return 0

  let startByte = startBlock div 8
  let endByte = (endBlock + 7) div 8

  result = 0
  for i in startByte ..< endByte:
    let p = bm.bitmapBytePtr(i)
    if p != nil:
      result += countSetBits(p[])

proc compactIndex*(bm: FileBlockmap) =
  if bm.readOnly:
    return

  for i in 0'u32 ..< bm.indexSize:
    let state = bm.getChunkState(i)
    if state == ChunkPartial:
      let bits = bm.countChunkBits(i)
      let startBlock = i.uint64 * BlocksPerChunk
      let blocksInChunk = min(BlocksPerChunk.uint64, bm.maxIndex - startBlock).int

      if bits == 0:
        bm.setChunkState(i, ChunkEmpty)
      elif bits == blocksInChunk:
        bm.setChunkState(i, ChunkFull)

proc countOnes*(bm: FileBlockmap): uint64 =
  result = 0
  for i in 0'u32 ..< bm.indexSize:
    let state = bm.getChunkState(i)
    case state
    of ChunkEmpty:
      discard
    of ChunkFull:
      let startBlock = i.uint64 * BlocksPerChunk
      result += min(BlocksPerChunk.uint64, bm.maxIndex - startBlock)
    else:
      result += bm.countChunkBits(i).uint64

proc isComplete*(bm: FileBlockmap, totalBlocks: uint64): bool =
  if bm.maxIndex < totalBlocks:
    return false
  let neededChunks = ((totalBlocks + BlocksPerChunk - 1) div BlocksPerChunk).uint32
  for i in 0'u32 ..< neededChunks:
    if bm.getChunkState(i) != ChunkFull:
      return false
  true

proc isEmpty*(bm: FileBlockmap): bool =
  for i in 0'u32 ..< bm.indexSize:
    if bm.getChunkState(i) != ChunkEmpty:
      return false
  true

proc maxBlockIndex*(bm: FileBlockmap): uint64 =
  bm.maxIndex

proc toRanges*(bm: FileBlockmap): seq[BlockRange] =
  result = @[]
  if bm.indexSize == 0:
    return

  var currentStart: uint64 = 0
  var inRange = false

  for i in 0'u32 ..< bm.indexSize:
    let state = bm.getChunkState(i)
    let chunkStart = i.uint64 * BlocksPerChunk
    let chunkEnd = min(chunkStart + BlocksPerChunk, bm.maxIndex)

    case state
    of ChunkFull:
      if not inRange:
        currentStart = chunkStart
        inRange = true

      if i == bm.indexSize - 1 or bm.getChunkState(i + 1) != ChunkFull:
        result.add(BlockRange(start: currentStart, count: chunkEnd - currentStart))
        inRange = false

    of ChunkEmpty:
      if inRange:
        result.add(BlockRange(start: currentStart, count: chunkStart - currentStart))
        inRange = false

    of ChunkPartial:
      if inRange:
        result.add(BlockRange(start: currentStart, count: chunkStart - currentStart))
        inRange = false

      var j = chunkStart
      while j < chunkEnd:
        if bm.get(j):
          let rangeStart = j
          while j < chunkEnd and bm.get(j):
            inc j
          result.add(BlockRange(start: rangeStart, count: j - rangeStart))
        else:
          inc j

    else:
      discard

proc flush*(bm: FileBlockmap) =
  if not bm.readOnly:
    bm.file.flush()

proc close*(bm: FileBlockmap) =
  if bm.file.mem != nil:
    bm.flush()
    bm.file.close()

proc setAll*(bm: FileBlockmap, totalBlocks: uint64): BResult[void] =
  if bm.readOnly:
    return err(ioError("Cannot write to read-only blockmap"))

  if totalBlocks == 0:
    return ok()

  ?bm.ensureCapacity(totalBlocks - 1)

  let fullBytes = totalBlocks div 8
  let remainderBits = totalBlocks mod 8

  for i in 0'u64 ..< fullBytes:
    let p = bm.bitmapBytePtr(i)
    if p != nil:
      p[] = 0xFF'u8

  if remainderBits > 0:
    let p = bm.bitmapBytePtr(fullBytes)
    if p != nil:
      p[] = (1'u8 shl remainderBits) - 1

  bm.maxIndex = totalBlocks
  headerMaxIndex(bm.file.mem)[] = totalBlocks

  let chunkCount = ((totalBlocks + BlocksPerChunk - 1) div BlocksPerChunk).uint32
  for i in 0'u32 ..< chunkCount:
    bm.setChunkState(i, ChunkFull)

  ok()

proc finalize*(bm: FileBlockmap, totalBlocks: uint64): BResult[void] =
  if bm.readOnly:
    return ok()

  if totalBlocks > bm.maxIndex:
    bm.maxIndex = totalBlocks
    headerMaxIndex(bm.file.mem)[] = totalBlocks

  bm.compactIndex()
  bm.flush()
  ok()

proc newFileBlockmap*(path: string, forWriting: bool = true): BResult[FileBlockmap] =
  let parentDir = parentDir(path)
  if not dirExists(parentDir):
    try:
      createDir(parentDir)
    except OSError as e:
      return err(ioError("Failed to create directory: " & e.msg))

  var isNew = not fileExists(path)

  if isNew and not forWriting:
    return err(ioError("Blockmap file does not exist: " & path))

  var initialSize = HeaderSize + GrowthChunk

  if isNew:
    try:
      let fd = posix.open(path.cstring, O_RDWR or O_CREAT, 0o644)
      if fd < 0:
        return err(ioError("Failed to create blockmap file"))
      if ftruncate(fd, initialSize.Off) != 0:
        discard posix.close(fd)
        return err(ioError("Failed to set initial file size"))
      discard posix.close(fd)
    except OSError as e:
      return err(ioError("Failed to create blockmap file: " & e.msg))
  else:
    try:
      initialSize = getFileSize(path).int
    except OSError as e:
      return err(ioError("Failed to get file size: " & e.msg))

  let mode = if forWriting: fmReadWrite else: fmRead
  var mf: MemFile
  try:
    mf = memfiles.open(path, mode, mappedSize = initialSize)
  except OSError as e:
    return err(ioError("Failed to mmap blockmap: " & e.msg))

  var bm = FileBlockmap(
    path: path,
    file: mf,
    fileSize: initialSize,
    maxIndex: 0,
    indexSize: 0,
    readOnly: not forWriting
  )

  if isNew:
    let header = cast[ptr uint32](mf.mem)
    header[] = BlockmapMagic
    cast[ptr uint8](cast[uint](mf.mem) + 4)[] = BlockmapVersion
    headerMaxIndex(mf.mem)[] = 0
    headerIndexSize(mf.mem)[] = 0
  else:
    let magic = cast[ptr uint32](mf.mem)[]
    if magic != BlockmapMagic:
      mf.close()
      return err(ioError("Invalid blockmap magic"))
    let version = cast[ptr uint8](cast[uint](mf.mem) + 4)[]
    if version != BlockmapVersion:
      mf.close()
      return err(ioError("Unsupported blockmap version"))
    bm.maxIndex = headerMaxIndex(mf.mem)[]
    bm.indexSize = headerIndexSize(mf.mem)[]

  ok(bm)

proc getBlockmapPath*(blockmapsDir: string, treeCid: Cid): string =
  getShardedPath(blockmapsDir, treeCid, ".blkmap")

proc getBlockmapPathStr*(blockmapsDir: string, treeCidStr: string): string =
  getShardedPathStr(blockmapsDir, treeCidStr, ".blkmap")

proc toSeqByte*(bm: FileBlockmap): seq[byte] =
  let bitmapSize = (bm.maxIndex + 7) div 8
  result = newSeq[byte](bitmapSize.int)
  for i in 0'u64 ..< bitmapSize:
    let p = bm.bitmapBytePtr(i)
    if p != nil:
      result[i.int] = p[]

proc fromSeqByte*(bm: FileBlockmap, data: seq[byte]): BResult[void] =
  if bm.readOnly:
    return err(ioError("Cannot write to read-only blockmap"))

  let maxIndex = data.len.uint64 * 8
  ?bm.ensureCapacity(maxIndex - 1)

  for i in 0'u64 ..< data.len.uint64:
    let p = bm.bitmapBytePtr(i)
    if p != nil:
      p[] = data[i.int]

  bm.maxIndex = maxIndex
  headerMaxIndex(bm.file.mem)[] = maxIndex

  let chunkCount = ((maxIndex + BlocksPerChunk - 1) div BlocksPerChunk).uint32
  for i in 0'u32 ..< chunkCount:
    bm.setChunkState(i, ChunkPartial)
  bm.compactIndex()

  ok()
