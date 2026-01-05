import std/[unittest, os, options]
import chronos
import taskpools
import results
import ../blockstore/errors
import ../blockstore/blocks
import ../blockstore/chunker

const testDir = getTempDir() / "nim_blockstore_test"

suite "Chunker tests":
  setup:
    createDir(testDir)

  teardown:
    removeDir(testDir)

  test "chunk small file":
    let testFile = testDir / "small.txt"
    let data = "hello world"
    writeFile(testFile, data)

    let chunker = newSyncChunker()
    let iterResult = chunker.chunkFile(testFile)
    check iterResult.isOk

    var iter = iterResult.value
    var blocks: seq[Block] = @[]

    while true:
      let blockOpt = iter.nextBlock()
      if blockOpt.isNone:
        break
      check blockOpt.get().isOk
      blocks.add(blockOpt.get().value)

    iter.close()

    check blocks.len == 1
    check blocks[0].data == cast[seq[byte]](data)

  test "chunk exact chunk size":
    let testFile = testDir / "exact.txt"
    let chunkSize = 1024
    var data = newSeq[byte](chunkSize)
    for i in 0 ..< chunkSize:
      data[i] = 42'u8
    writeFile(testFile, cast[string](data))

    let config = newChunkerConfig(chunkSize)
    let chunker = newSyncChunker(config)
    let iterResult = chunker.chunkFile(testFile)
    check iterResult.isOk

    var iter = iterResult.value
    var blocks: seq[Block] = @[]

    while true:
      let blockOpt = iter.nextBlock()
      if blockOpt.isNone:
        break
      check blockOpt.get().isOk
      blocks.add(blockOpt.get().value)

    iter.close()

    check blocks.len == 1
    check blocks[0].data.len == chunkSize

  test "chunk multiple chunks":
    let testFile = testDir / "multi.txt"
    let chunkSize = 1024
    let totalSize = chunkSize * 2 + 512
    var data = newSeq[byte](totalSize)
    for i in 0 ..< totalSize:
      data[i] = 42'u8
    writeFile(testFile, cast[string](data))

    let config = newChunkerConfig(chunkSize)
    let chunker = newSyncChunker(config)
    let iterResult = chunker.chunkFile(testFile)
    check iterResult.isOk

    var iter = iterResult.value
    var blocks: seq[Block] = @[]

    while true:
      let blockOpt = iter.nextBlock()
      if blockOpt.isNone:
        break
      check blockOpt.get().isOk
      blocks.add(blockOpt.get().value)

    iter.close()

    check blocks.len == 3
    check blocks[0].data.len == chunkSize
    check blocks[1].data.len == chunkSize
    check blocks[2].data.len == 512

  test "chunk empty file":
    let testFile = testDir / "empty.txt"
    writeFile(testFile, "")

    let chunker = newSyncChunker()
    let iterResult = chunker.chunkFile(testFile)
    check iterResult.isOk

    var iter = iterResult.value
    let blockOpt = iter.nextBlock()
    iter.close()

    check blockOpt.isNone

  test "unique block CIDs":
    let testFile = testDir / "unique.txt"
    writeFile(testFile, "aaaaaaaaaabbbbbbbbbb")

    let config = newChunkerConfig(10)
    let chunker = newSyncChunker(config)
    let iterResult = chunker.chunkFile(testFile)
    check iterResult.isOk

    var iter = iterResult.value
    var blocks: seq[Block] = @[]

    while true:
      let blockOpt = iter.nextBlock()
      if blockOpt.isNone:
        break
      check blockOpt.get().isOk
      blocks.add(blockOpt.get().value)

    iter.close()

    check blocks.len == 2
    check blocks[0].cid != blocks[1].cid

  test "chunkData helper":
    let data = cast[seq[byte]]("hello world, this is a test of chunking")
    let chunkSize = 10
    let blocksResults = chunkData(data, chunkSize)

    check blocksResults.len == 4

    for br in blocksResults:
      check br.isOk

    check blocksResults[^1].value.data.len == 9

  test "file not found error":
    let chunker = newSyncChunker()
    let iterResult = chunker.chunkFile("/nonexistent/file.txt")

    check iterResult.isErr
    check iterResult.error.kind == IoError

proc readBlocksAsync(pool: Taskpool, filePath: string): Future[seq[Block]] {.async.} =
  let chunker = newAsyncChunker(pool)
  let streamResult = await chunker.chunkFile(filePath)
  doAssert streamResult.isOk
  var stream = streamResult.value
  result = @[]
  while true:
    let blockOpt = await stream.nextBlock()
    if blockOpt.isNone:
      break
    doAssert blockOpt.get().isOk
    result.add(blockOpt.get().value)
  stream.close()

proc readBlocksAsyncWithConfig(pool: Taskpool, filePath: string, chunkSize: int): Future[seq[Block]] {.async.} =
  let config = newChunkerConfig(chunkSize)
  let chunker = newAsyncChunker(pool, config)
  let streamResult = await chunker.chunkFile(filePath)
  doAssert streamResult.isOk
  var stream = streamResult.value
  result = @[]
  while true:
    let blockOpt = await stream.nextBlock()
    if blockOpt.isNone:
      break
    doAssert blockOpt.get().isOk
    result.add(blockOpt.get().value)
  stream.close()

proc readTwoFilesAsync(pool: Taskpool, file1, file2: string): Future[(Block, Block)] {.async.} =
  let chunker1 = newAsyncChunker(pool)
  let chunker2 = newAsyncChunker(pool)

  let stream1Result = await chunker1.chunkFile(file1)
  let stream2Result = await chunker2.chunkFile(file2)
  doAssert stream1Result.isOk
  doAssert stream2Result.isOk

  var stream1 = stream1Result.value
  var stream2 = stream2Result.value

  let block1Opt = await stream1.nextBlock()
  let block2Opt = await stream2.nextBlock()
  doAssert block1Opt.isSome
  doAssert block2Opt.isSome
  doAssert block1Opt.get().isOk
  doAssert block2Opt.get().isOk

  stream1.close()
  stream2.close()
  return (block1Opt.get().value, block2Opt.get().value)

proc openNonexistentAsync(pool: Taskpool): Future[BResult[AsyncChunkStream]] {.async.} =
  let chunker = newAsyncChunker(pool)
  return await chunker.chunkFile("/nonexistent/async_file.txt")

suite "Async Chunker tests":
  var pool: Taskpool

  setup:
    createDir(testDir)
    pool = Taskpool.new(numThreads = 2)

  teardown:
    pool.shutdown()
    removeDir(testDir)

  test "async chunk small file":
    let testFile = testDir / "async_small.txt"
    let data = "hello async world"
    writeFile(testFile, data)

    let blocks = waitFor readBlocksAsync(pool, testFile)
    check blocks.len == 1
    check blocks[0].data == cast[seq[byte]](data)

  test "async chunk multiple chunks":
    let testFile = testDir / "async_multi.txt"
    let chunkSize = 1024
    let totalSize = chunkSize * 3 + 256
    var data = newSeq[byte](totalSize)
    for i in 0 ..< totalSize:
      data[i] = byte(i mod 256)
    writeFile(testFile, cast[string](data))

    let blocks = waitFor readBlocksAsyncWithConfig(pool, testFile, chunkSize)
    check blocks.len == 4
    check blocks[0].data.len == chunkSize
    check blocks[1].data.len == chunkSize
    check blocks[2].data.len == chunkSize
    check blocks[3].data.len == 256

  test "async shared pool across chunkers":
    let testFile1 = testDir / "shared1.txt"
    let testFile2 = testDir / "shared2.txt"
    writeFile(testFile1, "file one content")
    writeFile(testFile2, "file two content")

    let (block1, block2) = waitFor readTwoFilesAsync(pool, testFile1, testFile2)
    check block1.cid != block2.cid

  test "async file not found":
    let streamResult = waitFor openNonexistentAsync(pool)
    check streamResult.isErr
    check streamResult.error.kind == IoError
