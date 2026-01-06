import std/[os, times, strformat, random, options, strutils]
import chronos
import taskpools
import results
import ../blockstore/errors
import ../blockstore/blocks
import ../blockstore/chunker
import ../blockstore/dataset
import ../blockstore/cid
import ../blockstore/merkle
import ../blockstore/ioutils
import ../blockstore/blockmap

when defined(posix):
  import std/posix
elif defined(windows):
  import std/winlean

const
  DefaultSize = 4'u64 * 1024 * 1024 * 1024
  DefaultChunkSize = 64 * 1024
  DefaultPoolSize = 4
  TestDir = "nim_blockstore_bench"
  TestFile = TestDir / "testfile.bin"
  DbPath = TestDir / "bench_db"
  BlocksDir = TestDir / "blocks"

type
  BenchConfig = object
    totalSize: uint64
    chunkSize: int
    merkleBackend: MerkleBackend
    blockBackend: BlockBackend
    blockmapBackend: BlockmapBackend
    ioMode: IOMode
    syncBatchSize: int
    synthetic: bool
    reportInterval: float
    poolSize: int
    blockHashConfig: BlockHashConfig
    randomReads: bool

proc formatSize(bytes: uint64): string =
  if bytes >= 1024'u64 * 1024 * 1024 * 1024:
    &"{bytes.float / (1024 * 1024 * 1024 * 1024):.2f} TB"
  elif bytes >= 1024'u64 * 1024 * 1024:
    &"{bytes.float / (1024 * 1024 * 1024):.2f} GB"
  elif bytes >= 1024'u64 * 1024:
    &"{bytes.float / (1024 * 1024):.2f} MB"
  else:
    &"{bytes} bytes"

proc formatRate(bytesPerSec: float): string =
  if bytesPerSec >= 1024 * 1024 * 1024:
    &"{bytesPerSec / (1024 * 1024 * 1024):.2f} GB/s"
  elif bytesPerSec >= 1024 * 1024:
    &"{bytesPerSec / (1024 * 1024):.2f} MB/s"
  else:
    &"{bytesPerSec / 1024:.2f} KB/s"

proc parseSize(s: string): uint64 =
  var num = s
  var multiplier: uint64 = 1

  if s.endsWith("TB") or s.endsWith("tb"):
    num = s[0..^3]
    multiplier = 1024'u64 * 1024 * 1024 * 1024
  elif s.endsWith("GB") or s.endsWith("gb"):
    num = s[0..^3]
    multiplier = 1024'u64 * 1024 * 1024
  elif s.endsWith("MB") or s.endsWith("mb"):
    num = s[0..^3]
    multiplier = 1024'u64 * 1024
  elif s.endsWith("KB") or s.endsWith("kb"):
    num = s[0..^3]
    multiplier = 1024'u64

  try:
    result = uint64(parseInt(num)) * multiplier
  except ValueError:
    result = DefaultSize

proc syncFile(f: File) =
  flushFile(f)
  when defined(posix):
    discard fsync(f.getFileHandle().cint)
  elif defined(windows):
    discard flushFileBuffers(f.getFileHandle())

proc createTestFile(path: string, size: uint64) =
  echo &"Creating {formatSize(size)} test file..."
  let startTime = epochTime()

  randomize()
  var f = open(path, fmWrite)

  const bufSize = 1024 * 1024
  var buf = newSeq[byte](bufSize)
  var remaining = size

  while remaining > 0:
    for i in 0 ..< bufSize:
      buf[i] = byte(rand(255))

    let writeSize = min(remaining, bufSize.uint64)
    discard f.writeBytes(buf, 0, writeSize.int)
    remaining -= writeSize

  syncFile(f)
  f.close()

  let elapsed = epochTime() - startTime
  let rate = size.float / elapsed
  echo &"  Created in {elapsed:.2f}s ({formatRate(rate)})"

proc cleanup() =
  if dirExists(TestDir):
    removeDir(TestDir)

proc runBenchmark(config: BenchConfig) {.async.} =
  echo "=== Dataset Ingestion Benchmark ==="
  echo &"Size: {formatSize(config.totalSize)}"
  echo &"Chunk size: {config.chunkSize div 1024} KB"
  echo &"Expected blocks: {config.totalSize div config.chunkSize.uint64}"
  echo &"Merkle backend: {config.merkleBackend}"
  echo &"Block backend: {config.blockBackend}"
  echo &"Blockmap backend: {config.blockmapBackend}"
  echo &"IO mode: {config.ioMode}"
  echo &"Sync batch size: {config.syncBatchSize}"
  echo &"Thread pool size: {config.poolSize}"
  echo &"Data mode: {(if config.synthetic: \"synthetic\" else: \"file-based (async)\")}"
  echo &"Read order: {(if config.randomReads: \"random\" else: \"sequential\")}"
  echo ""

  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  if not config.synthetic:
    createTestFile(TestFile, config.totalSize)
    echo ""

  echo "Initializing dataset store..."
  let storeResult = newDatasetStore(DbPath, BlocksDir,
                                    blockHashConfig = config.blockHashConfig,
                                    merkleBackend = config.merkleBackend,
                                    blockBackend = config.blockBackend,
                                    blockmapBackend = config.blockmapBackend,
                                    ioMode = config.ioMode,
                                    syncBatchSize = config.syncBatchSize)
  if storeResult.isErr:
    echo &"Failed to create store: {storeResult.error}"
    return

  let store = storeResult.value

  let filename = if config.synthetic: some("benchmark") else: some(TestFile)
  let builderResult = store.startDataset(config.chunkSize.uint32, filename)
  if builderResult.isErr:
    echo &"Failed to start dataset: {builderResult.error}"
    return

  var builder = builderResult.value

  echo "Ingesting blocks..."
  let ingestStart = epochTime()
  var blockCount: uint64 = 0
  var totalBytes: uint64 = 0
  var lastReport = ingestStart
  var lastBytes: uint64 = 0
  let totalBlocks = config.totalSize div config.chunkSize.uint64

  if config.synthetic:
    var chunk = newSeq[byte](config.chunkSize)
    randomize()
    for i in 0 ..< config.chunkSize:
      chunk[i] = byte(rand(255))

    while totalBytes < config.totalSize:
      chunk[0] = byte(blockCount and 0xFF)
      chunk[1] = byte((blockCount shr 8) and 0xFF)
      chunk[2] = byte((blockCount shr 16) and 0xFF)
      chunk[3] = byte((blockCount shr 24) and 0xFF)

      let blkResult = newBlock(chunk, config.blockHashConfig)
      if blkResult.isErr:
        echo &"Failed to create block: {blkResult.error}"
        break

      let blk = blkResult.value
      totalBytes += blk.data.len.uint64

      let addResult = await builder.addBlock(blk)
      if addResult.isErr:
        echo &"Failed to add block: {addResult.error}"
        break

      blockCount += 1

      let now = epochTime()
      if now - lastReport >= config.reportInterval:
        let intervalBytes = totalBytes - lastBytes
        let intervalRate = intervalBytes.float / (now - lastReport)
        let overallRate = totalBytes.float / (now - ingestStart)
        let progress = (blockCount.float / totalBlocks.float) * 100
        let eta = if overallRate > 0: (config.totalSize - totalBytes).float / overallRate else: 0.0
        echo &"  Progress: {progress:.1f}% | Blocks: {blockCount}/{totalBlocks} | Rate: {formatRate(intervalRate)} (avg: {formatRate(overallRate)}) | ETA: {eta:.0f}s"
        lastReport = now
        lastBytes = totalBytes
  else:
    var pool = Taskpool.new(numThreads = config.poolSize)
    defer: pool.shutdown()

    let streamResult = await builder.chunkFile(pool)
    if streamResult.isErr:
      echo &"Failed to open file: {streamResult.error}"
      return

    var stream = streamResult.value

    while true:
      let blockOpt = await stream.nextBlock()
      if blockOpt.isNone:
        break

      let blockResult = blockOpt.get()
      if blockResult.isErr:
        echo &"Block read error: {blockResult.error}"
        break

      let blk = blockResult.value
      totalBytes += blk.data.len.uint64

      let addResult = await builder.addBlock(blk)
      if addResult.isErr:
        echo &"Failed to add block: {addResult.error}"
        break

      blockCount += 1

      let now = epochTime()
      if now - lastReport >= config.reportInterval:
        let intervalBytes = totalBytes - lastBytes
        let intervalRate = intervalBytes.float / (now - lastReport)
        let overallRate = totalBytes.float / (now - ingestStart)
        let progress = (blockCount.float / totalBlocks.float) * 100
        let eta = if overallRate > 0: (config.totalSize - totalBytes).float / overallRate else: 0.0
        echo &"  Progress: {progress:.1f}% | Blocks: {blockCount}/{totalBlocks} | Rate: {formatRate(intervalRate)} (avg: {formatRate(overallRate)}) | ETA: {eta:.0f}s"
        lastReport = now
        lastBytes = totalBytes

    stream.close()

  let ingestEnd = epochTime()
  let ingestTime = ingestEnd - ingestStart
  let ingestRate = totalBytes.float / ingestTime

  echo ""
  echo "Ingestion complete:"
  echo &"  Blocks: {blockCount}"
  echo &"  Bytes: {formatSize(totalBytes)}"
  echo &"  Time: {ingestTime:.2f}s"
  echo &"  Rate: {formatRate(ingestRate)}"
  echo ""

  echo "Finalizing dataset (building merkle tree)..."
  let finalizeStart = epochTime()

  let datasetResult = await builder.finalize()
  if datasetResult.isErr:
    echo &"Failed to finalize: {datasetResult.error}"
    return

  let dataset = datasetResult.value
  let finalizeEnd = epochTime()
  let finalizeTime = finalizeEnd - finalizeStart

  echo &"  Finalize time: {finalizeTime:.2f}s"
  echo ""

  let totalTime = ingestTime + finalizeTime
  let overallRate = totalBytes.float / totalTime

  echo "=== Write Summary ==="
  echo &"  Dataset manifest CID: {dataset.manifestCid}"
  echo &"  Dataset tree CID: {dataset.treeCid}"
  echo &"  Total blocks: {dataset.blockCount}"
  echo &"  Total time: {totalTime:.2f}s"
  echo &"  Overall rate: {formatRate(overallRate)}"
  echo &"  Storage used: {formatSize(store.used())}"
  echo ""

  let readOrder = if config.randomReads:
    echo "=== Read Benchmark (without verification, RANDOM order) ==="
    var indices = newSeq[uint32](dataset.blockCount)
    for i in 0 ..< dataset.blockCount:
      indices[i] = i.uint32
    randomize()
    shuffle(indices)
    indices
  else:
    echo "=== Read Benchmark (without verification) ==="
    var indices = newSeq[uint32](dataset.blockCount)
    for i in 0 ..< dataset.blockCount:
      indices[i] = i.uint32
    indices

  echo "Reading all blocks..."

  let readStart = epochTime()
  var readBytes: uint64 = 0
  var readBlocks = 0
  var lastReadReport = readStart
  var lastReadBytes: uint64 = 0

  for idx in readOrder:
    let blockResult = await dataset.getBlock(idx.int)
    if blockResult.isErr:
      echo &"Failed to read block {idx}: {blockResult.error}"
      break

    let blockOpt = blockResult.value
    if blockOpt.isNone:
      echo &"Block {idx} not found"
      break

    let (blk, _) = blockOpt.get()
    readBytes += blk.data.len.uint64
    readBlocks += 1

    let now = epochTime()
    if now - lastReadReport >= config.reportInterval:
      let intervalBytes = readBytes - lastReadBytes
      let intervalRate = intervalBytes.float / (now - lastReadReport)
      let overallReadRate = readBytes.float / (now - readStart)
      let progress = (readBytes.float / totalBytes.float) * 100
      echo &"  Progress: {progress:.1f}% | Blocks: {readBlocks} | Rate: {formatRate(intervalRate)} (avg: {formatRate(overallReadRate)})"
      lastReadReport = now
      lastReadBytes = readBytes

  let readEnd = epochTime()
  let readTime = readEnd - readStart
  let readRate = readBytes.float / readTime

  echo ""
  echo &"Read complete (no verification{(if config.randomReads: \", random\" else: \"\")}):"
  echo &"  Blocks read: {readBlocks}"
  echo &"  Bytes read: {formatSize(readBytes)}"
  echo &"  Time: {readTime:.2f}s"
  echo &"  Rate: {formatRate(readRate)}"
  echo ""

  if config.randomReads:
    echo "=== Read Benchmark (with verification, RANDOM order) ==="
  else:
    echo "=== Read Benchmark (with verification) ==="
  echo "Reading and verifying all blocks..."

  let mhashResult = dataset.treeCid.mhash()
  if mhashResult.isErr:
    echo &"Failed to get multihash from treeCid: {mhashResult.error}"
    return

  let mhash = mhashResult.value
  var rootHash: MerkleHash
  let digestBytes = mhash.data.buffer
  if digestBytes.len >= HashSize + 2:
    copyMem(addr rootHash[0], unsafeAddr digestBytes[2], HashSize)
  else:
    echo "Invalid multihash length"
    return

  let verifyStart = epochTime()
  var verifiedBlocks = 0
  var verifiedBytes: uint64 = 0
  var verifyFailed = 0
  var lastVerifyReport = verifyStart
  var lastVerifyBytes: uint64 = 0

  for idx in readOrder:
    let blockResult = await dataset.getBlock(idx.int)
    if blockResult.isErr:
      echo &"Failed to read block {idx}: {blockResult.error}"
      break

    let blockOpt = blockResult.value
    if blockOpt.isNone:
      echo &"Block {idx} not found"
      break

    let (blk, proof) = blockOpt.get()

    let leafHash = config.blockHashConfig.hashFunc(blk.data)
    if not verify(proof, rootHash, leafHash):
      verifyFailed += 1
      if verifyFailed <= 5:
        echo &"  WARNING: Block {idx} verification failed!"

    verifiedBlocks += 1
    verifiedBytes += blk.data.len.uint64

    let now = epochTime()
    if now - lastVerifyReport >= config.reportInterval:
      let intervalBytes = verifiedBytes - lastVerifyBytes
      let intervalRate = intervalBytes.float / (now - lastVerifyReport)
      let overallVerifyRate = verifiedBytes.float / (now - verifyStart)
      let progress = (verifiedBytes.float / totalBytes.float) * 100
      echo &"  Progress: {progress:.1f}% | Verified: {verifiedBlocks} | Failed: {verifyFailed} | Rate: {formatRate(intervalRate)} (avg: {formatRate(overallVerifyRate)})"
      lastVerifyReport = now
      lastVerifyBytes = verifiedBytes

  let verifyEnd = epochTime()
  let verifyTime = verifyEnd - verifyStart
  let verifyRate = verifiedBytes.float / verifyTime

  echo ""
  echo &"Read with verification complete{(if config.randomReads: \" (random)\" else: \"\")}:"
  echo &"  Blocks verified: {verifiedBlocks}"
  echo &"  Verification failures: {verifyFailed}"
  echo &"  Bytes verified: {formatSize(verifiedBytes)}"
  echo &"  Time: {verifyTime:.2f}s"
  echo &"  Rate: {formatRate(verifyRate)}"
  echo ""

  echo "Closing store..."
  await store.closeAsync()

  echo "Cleaning up..."
  cleanup()
  echo "Done!"

proc printUsage() =
  echo "Usage: bench_dataset [options]"
  echo ""
  echo "Options:"
  echo "  --size=<size>        Dataset size (e.g., 1GB, 4GB, 100GB, 1TB)"
  echo "  --chunk=<size>       Chunk size in KB (default: 64)"
  echo "  --merkle=<type>      Merkle backend: embedded, leveldb, packed (default: packed)"
  echo "  --blocks=<type>      Block backend: sharded, packed (default: sharded)"
  echo "  --blockmap=<type>    Blockmap backend: leveldb, file (default: leveldb)"
  echo "  --io=<mode>          I/O mode: direct, buffered (default: direct)"
  echo "  --sync=<value>       Sync batch: none, every, or N (default: none)"
  echo "  --pool=<size>        Thread pool size for async I/O (default: 4, min: 2)"
  echo "  --synthetic          Use synthetic in-memory data (no file I/O)"
  echo "  --random             Read blocks in random order (default: sequential)"
  echo "  --help               Show this help"

proc main() =
  var config = BenchConfig(
    totalSize: DefaultSize,
    chunkSize: DefaultChunkSize,
    merkleBackend: mbPacked,
    blockBackend: bbSharded,
    blockmapBackend: bmLevelDb,
    ioMode: ioDirect,
    syncBatchSize: 0,
    synthetic: false,
    reportInterval: 1.0,
    poolSize: DefaultPoolSize,
    blockHashConfig: defaultBlockHashConfig(),
    randomReads: false
  )

  for arg in commandLineParams():
    if arg.startsWith("--size="):
      config.totalSize = parseSize(arg[7..^1])
    elif arg.startsWith("--chunk="):
      config.chunkSize = parseInt(arg[8..^1]) * 1024
    elif arg.startsWith("--merkle="):
      let backend = arg[9..^1]
      case backend
      of "embedded", "embeddedproofs": config.merkleBackend = mbEmbeddedProofs
      of "leveldb": config.merkleBackend = mbLevelDb
      of "packed": config.merkleBackend = mbPacked
      else: echo &"Unknown merkle backend: {backend}"; return
    elif arg.startsWith("--blocks="):
      let backend = arg[9..^1]
      case backend
      of "sharded": config.blockBackend = bbSharded
      of "packed": config.blockBackend = bbPacked
      else: echo &"Unknown block backend: {backend}"; return
    elif arg.startsWith("--blockmap="):
      let backend = arg[11..^1]
      case backend
      of "leveldb": config.blockmapBackend = bmLevelDb
      of "file": config.blockmapBackend = bmFile
      else: echo &"Unknown blockmap backend: {backend}"; return
    elif arg.startsWith("--io="):
      let mode = arg[5..^1]
      case mode
      of "direct": config.ioMode = ioDirect
      of "buffered": config.ioMode = ioBuffered
      else: echo &"Unknown IO mode: {mode}"; return
    elif arg.startsWith("--sync="):
      let value = arg[7..^1]
      if value == "none":
        config.syncBatchSize = 0
      elif value == "every":
        config.syncBatchSize = 1
      else:
        try:
          config.syncBatchSize = parseInt(value)
        except ValueError:
          echo &"Invalid sync batch size: {value}"; return
    elif arg.startsWith("--pool="):
      try:
        config.poolSize = max(2, parseInt(arg[7..^1]))
      except ValueError:
        echo &"Invalid pool size: {arg[7..^1]}"; return
    elif arg == "--synthetic":
      config.synthetic = true
    elif arg == "--random":
      config.randomReads = true
    elif arg == "--help":
      printUsage()
      return

  waitFor runBenchmark(config)

when isMainModule:
  main()
