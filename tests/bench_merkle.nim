import std/[os, times, strformat, random, options, strutils]
import chronos
import results
import leveldbstatic as leveldb
import ../blockstore/errors
import ../blockstore/blocks
import ../blockstore/dataset
import ../blockstore/merkle
import ../blockstore/sha256
import ../blockstore/cid

proc toHexStr(data: openArray[byte]): string =
  result = ""
  for b in data:
    result.add(b.toHex(2))

const
  DefaultChunkSize = 64 * 1024
  TestDir = "bench_merkle_streaming"
  DbPath = TestDir / "db"
  BlocksDir = TestDir / "blocks"
  TreesDir = TestDir / "trees"

type
  BenchConfig = object
    totalSize: uint64
    chunkSize: int
    backend: MerkleBackend
    storeBlocks: bool
    reportInterval: float

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

proc cleanup() =
  if dirExists(TestDir):
    removeDir(TestDir)

proc runMerkleOnlyBenchmark(config: BenchConfig) =
  echo "=== Merkle Tree Only Benchmark ==="
  echo &"Simulated size: {formatSize(config.totalSize)}"
  echo &"Chunk size: {config.chunkSize div 1024} KB"
  echo &"Expected blocks: {config.totalSize div config.chunkSize.uint64}"
  echo &"Backend: {config.backend}"
  echo ""

  cleanup()
  createDir(TestDir)
  createDir(TreesDir)

  let treeId = "bench_" & $epochTime().int
  var storage: MerkleStorage
  case config.backend
  of mbPacked:
    let treePath = TreesDir / (treeId & ".tree")
    storage = newPackedMerkleStorage(treePath, forWriting = true).get()
  of mbLevelDb:
    let db = leveldb.open(DbPath)
    storage = newLevelDbMerkleStorage(db, "bench")
  of mbEmbeddedProofs:
    echo "Embedded proofs backend not supported for this benchmark"
    return

  var builder = newStreamingMerkleBuilder(storage)

  var chunk = newSeq[byte](config.chunkSize)
  randomize()
  for i in 0 ..< config.chunkSize:
    chunk[i] = byte(rand(255))

  echo "Building merkle tree..."
  let startTime = epochTime()
  var blockCount: uint64 = 0
  var processedBytes: uint64 = 0
  var lastReport = startTime
  var lastBytes: uint64 = 0
  let totalBlocks = config.totalSize div config.chunkSize.uint64

  while processedBytes < config.totalSize:
    chunk[0] = byte(blockCount and 0xFF)
    chunk[1] = byte((blockCount shr 8) and 0xFF)
    chunk[2] = byte((blockCount shr 16) and 0xFF)
    chunk[3] = byte((blockCount shr 24) and 0xFF)

    let leafHash = sha256Hash(chunk)
    let addResult = builder.addLeaf(leafHash)
    if addResult.isErr:
      echo &"Error adding leaf: {addResult.error.msg}"
      return

    blockCount += 1
    processedBytes += config.chunkSize.uint64

    let now = epochTime()
    if now - lastReport >= config.reportInterval:
      let intervalBytes = processedBytes - lastBytes
      let intervalRate = intervalBytes.float / (now - lastReport)
      let overallRate = processedBytes.float / (now - startTime)
      let progress = (blockCount.float / totalBlocks.float) * 100
      let eta = if overallRate > 0: (config.totalSize - processedBytes).float / overallRate else: 0.0
      echo &"  Progress: {progress:.2f}% | Blocks: {blockCount}/{totalBlocks} | Rate: {formatRate(intervalRate)} (avg: {formatRate(overallRate)}) | ETA: {eta:.0f}s"
      lastReport = now
      lastBytes = processedBytes

  echo ""
  echo "Finalizing tree..."
  let finalizeStart = epochTime()

  let rootResult = builder.finalize()
  if rootResult.isErr:
    echo &"Finalize failed: {rootResult.error}"
    return

  let root = rootResult.value
  let finalizeEnd = epochTime()

  var treeFileSize: int64 = 0
  if config.backend == mbPacked:
    let treePath = TreesDir / (treeId & ".tree")
    treeFileSize = getFileSize(treePath)

  discard storage.close()

  let totalTime = finalizeEnd - startTime
  let buildTime = finalizeStart - startTime
  let finalizeTime = finalizeEnd - finalizeStart
  let overallRate = processedBytes.float / totalTime

  echo ""
  echo "=== Results ==="
  echo &"  Root hash: {toHexStr(root[0..7])}..."
  echo &"  Total blocks: {blockCount}"
  echo &"  Simulated data: {formatSize(processedBytes)}"
  echo &"  Build time: {buildTime:.2f}s"
  echo &"  Finalize time: {finalizeTime:.2f}s"
  echo &"  Total time: {totalTime:.2f}s"
  echo &"  Throughput: {formatRate(overallRate)}"
  echo &"  Blocks/sec: {blockCount.float / totalTime:.0f}"
  if treeFileSize > 0:
    echo &"  Tree file size: {formatSize(treeFileSize.uint64)}"
    echo &"  Overhead: {treeFileSize.float / processedBytes.float * 100:.4f}%"
  echo ""

  cleanup()

proc runFullDatasetBenchmark(config: BenchConfig) {.async.} =
  echo "=== Full Dataset Benchmark ==="
  echo &"Simulated size: {formatSize(config.totalSize)}"
  echo &"Chunk size: {config.chunkSize div 1024} KB"
  echo &"Backend: {config.backend}"
  echo ""

  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir, merkleBackend = config.backend)
  if storeResult.isErr:
    echo &"Failed to create store: {storeResult.error}"
    return

  let store = storeResult.value
  defer: store.close()

  let builderResult = store.startDataset(config.chunkSize.uint32, some("benchmark"))
  if builderResult.isErr:
    echo &"Failed to start dataset: {builderResult.error}"
    return

  var builder = builderResult.value

  var chunk = newSeq[byte](config.chunkSize)
  randomize()
  for i in 0 ..< config.chunkSize:
    chunk[i] = byte(rand(255))

  echo "Ingesting blocks..."
  let startTime = epochTime()
  var blockCount: uint64 = 0
  var processedBytes: uint64 = 0
  var lastReport = startTime
  var lastBytes: uint64 = 0

  while processedBytes < config.totalSize:
    chunk[0] = byte(blockCount and 0xFF)
    chunk[1] = byte((blockCount shr 8) and 0xFF)
    chunk[2] = byte((blockCount shr 16) and 0xFF)
    chunk[3] = byte((blockCount shr 24) and 0xFF)

    let blkResult = newBlock(chunk)
    if blkResult.isErr:
      echo &"Failed to create block: {blkResult.error}"
      return

    let addResult = await builder.addBlock(blkResult.value)
    if addResult.isErr:
      echo &"Failed to add block: {addResult.error}"
      return

    blockCount += 1
    processedBytes += config.chunkSize.uint64

    let now = epochTime()
    if now - lastReport >= config.reportInterval:
      let intervalBytes = processedBytes - lastBytes
      let intervalRate = intervalBytes.float / (now - lastReport)
      let overallRate = processedBytes.float / (now - startTime)
      let progress = (processedBytes.float / config.totalSize.float) * 100
      echo &"  Progress: {progress:.1f}% | Blocks: {blockCount} | Rate: {formatRate(intervalRate)} (avg: {formatRate(overallRate)})"
      lastReport = now
      lastBytes = processedBytes

  echo ""
  echo "Finalizing dataset..."
  let finalizeStart = epochTime()

  let datasetResult = await builder.finalize()
  if datasetResult.isErr:
    echo &"Failed to finalize: {datasetResult.error}"
    return

  let dataset = datasetResult.value
  let totalTime = epochTime() - startTime
  let overallRate = processedBytes.float / totalTime

  echo ""
  echo "=== Results ==="
  echo &"  Manifest CID: {dataset.manifestCid}"
  echo &"  Tree CID: {dataset.treeCid}"
  echo &"  Total blocks: {dataset.blockCount}"
  echo &"  Total time: {totalTime:.2f}s"
  echo &"  Throughput: {formatRate(overallRate)}"
  echo &"  Storage used: {formatSize(store.used())}"
  echo ""

  cleanup()

proc printUsage() =
  echo "Usage: bench_merkle_streaming [options]"
  echo ""
  echo "Options:"
  echo "  --size=<size>      Dataset size (e.g., 1GB, 100GB, 1TB, 100TB)"
  echo "  --chunk=<size>     Chunk size in KB (default: 64)"
  echo "  --backend=<type>   Backend: packed, leveldb (default: packed)"
  echo "  --full             Run full dataset benchmark (with block storage)"
  echo "  --help             Show this help"

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
    result = 10'u64 * 1024 * 1024 * 1024

proc main() =
  var config = BenchConfig(
    totalSize: 10'u64 * 1024 * 1024 * 1024,
    chunkSize: DefaultChunkSize,
    backend: mbPacked,
    storeBlocks: false,
    reportInterval: 2.0
  )

  var runFull = false

  for arg in commandLineParams():
    if arg.startsWith("--size="):
      config.totalSize = parseSize(arg[7..^1])
    elif arg.startsWith("--chunk="):
      config.chunkSize = parseInt(arg[8..^1]) * 1024
    elif arg.startsWith("--backend="):
      let backend = arg[10..^1]
      case backend
      of "packed": config.backend = mbPacked
      of "leveldb": config.backend = mbLevelDb
      else: echo &"Unknown backend: {backend}"; return
    elif arg == "--full":
      runFull = true
    elif arg == "--help":
      printUsage()
      return

  if runFull:
    waitFor runFullDatasetBenchmark(config)
  else:
    runMerkleOnlyBenchmark(config)

when isMainModule:
  main()
