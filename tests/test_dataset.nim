import std/[unittest, os, options]
import chronos
import results
import ../blockstore/errors
import ../blockstore/cid
import ../blockstore/blocks
import ../blockstore/dataset
import ../blockstore/blockmap
import ../blockstore/merkle

const
  TestDir = getTempDir() / "nim_blockstore_dataset_test"
  DbPath = TestDir / "db"
  BlocksDir = TestDir / "blocks"

proc cleanup() =
  if dirExists(TestDir):
    removeDir(TestDir)

proc createTestDataset(store: DatasetStore, blockCount: int, chunkSize: int = 4096): Future[BResult[Dataset]] {.async.} =
  let builderResult = store.startDataset(chunkSize.uint32, some("test"))
  if builderResult.isErr:
    return err(builderResult.error)

  var builder = builderResult.value

  for i in 0 ..< blockCount:
    var data = newSeq[byte](chunkSize)
    for j in 0 ..< chunkSize:
      data[j] = byte((i * chunkSize + j) mod 256)

    let blkResult = newBlock(data)
    if blkResult.isErr:
      return err(blkResult.error)

    let addResult = await builder.addBlock(blkResult.value)
    if addResult.isErr:
      return err(addResult.error)

  return await builder.finalize()

proc runDeleteExistingDataset() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir)
  doAssert storeResult.isOk, "Failed to create store: " & $storeResult.error
  let store = storeResult.value
  defer: store.close()

  let datasetResult = await createTestDataset(store, 5)
  doAssert datasetResult.isOk, "Failed to create dataset: " & $datasetResult.error
  let dataset = datasetResult.value

  let manifestCid = dataset.manifestCid

  let getResult1 = await store.getDataset(dataset.treeCid)
  doAssert getResult1.isOk
  doAssert getResult1.value.isSome, "Dataset should exist before deletion"

  let deleteResult = await store.deleteDataset(manifestCid)
  doAssert deleteResult.isOk, "Delete should succeed: " & $deleteResult.error

  let getResult2 = await store.getDataset(dataset.treeCid)
  doAssert getResult2.isOk
  doAssert getResult2.value.isNone, "Dataset should not exist after deletion"

  cleanup()

proc runDeleteNonExistentDataset() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let fakeCidResult = cidFromString("bagazuayseaka5yn4pfmebc7bqkkoij6wb5x3o4jlvzq7flqhd63qalnrskwvy")
  doAssert fakeCidResult.isOk
  let fakeCid = fakeCidResult.value

  let deleteResult = await store.deleteDataset(fakeCid)
  doAssert deleteResult.isErr, "Delete should fail for non-existent dataset"
  doAssert deleteResult.error.kind == DatasetNotFound

  cleanup()

proc runStorageReleasedAfterDeletion() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let usedBefore = store.used()

  let datasetResult = await createTestDataset(store, 10, 4096)
  doAssert datasetResult.isOk
  let dataset = datasetResult.value

  let usedAfterCreate = store.used()
  doAssert usedAfterCreate > usedBefore, "Storage should increase after adding dataset"

  let deleteResult = await store.deleteDataset(dataset.manifestCid)
  doAssert deleteResult.isOk

  # Wait for pending deletions to be processed by the worker - hoping that
  # 500milli will do the job
  for _ in 0 ..< 10:
    await sleepAsync(50.milliseconds)
    if store.used() < usedAfterCreate:
      break

  let usedAfterDelete = store.used()
  doAssert usedAfterDelete < usedAfterCreate, "Storage should decrease after deletion"

  cleanup()

proc runMultipleDatasetsDeletion() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let dataset1Result = await createTestDataset(store, 3, 4096)
  doAssert dataset1Result.isOk
  let dataset1 = dataset1Result.value

  let dataset2Result = await createTestDataset(store, 4, 4096)
  doAssert dataset2Result.isOk
  let dataset2 = dataset2Result.value

  let get1Before = await store.getDataset(dataset1.treeCid)
  let get2Before = await store.getDataset(dataset2.treeCid)
  doAssert get1Before.isOk and get1Before.value.isSome
  doAssert get2Before.isOk and get2Before.value.isSome

  let delete1Result = await store.deleteDataset(dataset1.manifestCid)
  doAssert delete1Result.isOk

  let get1After = await store.getDataset(dataset1.treeCid)
  let get2After = await store.getDataset(dataset2.treeCid)
  doAssert get1After.isOk and get1After.value.isNone, "Dataset 1 should be deleted"
  doAssert get2After.isOk and get2After.value.isSome, "Dataset 2 should still exist"

  let delete2Result = await store.deleteDataset(dataset2.manifestCid)
  doAssert delete2Result.isOk

  let get2Final = await store.getDataset(dataset2.treeCid)
  doAssert get2Final.isOk and get2Final.value.isNone, "Dataset 2 should be deleted"

  cleanup()

proc runDeleteDatasetWithManyBlocks() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let datasetResult = await createTestDataset(store, 100, 4096)
  doAssert datasetResult.isOk
  let dataset = datasetResult.value

  doAssert dataset.blockCount == 100

  let deleteResult = await store.deleteDataset(dataset.manifestCid)
  doAssert deleteResult.isOk, "Delete should succeed for dataset with many blocks"

  let getResult = await store.getDataset(dataset.treeCid)
  doAssert getResult.isOk and getResult.value.isNone

  cleanup()

proc runMappedBlockmapBasic() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir, blockmapBackend = bmFile)
  doAssert storeResult.isOk, "Failed to create store with mapped blockmap: " & $storeResult.error
  let store = storeResult.value
  defer: store.close()

  let datasetResult = await createTestDataset(store, 10)
  doAssert datasetResult.isOk, "Failed to create dataset with mapped blockmap: " & $datasetResult.error
  let dataset = datasetResult.value

  doAssert dataset.blockCount == 10
  doAssert dataset.completed() == 10

  for i in 0 ..< 10:
    let blockResult = await dataset.getBlock(i)
    doAssert blockResult.isOk
    doAssert blockResult.value.isSome

  cleanup()

proc runMappedBlockmapRanges() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir, blockmapBackend = bmFile)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let datasetResult = await createTestDataset(store, 20)
  doAssert datasetResult.isOk
  let dataset = datasetResult.value

  let ranges = dataset.getBlockmapRanges()
  doAssert ranges.len >= 1, "Expected at least one range"

  var totalBlocks: uint64 = 0
  for r in ranges:
    totalBlocks += r.count
  doAssert totalBlocks == 20, "Expected 20 blocks in ranges"

  cleanup()

proc runMappedBlockmapPersistence() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  var treeCid: Cid
  block:
    let storeResult = newDatasetStore(DbPath, BlocksDir, blockmapBackend = bmFile)
    doAssert storeResult.isOk
    let store = storeResult.value

    let datasetResult = await createTestDataset(store, 15)
    doAssert datasetResult.isOk
    treeCid = datasetResult.value.treeCid

    store.close()

  block:
    let storeResult = newDatasetStore(DbPath, BlocksDir, blockmapBackend = bmFile)
    doAssert storeResult.isOk
    let store = storeResult.value
    defer: store.close()

    let getResult = await store.getDataset(treeCid)
    doAssert getResult.isOk
    doAssert getResult.value.isSome, "Dataset should persist after reopen"

    let dataset = getResult.value.get()
    doAssert dataset.blockCount == 15
    doAssert dataset.completed() == 15

  cleanup()

proc runMappedBlockmapDeletion() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir, blockmapBackend = bmFile)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let datasetResult = await createTestDataset(store, 5)
  doAssert datasetResult.isOk
  let dataset = datasetResult.value

  let manifestCid = dataset.manifestCid

  let deleteResult = await store.deleteDataset(manifestCid)
  doAssert deleteResult.isOk

  let getResult = await store.getDataset(dataset.treeCid)
  doAssert getResult.isOk
  doAssert getResult.value.isNone, "Dataset should not exist after deletion"

  cleanup()

suite "Dataset deletion tests":
  setup:
    cleanup()

  teardown:
    cleanup()

  test "delete existing dataset":
    waitFor runDeleteExistingDataset()

  test "delete non-existent dataset returns error":
    waitFor runDeleteNonExistentDataset()

  test "storage released after deletion":
    waitFor runStorageReleasedAfterDeletion()

  test "delete one dataset doesn't affect others":
    waitFor runMultipleDatasetsDeletion()

  test "delete dataset with many blocks":
    waitFor runDeleteDatasetWithManyBlocks()

suite "Mapped blockmap backend tests":
  setup:
    cleanup()

  teardown:
    cleanup()

  test "basic dataset operations with mapped blockmap":
    waitFor runMappedBlockmapBasic()

  test "blockmap ranges work with mapped backend":
    waitFor runMappedBlockmapRanges()

  test "mapped blockmap persists across reopens":
    waitFor runMappedBlockmapPersistence()

  test "mapped blockmap files deleted with dataset":
    waitFor runMappedBlockmapDeletion()

proc runAbortBasic() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let builderResult = store.startDataset(4096.uint32, some("test"))
  doAssert builderResult.isOk
  var builder = builderResult.value

  for i in 0 ..< 5:
    var data = newSeq[byte](4096)
    for j in 0 ..< 4096:
      data[j] = byte((i * 4096 + j) mod 256)
    let blkResult = newBlock(data)
    doAssert blkResult.isOk
    let addResult = await builder.addBlock(blkResult.value)
    doAssert addResult.isOk

  let usedBefore = store.used()
  doAssert usedBefore > 0

  let abortResult = await builder.abort()
  doAssert abortResult.isOk

  cleanup()

proc runAbortPreventsAddBlock() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let builderResult = store.startDataset(4096.uint32, some("test"))
  doAssert builderResult.isOk
  var builder = builderResult.value

  let abortResult = await builder.abort()
  doAssert abortResult.isOk

  var data = newSeq[byte](4096)
  let blkResult = newBlock(data)
  doAssert blkResult.isOk

  let addResult = await builder.addBlock(blkResult.value)
  doAssert addResult.isErr
  doAssert addResult.error.kind == InvalidOperation

  cleanup()

proc runAbortPreventsFinalize() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let builderResult = store.startDataset(4096.uint32, some("test"))
  doAssert builderResult.isOk
  var builder = builderResult.value

  var data = newSeq[byte](4096)
  let blkResult = newBlock(data)
  doAssert blkResult.isOk
  let addResult = await builder.addBlock(blkResult.value)
  doAssert addResult.isOk

  let abortResult = await builder.abort()
  doAssert abortResult.isOk

  let finalizeResult = await builder.finalize()
  doAssert finalizeResult.isErr
  doAssert finalizeResult.error.kind == InvalidOperation

  cleanup()

proc runAbortWithPackedBackend() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir, merkleBackend = mbPacked, blockBackend = bbPacked)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let builderResult = store.startDataset(4096.uint32, some("test"))
  doAssert builderResult.isOk
  var builder = builderResult.value

  for i in 0 ..< 5:
    var data = newSeq[byte](4096)
    for j in 0 ..< 4096:
      data[j] = byte((i * 4096 + j) mod 256)
    let blkResult = newBlock(data)
    doAssert blkResult.isOk
    let addResult = await builder.addBlock(blkResult.value)
    doAssert addResult.isOk

  let abortResult = await builder.abort()
  doAssert abortResult.isOk

  cleanup()

proc runAbortWithLevelDbBackend() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir, merkleBackend = mbLevelDb)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let builderResult = store.startDataset(4096.uint32, some("test"))
  doAssert builderResult.isOk
  var builder = builderResult.value

  for i in 0 ..< 5:
    var data = newSeq[byte](4096)
    for j in 0 ..< 4096:
      data[j] = byte((i * 4096 + j) mod 256)
    let blkResult = newBlock(data)
    doAssert blkResult.isOk
    let addResult = await builder.addBlock(blkResult.value)
    doAssert addResult.isOk

  let abortResult = await builder.abort()
  doAssert abortResult.isOk

  cleanup()

proc runAbortIdempotent() {.async.} =
  cleanup()
  createDir(TestDir)
  createDir(BlocksDir)

  let storeResult = newDatasetStore(DbPath, BlocksDir)
  doAssert storeResult.isOk
  let store = storeResult.value
  defer: store.close()

  let builderResult = store.startDataset(4096.uint32, some("test"))
  doAssert builderResult.isOk
  var builder = builderResult.value

  let abort1 = await builder.abort()
  doAssert abort1.isOk

  let abort2 = await builder.abort()
  doAssert abort2.isOk

  cleanup()

suite "DatasetBuilder abort tests":
  setup:
    cleanup()

  teardown:
    cleanup()

  test "abort cleans up builder":
    waitFor runAbortBasic()

  test "abort prevents further addBlock":
    waitFor runAbortPreventsAddBlock()

  test "abort prevents finalize":
    waitFor runAbortPreventsFinalize()

  test "abort with packed backend":
    waitFor runAbortWithPackedBackend()

  test "abort with leveldb backend":
    waitFor runAbortWithLevelDbBackend()

  test "abort is idempotent":
    waitFor runAbortIdempotent()
