# nim-blockstore

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Stability: experimental](https://img.shields.io/badge/Stability-experimental-orange.svg)](#stability)
[![nim](https://img.shields.io/badge/nim-2.2.4+-yellow.svg)](https://nim-lang.org/)

A content-addressed block storage library for Nim with configurable hash algorithms, codecs, and merkle tree proofs for block verification.

## Features

- Content-addressed storage with CIDv1 identifiers
- Configurable hash functions and codecs via `BlockHashConfig`
- Merkle tree proofs for block verification
- Multiple storage backends:
  - Block storage: sharded files (`bbSharded`) or packed files (`bbPacked`)
  - Merkle tree storage: embedded proofs (`mbEmbeddedProofs`), LevelDB (`mbLevelDb`), or packed files (`mbPacked`)
  - Blockmap storage: LevelDB (`bmLevelDb`) or files (`bmFile`)
- Direct I/O support (`ioDirect`) for crash consistency and OS cache bypass
- Buffered I/O mode (`ioBuffered`) with configurable batch sync
- Storage quota management
- Background garbage collection with deletion worker
- Metadata stored in LevelDB
- Async file chunking
- Dataset management with manifests

## Usage

### Building a Dataset from a File

```nim
import blockstore
import taskpools
import std/options

proc buildDataset() {.async.} =
  # Create a shared thread pool for async file I/O
  let pool = Taskpool.new(numThreads = 4)
  defer: pool.shutdown()

  let store = newDatasetStore("./db", "./blocks").get()

  # Start building a dataset with 64KB chunks
  let builder = store.startDataset(64 * 1024, some("myfile.txt")).get()

  # Chunk the file
  let stream = (await builder.chunkFile(pool)).get()

  while true:
    let blockOpt = await stream.nextBlock()
    if blockOpt.isNone:
      break
    let blockResult = blockOpt.get()
    if blockResult.isErr:
      echo "Error: ", blockResult.error
      break
    discard await builder.addBlock(blockResult.value)

  stream.close()

  # Finalize and get the dataset
  let dataset = (await builder.finalize()).get()
  echo "Tree CID: ", dataset.treeCid
  echo "Manifest CID: ", dataset.manifestCid

waitFor buildDataset()
```

### Retrieving Blocks with Proofs

```nim
import blockstore

proc getBlockWithProof(store: DatasetStore, treeCid: Cid, index: int) {.async.} =
  let datasetOpt = (await store.getDataset(treeCid)).get()
  if datasetOpt.isNone:
    echo "Dataset not found"
    return

  let dataset = datasetOpt.get()
  let blockOpt = (await dataset.getBlock(index)).get()

  if blockOpt.isSome:
    let (blk, proof) = blockOpt.get()
    echo "Block: ", blk
    echo "Proof index: ", proof.index
    echo "Proof path length: ", proof.path.len
```

## API Reference

### newDatasetStore

Creates a new dataset store with configurable backends and I/O modes:

```nim
proc newDatasetStore*(
  dbPath: string,                                    # Path to LevelDB database
  blocksDir: string,                                 # Directory for block storage
  quota: uint64 = 0,                                 # Storage quota (0 = unlimited)
  blockHashConfig: BlockHashConfig = defaultBlockHashConfig(),
  merkleBackend: MerkleBackend = mbPacked,           # Merkle tree storage backend
  blockBackend: BlockBackend = bbSharded,            # Block storage backend
  blockmapBackend: BlockmapBackend = bmLevelDb,      # Blockmap storage backend
  ioMode: IOMode = ioDirect,                         # I/O mode
  syncBatchSize: int = 0,                            # Batch size for sync (buffered mode)
  pool: Taskpool = nil                               # Thread pool for deletion worker
): BResult[DatasetStore]
```

### Storage Backends

#### BlockBackend

| Value | Description |
|-------|-------------|
| `bbSharded` | Sharded directory structure (default). One file per block with 2-level sharding. |
| `bbPacked` | Packed file format. All blocks for a dataset in a single file. |

#### MerkleBackend

Controls how merkle proofs are stored and generated.

| Value | Description |
|-------|-------------|
| `mbEmbeddedProofs` | Proofs computed during build (tree in memory) and embedded in block references in LevelDB. Tree discarded after finalize. Good for smaller datasets. |
| `mbLevelDb` | Tree nodes stored in LevelDB. Proofs generated on-demand from stored tree. |
| `mbPacked` | Tree nodes in packed files (default). One file per tree. Proofs generated on-demand. Efficient for large datasets. |

#### BlockmapBackend

| Value | Description |
|-------|-------------|
| `bmLevelDb` | LevelDB storage (default). Shared with metadata. |
| `bmFile` | File-based storage. One file per blockmap. |

### I/O Modes

| Value | Description |
|-------|-------------|
| `ioDirect` | Direct I/O (default). Bypasses OS cache, data written directly to disk. Provides crash consistency. |
| `ioBuffered` | Buffered I/O. Uses OS cache. Use `syncBatchSize` to control sync frequency if needed. |

### BlockHashConfig

Configuration for block hashing and CID generation:

| Field | Type | Description |
|-------|------|-------------|
| `hashFunc` | `HashFunc` | Hash function `proc(data: openArray[byte]): HashDigest` |
| `hashCode` | `MultiCodec` | Multicodec identifier for the hash (e.g., `Sha256Code`) |
| `blockCodec` | `MultiCodec` | Codec for blocks (e.g., `LogosStorageBlock`) |
| `treeCodec` | `MultiCodec` | Codec for merkle tree CIDs (e.g., `LogosStorageTree`) |

## Running Tests

```bash
nimble test
```

## Code Coverage

Generate HTML coverage reports:

```bash
# All tests
nimble coverage

# Individual test suites
nimble coverage_merkle
nimble coverage_block
nimble coverage_chunker

# Clean coverage data
nimble coverage_clean
```

## Stability

This library is in experimental status and may have breaking changes between versions until it stabilizes.

## License

nim-blockstore is licensed and distributed under either of:

* Apache License, Version 2.0: [LICENSE-APACHEv2](LICENSE-APACHEv2) or https://opensource.org/licenses/Apache-2.0
* MIT license: [LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT

at your option. The contents of this repository may not be copied, modified, or distributed except according to those terms.
