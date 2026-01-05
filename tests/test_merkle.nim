import std/[unittest, os, options, sets, syncio, strutils]
import results
import leveldbstatic as leveldb
import libp2p/multicodec
import ../blockstore/errors
import ../blockstore/cid
import ../blockstore/merkle
import ../blockstore/sha256

const
  TestDbPath = "/tmp/test_merkle_db"
  TestPackedPath = "/tmp/test_merkle_packed.tree"

proc cleanup() =
  if dirExists(TestDbPath):
    removeDir(TestDbPath)
  if fileExists(TestPackedPath):
    removeFile(TestPackedPath)

suite "MerkleTreeBuilder tests":
  test "tree builder basic":
    var builder = newMerkleTreeBuilder()

    builder.addBlock(cast[seq[byte]]("block1"))
    builder.addBlock(cast[seq[byte]]("block2"))
    builder.addBlock(cast[seq[byte]]("block3"))

    check builder.blockCount == 3

    builder.buildTree()
    let rootCidResult = builder.rootCid()
    check rootCidResult.isOk

    let rootCid = rootCidResult.value
    check rootCid.cidver == CIDv1
    check rootCid.mcodec == LogosStorageTree

  test "single block proof":
    var builder = newMerkleTreeBuilder()
    let blockData = cast[seq[byte]]("hello world")
    builder.addBlock(blockData)
    builder.buildTree()

    let rootCidResult = builder.rootCid()
    check rootCidResult.isOk
    let rootCid = rootCidResult.value

    let proofResult = builder.getProof(0)
    check proofResult.isOk
    let proof = proofResult.value

    let mhResult = rootCid.mhash()
    check mhResult.isOk
    let mh = mhResult.get()
    let rootBytes = mh.data.buffer[mh.dpos .. mh.dpos + mh.size - 1]
    let verifyResult = proof.verify(rootBytes, blockData)
    check verifyResult.isOk
    check verifyResult.value == true

  test "proof fails for wrong data":
    var builder = newMerkleTreeBuilder()
    let blockData = cast[seq[byte]]("hello world")
    builder.addBlock(blockData)
    builder.buildTree()

    let rootCidResult = builder.rootCid()
    check rootCidResult.isOk
    let rootCid = rootCidResult.value

    let proofResult = builder.getProof(0)
    check proofResult.isOk
    let proof = proofResult.value

    let wrongData = cast[seq[byte]]("wrong data")
    let mhResult = rootCid.mhash()
    check mhResult.isOk
    let mh = mhResult.get()
    let rootBytes = mh.data.buffer[mh.dpos .. mh.dpos + mh.size - 1]
    let verifyResult = proof.verify(rootBytes, wrongData)
    check verifyResult.isOk
    check verifyResult.value == false

  test "deterministic root":
    var builder1 = newMerkleTreeBuilder()
    builder1.addBlock(cast[seq[byte]]("a"))
    builder1.addBlock(cast[seq[byte]]("b"))
    builder1.buildTree()

    var builder2 = newMerkleTreeBuilder()
    builder2.addBlock(cast[seq[byte]]("a"))
    builder2.addBlock(cast[seq[byte]]("b"))
    builder2.buildTree()

    check builder1.rootCid().value == builder2.rootCid().value

  test "proof structure for 4-leaf tree":
    var builder = newMerkleTreeBuilder()
    builder.addBlock(cast[seq[byte]]("a"))
    builder.addBlock(cast[seq[byte]]("b"))
    builder.addBlock(cast[seq[byte]]("c"))
    builder.addBlock(cast[seq[byte]]("d"))
    builder.buildTree()

    let proof = builder.getProof(1).value

    check proof.index == 1
    check proof.leafCount == 4
    check proof.path.len == 2

suite "Streaming Merkle Storage tests":
  setup:
    cleanup()

  teardown:
    cleanup()

  test "computeNumLevels":
    check computeNumLevels(0) == 0
    check computeNumLevels(1) == 1
    check computeNumLevels(2) == 2
    check computeNumLevels(3) == 3
    check computeNumLevels(4) == 3
    check computeNumLevels(5) == 4
    check computeNumLevels(8) == 4
    check computeNumLevels(16) == 5

  test "nodesAtLevel":
    check nodesAtLevel(4, 0) == 4
    check nodesAtLevel(4, 1) == 2
    check nodesAtLevel(4, 2) == 1

    check nodesAtLevel(5, 0) == 5
    check nodesAtLevel(5, 1) == 3
    check nodesAtLevel(5, 2) == 2
    check nodesAtLevel(5, 3) == 1

  test "computeNumLevels edge cases":
    check computeNumLevels(1'u64 shl 20) == 21      # 1M leaves -> 21 levels
    check computeNumLevels(1'u64 shl 30) == 31      # 1B leaves -> 31 levels
    check computeNumLevels(1'u64 shl 40) == 41      # 1T leaves -> 41 levels
    check computeNumLevels(1'u64 shl 50) == 51
    check computeNumLevels(1'u64 shl 60) == 61
    check computeNumLevels(1'u64 shl 63) == 64      # 2^63 leaves -> 64 levels

    check computeNumLevels((1'u64 shl 20) + 1) == 22
    check computeNumLevels((1'u64 shl 30) + 1) == 32
    check computeNumLevels((1'u64 shl 63) + 1) == 65

    check computeNumLevels(high(uint64)) == 65      # 2^64 - 1 -> 65 levels

  test "nodesAtLevel edge cases":
    let bigLeafCount = 1'u64 shl 40  # 1 trillion leaves
    check nodesAtLevel(bigLeafCount, 0) == bigLeafCount
    check nodesAtLevel(bigLeafCount, 10) == 1'u64 shl 30
    check nodesAtLevel(bigLeafCount, 20) == 1'u64 shl 20
    check nodesAtLevel(bigLeafCount, 40) == 1  # root level

    let oddLeafCount = (1'u64 shl 40) + 7
    check nodesAtLevel(oddLeafCount, 0) == oddLeafCount
    check nodesAtLevel(oddLeafCount, 1) == (oddLeafCount + 1) shr 1
    check nodesAtLevel(oddLeafCount, 40) == 2
    check nodesAtLevel(oddLeafCount, 41) == 1

    let maxLeaves = high(uint64)
    check nodesAtLevel(maxLeaves, 0) == maxLeaves
    check nodesAtLevel(maxLeaves, 63) == 2
    check nodesAtLevel(maxLeaves, 64) == 1  # root

    check nodesAtLevel(3, 1) == 2
    check nodesAtLevel(7, 2) == 2
    check nodesAtLevel(9, 3) == 2
    check nodesAtLevel(17, 4) == 2

  test "hashConcat deterministic":
    var h1, h2: MerkleHash
    for i in 0 ..< 32:
      h1[i] = byte(i)
      h2[i] = byte(i + 32)

    let result1 = hashConcat(h1, h2)
    let result2 = hashConcat(h1, h2)
    check result1 == result2

    let result3 = hashConcat(h2, h1)
    check result1 != result3

  test "LevelDB streaming builder - single leaf":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "tree1")
    var builder = newStreamingMerkleBuilder(storage)

    let leafHash = sha256Hash(cast[seq[byte]]("block0"))
    discard builder.addLeaf(leafHash)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    check root == leafHash
    check builder.leafCount == 1

  test "LevelDB streaming builder - two leaves":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "tree2")
    var builder = newStreamingMerkleBuilder(storage)

    let h0 = sha256Hash(cast[seq[byte]]("block0"))
    let h1 = sha256Hash(cast[seq[byte]]("block1"))
    discard builder.addLeaf(h0)
    discard builder.addLeaf(h1)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let expected = hashConcat(h0, h1)
    check root == expected

  test "LevelDB streaming builder - four leaves":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "tree4")
    var builder = newStreamingMerkleBuilder(storage)

    var hashes: seq[MerkleHash]
    for i in 0 ..< 4:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      hashes.add(h)
      discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let left = hashConcat(hashes[0], hashes[1])
    let right = hashConcat(hashes[2], hashes[3])
    let expected = hashConcat(left, right)
    check root == expected

  test "LevelDB reader and proof generation":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "treeProof")
    var builder = newStreamingMerkleBuilder(storage)

    var hashes: seq[MerkleHash]
    for i in 0 ..< 4:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      hashes.add(h)
      discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let reader = newMerkleReader(storage)
    check reader.leafCount == 4

    let rootOpt = reader.root()
    check rootOpt.isSome
    check rootOpt.get() == root

    let proofResult = reader.getProof(1)
    check proofResult.isOk
    let proof = proofResult.value

    check proof.index == 1
    check proof.leafCount == 4
    check proof.path.len == 2

    check verify(proof, root, hashes[1])
    check not verify(proof, root, hashes[0])

  test "LevelDB proof for all leaves":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "treeAllProofs")
    var builder = newStreamingMerkleBuilder(storage)

    var hashes: seq[MerkleHash]
    for i in 0 ..< 8:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      hashes.add(h)
      discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let reader = newMerkleReader(storage)

    for i in 0 ..< 8:
      let proofResult = reader.getProof(uint64(i))
      check proofResult.isOk
      let proof = proofResult.value
      check verify(proof, root, hashes[i])

  test "Packed storage - basic write and read":
    let storage = newPackedMerkleStorage(TestPackedPath, forWriting = true).get()
    defer: discard storage.close()

    var builder = newStreamingMerkleBuilder(storage)

    var hashes: seq[MerkleHash]
    for i in 0 ..< 4:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      hashes.add(h)
      discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let (leafCount, numLevels) = storage.getMetadata()
    check leafCount == 4
    check numLevels == 3

  test "Packed storage - read after close":
    block:
      let storage = newPackedMerkleStorage(TestPackedPath, forWriting = true).get()
      var builder = newStreamingMerkleBuilder(storage)

      for i in 0 ..< 4:
        let h = sha256Hash(cast[seq[byte]]("block" & $i))
        discard builder.addLeaf(h)

      discard builder.finalize()
      discard storage.close()

    block:
      let storage = newPackedMerkleStorage(TestPackedPath).get()
      defer: discard storage.close()

      let (leafCount, numLevels) = storage.getMetadata()
      check leafCount == 4
      check numLevels == 3

      let reader = newMerkleReader(storage)
      check reader.leafCount == 4

      let rootOpt = reader.root()
      check rootOpt.isSome

  test "Packed storage - proof verification":
    var hashes: seq[MerkleHash]
    var root: MerkleHash

    block:
      let storage = newPackedMerkleStorage(TestPackedPath, forWriting = true).get()
      var builder = newStreamingMerkleBuilder(storage)

      for i in 0 ..< 8:
        let h = sha256Hash(cast[seq[byte]]("block" & $i))
        hashes.add(h)
        discard builder.addLeaf(h)

      let rootResult = builder.finalize()
      check rootResult.isOk
      root = rootResult.value
      discard storage.close()

    block:
      let storage = newPackedMerkleStorage(TestPackedPath).get()
      defer: discard storage.close()

      let reader = newMerkleReader(storage)

      for i in 0 ..< 8:
        let proofResult = reader.getProof(uint64(i))
        check proofResult.isOk
        let proof = proofResult.value
        check verify(proof, root, hashes[i])

  test "Non-power-of-two leaves - 5 leaves":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "tree5")
    var builder = newStreamingMerkleBuilder(storage)

    var hashes: seq[MerkleHash]
    for i in 0 ..< 5:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      hashes.add(h)
      discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let reader = newMerkleReader(storage)
    check reader.leafCount == 5

    for i in 0 ..< 5:
      let proofResult = reader.getProof(uint64(i))
      check proofResult.isOk
      let proof = proofResult.value
      check verify(proof, root, hashes[i])

  test "Non-power-of-two leaves - 7 leaves":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "tree7")
    var builder = newStreamingMerkleBuilder(storage)

    var hashes: seq[MerkleHash]
    for i in 0 ..< 7:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      hashes.add(h)
      discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let reader = newMerkleReader(storage)

    for i in 0 ..< 7:
      let proofResult = reader.getProof(uint64(i))
      check proofResult.isOk
      let proof = proofResult.value
      check verify(proof, root, hashes[i])

  test "Large tree - 1000 leaves":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    const numLeaves = 1000
    let storage = newLevelDbMerkleStorage(db, "tree1000")
    var builder = newStreamingMerkleBuilder(storage)

    var hashes: seq[MerkleHash]
    for i in 0 ..< numLeaves:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      hashes.add(h)
      discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let reader = newMerkleReader(storage)
    check reader.leafCount == numLeaves

    let testIndices = @[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                        990, 991, 992, 993, 994, 995, 996, 997, 998, 999,
                        100, 250, 500, 750, 333, 666, 512, 511, 513]
    for i in testIndices:
      let proofResult = reader.getProof(uint64(i))
      check proofResult.isOk
      let proof = proofResult.value
      check verify(proof, root, hashes[i])

  test "Large tree - 997 leaves":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    const numLeaves = 997
    let storage = newLevelDbMerkleStorage(db, "tree997")
    var builder = newStreamingMerkleBuilder(storage)

    var hashes: seq[MerkleHash]
    for i in 0 ..< numLeaves:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      hashes.add(h)
      discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let reader = newMerkleReader(storage)
    check reader.leafCount == numLeaves

    for i in 0 ..< numLeaves:
      let proofResult = reader.getProof(uint64(i))
      check proofResult.isOk
      let proof = proofResult.value
      if not verify(proof, root, hashes[i]):
        echo "Proof verification failed for leaf ", i
        check false

  test "Large tree - 1024 leaves":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    const numLeaves = 1024
    let storage = newLevelDbMerkleStorage(db, "tree1024")
    var builder = newStreamingMerkleBuilder(storage)

    var hashes: seq[MerkleHash]
    for i in 0 ..< numLeaves:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      hashes.add(h)
      discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let reader = newMerkleReader(storage)
    check reader.leafCount == numLeaves

    let testIndices = @[0, 1, 2, 511, 512, 513, 1022, 1023,
                        256, 768, 128, 384, 640, 896]
    for i in testIndices:
      let proofResult = reader.getProof(uint64(i))
      check proofResult.isOk
      let proof = proofResult.value
      check verify(proof, root, hashes[i])

  test "Large packed storage - 500000 leaves":
    const numLeaves = 500000
    var hashes: seq[MerkleHash]
    var root: MerkleHash

    block:
      let storage = newPackedMerkleStorage(TestPackedPath, forWriting = true).get()
      var builder = newStreamingMerkleBuilder(storage)

      for i in 0 ..< numLeaves:
        let h = sha256Hash(cast[seq[byte]]("block" & $i))
        hashes.add(h)
        discard builder.addLeaf(h)

      let rootResult = builder.finalize()
      check rootResult.isOk
      root = rootResult.value
      discard storage.close()

    block:
      let storage = newPackedMerkleStorage(TestPackedPath).get()
      defer: discard storage.close()

      let reader = newMerkleReader(storage)
      check reader.leafCount == numLeaves

      for i in 0 ..< numLeaves:
        let proofResult = reader.getProof(uint64(i))
        check proofResult.isOk
        let proof = proofResult.value
        if not verify(proof, root, hashes[i]):
          echo "Packed proof verification failed for leaf ", i
          doAssert false

  test "Empty tree finalize fails":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "emptyTree")
    var builder = newStreamingMerkleBuilder(storage)

    let rootResult = builder.finalize()
    check rootResult.isErr

  test "Invalid proof index":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "treeInvalid")
    var builder = newStreamingMerkleBuilder(storage)

    for i in 0 ..< 4:
      let h = sha256Hash(cast[seq[byte]]("block" & $i))
      discard builder.addLeaf(h)

    discard builder.finalize()

    let reader = newMerkleReader(storage)
    let proofResult = reader.getProof(10)
    check proofResult.isErr

suite "PackedMerkleStorage error cases":
  setup:
    cleanup()

  teardown:
    cleanup()

  test "Invalid magic in packed file":
    # Here create a file with wrong magic bytes
    let f = syncio.open(TestPackedPath, fmWrite)
    var wrongMagic: uint32 = 0xDEADBEEF'u32
    var version: uint32 = 2
    var leafCount: uint64 = 0
    var numLevels: int32 = 0
    discard f.writeBuffer(addr wrongMagic, 4)
    discard f.writeBuffer(addr version, 4)
    discard f.writeBuffer(addr leafCount, 8)
    discard f.writeBuffer(addr numLevels, 4)
    f.close()

    let res = newPackedMerkleStorage(TestPackedPath, forWriting = false)
    check res.isErr
    check res.error.msg == "Invalid packed merkle file magic"

  test "Unsupported version in packed file":
    # Now create a file with correct magic but wrong version
    let f = syncio.open(TestPackedPath, fmWrite)
    var magic: uint32 = 0x534B4C4D'u32  # PackedMagic
    var wrongVersion: uint32 = 99
    var leafCount: uint64 = 0
    var numLevels: int32 = 0
    discard f.writeBuffer(addr magic, 4)
    discard f.writeBuffer(addr wrongVersion, 4)
    discard f.writeBuffer(addr leafCount, 8)
    discard f.writeBuffer(addr numLevels, 4)
    f.close()

    let res = newPackedMerkleStorage(TestPackedPath, forWriting = false)
    check res.isErr
    check "Unsupported packed merkle file version" in res.error.msg

  test "File too small for header":
    # And now create a file that's too small
    let f = syncio.open(TestPackedPath, fmWrite)
    var magic: uint32 = 0x534B4C4D'u32
    discard f.writeBuffer(addr magic, 4)
    f.close()

    let res = newPackedMerkleStorage(TestPackedPath, forWriting = false)
    check res.isErr
    check res.error.msg == "File too small for header"

suite "MerkleTreeBuilder edge cases":
  test "root() returns none when not built":
    var builder = newMerkleTreeBuilder()
    builder.addBlock(cast[seq[byte]]("block1"))
    let rootOpt = builder.root()
    check rootOpt.isNone

  test "rootCid() fails when not built":
    var builder = newMerkleTreeBuilder()
    builder.addBlock(cast[seq[byte]]("block1"))
    let cidResult = builder.rootCid()
    check cidResult.isErr
    check "Tree not built" in cidResult.error.msg

  test "getProof() fails when not built":
    var builder = newMerkleTreeBuilder()
    builder.addBlock(cast[seq[byte]]("block1"))
    let proofResult = builder.getProof(0)
    check proofResult.isErr
    check "Tree not built" in proofResult.error.msg

  test "addBlock after buildTree raises Defect":
    var builder = newMerkleTreeBuilder()
    builder.addBlock(cast[seq[byte]]("block1"))
    builder.buildTree()

    var raised = false
    try:
      builder.addBlock(cast[seq[byte]]("block2"))
    except Defect:
      raised = true
    check raised

  test "buildTree on empty builder does nothing":
    var builder = newMerkleTreeBuilder()
    builder.buildTree()
    let rootOpt = builder.root()
    check rootOpt.isNone

suite "Proof verification edge cases":
  test "verify() with wrong root length returns error":
    var builder = newMerkleTreeBuilder()
    let blockData = cast[seq[byte]]("hello world")
    builder.addBlock(blockData)
    builder.buildTree()

    let proofResult = builder.getProof(0)
    check proofResult.isOk
    let proof = proofResult.value

    let
      shortRoot: array[16, byte] = default(array[16, byte])
      verifyResult = proof.verify(shortRoot, blockData)
    check verifyResult.isErr

suite "rootToCid function":
  setup:
    cleanup()

  teardown:
    cleanup()

  test "rootToCid converts hash to valid CID":
    let db = leveldb.open(TestDbPath)
    defer: db.close()

    let storage = newLevelDbMerkleStorage(db, "rootToCidTest")
    var builder = newStreamingMerkleBuilder(storage)

    let h = sha256Hash(cast[seq[byte]]("block0"))
    discard builder.addLeaf(h)

    let rootResult = builder.finalize()
    check rootResult.isOk
    let root = rootResult.value

    let cidResult = rootToCid(root)
    check cidResult.isOk
    let cid = cidResult.value
    check cid.cidver == CIDv1
    check cid.mcodec == LogosStorageTree

suite "getRequiredLeafIndices function":
  test "single leaf at start":
    let res = getRequiredLeafIndices(0, 1, 4)
    check 1 in res

  test "single leaf in middle":
    let res = getRequiredLeafIndices(2, 1, 4)
    check 3 in res

  test "consecutive pair - no extra leaves needed at first level":
    let res = getRequiredLeafIndices(0, 2, 4)
    check 2 in res
    check 3 in res

  test "full range - no extra leaves needed":
    let res = getRequiredLeafIndices(0, 4, 4)
    check res.len == 0

  test "larger tree - partial range":
    let res = getRequiredLeafIndices(0, 3, 8)
    check 3 in res
    check 4 in res
    check 5 in res
    check 6 in res
    check 7 in res

  test "non-power-of-two total leaves":
    let res = getRequiredLeafIndices(0, 2, 5)
    check 2 in res
    check 3 in res
    check 4 in res
