import std/unittest
import results
import libp2p/multicodec
import ../blockstore/errors
import ../blockstore/cid
import ../blockstore/blocks

suite "Block tests":
  test "block creation":
    let data = cast[seq[byte]]("hello world")
    let blockResult = newBlock(data)

    check blockResult.isOk
    let b = blockResult.value
    check b.data == data
    check b.size == data.len

  test "block verification":
    let data = cast[seq[byte]]("hello world")
    let blockResult = newBlock(data)
    check blockResult.isOk

    let b = blockResult.value
    let verifyResult = b.verify()
    check verifyResult.isOk
    check verifyResult.value == true

  test "block verification fails for corrupted data":
    let data = cast[seq[byte]]("verify me")
    let blockResult = newBlock(data)
    check blockResult.isOk

    var b = blockResult.value
    b.data[0] = b.data[0] xor 1

    let verifyResult = b.verify()
    check verifyResult.isOk
    check verifyResult.value == false

  test "same data produces same CID":
    let data = cast[seq[byte]]("same_cid")
    let block1Result = newBlock(data)
    let block2Result = newBlock(data)

    check block1Result.isOk
    check block2Result.isOk
    check block1Result.value.cid == block2Result.value.cid

  test "different data produces different CID":
    let data1 = cast[seq[byte]]("data1")
    let data2 = cast[seq[byte]]("data2")
    let block1Result = newBlock(data1)
    let block2Result = newBlock(data2)

    check block1Result.isOk
    check block2Result.isOk
    check block1Result.value.cid != block2Result.value.cid

  test "CID has correct codec and hash":
    let data = cast[seq[byte]]("test data")
    let cidResult = computeCid(data)

    check cidResult.isOk
    let c = cidResult.value
    check c.cidver == CIDv1
    check c.mcodec == LogosStorageBlock

  test "CID string roundtrip":
    let data = cast[seq[byte]]("roundtrip test")
    let blockResult = newBlock(data)
    check blockResult.isOk

    let c = blockResult.value.cid
    let cidStr = $c
    let parsedResult = cidFromString(cidStr)

    check parsedResult.isOk
    check parsedResult.value == c

  test "BlockMetadata creation":
    let data = cast[seq[byte]]("metadata test")
    let blockResult = newBlock(data)
    check blockResult.isOk

    let b = blockResult.value
    let meta = newBlockMetadata(b.cid, b.size, 42)

    check meta.size == b.size
    check meta.index == 42
    check meta.cid == $b.cid
