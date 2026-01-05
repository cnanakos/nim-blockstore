import std/hashes
import results
import libp2p/multicodec

import ./errors
import ./cid
import ./sha256

type
  HashDigest* = array[32, byte]
  HashFunc* = proc(data: openArray[byte]): HashDigest {.noSideEffect, gcsafe, raises: [].}

  BlockHashConfig* = object
    hashFunc*: HashFunc
    hashCode*: MultiCodec
    blockCodec*: MultiCodec
    treeCodec*: MultiCodec

  Block* = ref object
    cid*: Cid
    data*: seq[byte]

  BlockMetadata* = object
    cid*: string
    size*: int
    index*: int

proc sha256HashFunc*(data: openArray[byte]): HashDigest {.noSideEffect, gcsafe, raises: [].} =
  sha256Hash(data)

proc defaultBlockHashConfig*(): BlockHashConfig {.gcsafe.} =
  BlockHashConfig(
    hashFunc: sha256HashFunc,
    hashCode: Sha256Code,
    blockCodec: LogosStorageBlock,
    treeCodec: LogosStorageTree
  )

proc computeCid*(data: openArray[byte], config: BlockHashConfig): BResult[Cid] =
  let
    hash = config.hashFunc(data)
    mh = ?wrap(config.hashCode, hash)
  newCidV1(config.blockCodec, mh)

proc computeCid*(data: openArray[byte]): BResult[Cid] =
  computeCid(data, defaultBlockHashConfig())

proc newBlock*(data: seq[byte], config: BlockHashConfig): BResult[Block] =
  let c = ?computeCid(data, config)
  var blk = new(Block)
  blk.cid = c
  blk.data = data
  ok(blk)

proc newBlock*(data: seq[byte]): BResult[Block] =
  newBlock(data, defaultBlockHashConfig())

proc newBlock*(data: string, config: BlockHashConfig): BResult[Block] =
  newBlock(cast[seq[byte]](data), config)

proc newBlock*(data: string): BResult[Block] =
  newBlock(cast[seq[byte]](data), defaultBlockHashConfig())

proc fromCidUnchecked*(cid: Cid, data: seq[byte]): Block =
  var blk = new(Block)
  blk.cid = cid
  blk.data = data
  blk

proc verify*(b: Block): BResult[bool] =
  let computed = ?computeCid(b.data)
  ok(computed == b.cid)

proc size*(b: Block): int {.inline.} =
  b.data.len

proc `==`*(a, b: Block): bool =
  a.cid == b.cid and a.data == b.data

proc hash*(b: Block): Hash =
  var h: Hash = 0
  h = h !& hash(b.cid.toBytes())
  !$h

proc newBlockMetadata*(cid: Cid, size: int, index: int): BlockMetadata =
  BlockMetadata(cid: $cid, size: size, index: index)

proc `$`*(b: Block): string =
  "Block(" & $b.cid & ", size=" & $b.size & ")"

proc `$`*(m: BlockMetadata): string =
  "BlockMetadata(cid=" & m.cid & ", size=" & $m.size & ", index=" & $m.index & ")"
