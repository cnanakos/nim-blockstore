import std/[math, streams, options, strutils]
import results
import libp2p/multicodec
import ./errors
import ./cid
import ./sha256
import ./serialization as ser

const
  LogosStorageBlockCodec* = 0xCD02'u32
  Sha256Hcodec* = 0x12'u32
  CidVersionV1* = 1'u8

type
  Manifest* = object
    treeCid*: seq[byte]
    blockSize*: uint32
    datasetSize*: uint64
    codec*: uint32
    hcodec*: uint32
    version*: uint8
    filename*: Option[string]
    mimetype*: Option[string]

proc newManifest*(treeCid: seq[byte], blockSize: uint32, datasetSize: uint64): Manifest =
  Manifest(
    treeCid: treeCid,
    blockSize: blockSize,
    datasetSize: datasetSize,
    codec: LogosStorageBlockCodec,
    hcodec: Sha256Hcodec,
    version: CidVersionV1,
    filename: none(string),
    mimetype: none(string)
  )

proc newManifest*(
    treeCid: seq[byte],
    blockSize: uint32,
    datasetSize: uint64,
    filename: Option[string],
    mimetype: Option[string]
): Manifest =
  Manifest(
    treeCid: treeCid,
    blockSize: blockSize,
    datasetSize: datasetSize,
    codec: LogosStorageBlockCodec,
    hcodec: Sha256Hcodec,
    version: CidVersionV1,
    filename: filename,
    mimetype: mimetype
  )

proc blocksCount*(m: Manifest): int =
  int(ceilDiv(m.datasetSize, m.blockSize.uint64))

proc validate*(m: Manifest): BResult[void] =
  if m.treeCid.len == 0:
    return err(manifestDecodingError("tree_cid cannot be empty"))

  if m.blockSize == 0:
    return err(manifestDecodingError("block_size must be greater than 0"))

  if m.codec != LogosStorageBlockCodec:
    return err(manifestDecodingError(
      "invalid codec: expected 0xCD02, got 0x" & m.codec.toHex
    ))

  if m.hcodec != Sha256Hcodec:
    return err(manifestDecodingError(
      "invalid hcodec: expected 0x12 (sha2-256), got 0x" & m.hcodec.toHex
    ))

  if m.version != CidVersionV1:
    return err(manifestDecodingError(
      "invalid version: expected 1, got " & $m.version
    ))

  ok()

proc encodeManifest*(m: Manifest): BResult[seq[byte]] =
  try:
    let s = newStringStream()

    ser.writeBytes(s, m.treeCid)
    ser.writeUint32(s, m.blockSize)
    ser.writeUint64(s, m.datasetSize)
    ser.writeUint32(s, m.codec)
    ser.writeUint32(s, m.hcodec)
    ser.writeUint8(s, m.version)

    if m.filename.isSome:
      ser.writeBool(s, true)
      ser.writeString(s, m.filename.get)
    else:
      ser.writeBool(s, false)

    if m.mimetype.isSome:
      ser.writeBool(s, true)
      ser.writeString(s, m.mimetype.get)
    else:
      ser.writeBool(s, false)

    s.setPosition(0)
    ok(cast[seq[byte]](s.readAll()))
  except CatchableError as e:
    err(manifestEncodingError(e.msg))

proc decodeManifest*(data: openArray[byte]): BResult[Manifest] =
  try:
    let dataCopy = @data
    let s = newStringStream(cast[string](dataCopy))

    var m: Manifest
    m.treeCid = ?ser.readBytes(s)
    m.blockSize = ser.readUint32(s)
    m.datasetSize = ser.readUint64(s)
    m.codec = ser.readUint32(s)
    m.hcodec = ser.readUint32(s)
    m.version = ser.readUint8(s)

    if ser.readBool(s):
      m.filename = some(?ser.readString(s))
    else:
      m.filename = none(string)

    if ser.readBool(s):
      m.mimetype = some(?ser.readString(s))
    else:
      m.mimetype = none(string)

    ok(m)
  except CatchableError as e:
    err(manifestDecodingError(e.msg))

proc toCid*(m: Manifest): BResult[Cid] =
  let encoded = ?encodeManifest(m)
  let hash = sha256Hash(encoded)
  let mh = ?wrap(Sha256Code, hash)
  newCidV1(LogosStorageManifest, mh)

proc fromCidData*(c: Cid, data: openArray[byte]): BResult[Manifest] =
  if c.mcodec != LogosStorageManifest:
    return err(cidError(
      "Expected manifest codec 0xCD01, got 0x" & int(c.mcodec).toHex
    ))

  let
    manifest = ?decodeManifest(data)
    computedCid = ?manifest.toCid()

  if computedCid != c:
    return err(cidError("Manifest CID mismatch"))

  ok(manifest)

proc `$`*(m: Manifest): string =
  result = "Manifest("
  result.add("blockSize=" & $m.blockSize)
  result.add(", datasetSize=" & $m.datasetSize)
  result.add(", blocks=" & $m.blocksCount)
  if m.filename.isSome:
    result.add(", filename=" & m.filename.get)
  result.add(")")
