import std/[streams, endians]
import results

import ./errors

proc writeUint8*(s: Stream, v: uint8) =
  s.write(v)

proc readUint8*(s: Stream): uint8 =
  s.read(result)

proc writeUint16*(s: Stream, v: uint16) =
  var le: uint16
  littleEndian16(addr le, unsafeAddr v)
  s.write(le)

proc readUint16*(s: Stream): uint16 =
  var le: uint16
  s.read(le)
  littleEndian16(addr result, addr le)

proc writeUint32*(s: Stream, v: uint32) =
  var le: uint32
  littleEndian32(addr le, unsafeAddr v)
  s.write(le)

proc readUint32*(s: Stream): uint32 =
  var le: uint32
  s.read(le)
  littleEndian32(addr result, addr le)

proc writeUint64*(s: Stream, v: uint64) =
  var le: uint64
  littleEndian64(addr le, unsafeAddr v)
  s.write(le)

proc readUint64*(s: Stream): uint64 =
  var le: uint64
  s.read(le)
  littleEndian64(addr result, addr le)

proc writeInt64*(s: Stream, v: int64) =
  writeUint64(s, cast[uint64](v))

proc readInt64*(s: Stream): int64 =
  cast[int64](readUint64(s))

proc writeBytes*(s: Stream, data: openArray[byte]) =
  s.writeUint64(data.len.uint64)
  if data.len > 0:
    s.writeData(unsafeAddr data[0], data.len)

proc readBytes*(s: Stream): BResult[seq[byte]] =
  let len = s.readUint64().int
  if len > 0:
    var data = newSeq[byte](len)
    let bytesRead = s.readData(addr data[0], len)
    if bytesRead != len:
      return err(ioError("Failed to read " & $len & " bytes, got " & $bytesRead))
    ok(data)
  else:
    ok(newSeq[byte]())

proc writeString*(s: Stream, str: string) =
  writeBytes(s, cast[seq[byte]](str))

proc readString*(s: Stream): BResult[string] =
  let bytes = ?readBytes(s)
  var res = newString(bytes.len)
  if bytes.len > 0:
    copyMem(addr res[0], unsafeAddr bytes[0], bytes.len)
  ok(res)

proc writeBool*(s: Stream, v: bool) =
  s.writeUint8(if v: 1 else: 0)

proc readBool*(s: Stream): bool =
  s.readUint8() != 0

proc toBytes*[T](obj: T, writer: proc(s: Stream, v: T) {.gcsafe.}): BResult[seq[byte]] {.gcsafe.} =
  try:
    let s = newStringStream()
    {.cast(gcsafe).}:
      writer(s, obj)
    s.setPosition(0)
    ok(cast[seq[byte]](s.readAll()))
  except Exception as e:
    err(serializationError("serialization failed: " & e.msg))

proc fromBytes*[T](data: openArray[byte], reader: proc(s: Stream): T {.gcsafe.}): BResult[T] {.gcsafe.} =
  try:
    var str = newString(data.len)
    if data.len > 0:
      copyMem(addr str[0], unsafeAddr data[0], data.len)
    let s = newStringStream(str)
    {.cast(gcsafe).}:
      ok(reader(s))
  except Exception as e:
    err(deserializationError("deserialization failed: " & e.msg))

proc fromBytesResult*[T](data: openArray[byte], reader: proc(s: Stream): BResult[T] {.gcsafe.}): BResult[T] {.gcsafe.} =
  try:
    var str = newString(data.len)
    if data.len > 0:
      copyMem(addr str[0], unsafeAddr data[0], data.len)
    let s = newStringStream(str)
    {.cast(gcsafe).}:
      reader(s)
  except Exception as e:
    err(deserializationError("deserialization failed: " & e.msg))

type
  BlockInfo* = object
    size*: int
    refCount*: uint32

proc writeBlockInfo*(s: Stream, info: BlockInfo) {.gcsafe.} =
  s.writeUint64(info.size.uint64)
  s.writeUint32(info.refCount)

proc readBlockInfo*(s: Stream): BlockInfo {.gcsafe.} =
  result.size = s.readUint64().int
  result.refCount = s.readUint32()

proc serializeBlockInfo*(info: BlockInfo): BResult[seq[byte]] =
  toBytes(info, writeBlockInfo)

proc deserializeBlockInfo*(data: openArray[byte]): BResult[BlockInfo] =
  fromBytes(data, readBlockInfo)

type
  PendingDeletion* = object
    queuedAt*: uint64
    blockPath*: string
    size*: uint64

proc writePendingDeletion*(s: Stream, pd: PendingDeletion) {.gcsafe.} =
  s.writeUint64(pd.queuedAt)
  s.writeString(pd.blockPath)
  s.writeUint64(pd.size)

proc readPendingDeletion*(s: Stream): BResult[PendingDeletion] {.gcsafe.} =
  var pd: PendingDeletion
  pd.queuedAt = s.readUint64()
  pd.blockPath = ?s.readString()
  pd.size = s.readUint64()
  ok(pd)

proc serializePendingDeletion*(pd: PendingDeletion): BResult[seq[byte]] =
  toBytes(pd, writePendingDeletion)

proc deserializePendingDeletion*(data: openArray[byte]): BResult[PendingDeletion] =
  fromBytesResult(data, readPendingDeletion)

proc serializeUint64*(v: uint64): BResult[seq[byte]] =
  try:
    let s = newStringStream()
    s.writeUint64(v)
    s.setPosition(0)
    ok(cast[seq[byte]](s.readAll()))
  except Exception as e:
    err(serializationError("serialization failed: " & e.msg))

proc deserializeUint64*(data: openArray[byte]): BResult[uint64] =
  if data.len < 8:
    return err(deserializationError("Data too short for uint64"))
  var str = newString(data.len)
  if data.len > 0:
    copyMem(addr str[0], unsafeAddr data[0], data.len)
  let s = newStringStream(str)
  ok(s.readUint64())
