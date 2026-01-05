import std/hashes
import results
import libp2p/cid as libp2pCid
import libp2p/[multicodec, multihash]
import ./errors

type
  Cid* = libp2pCid.Cid
  CidVersion* = libp2pCid.CidVersion
  CidError* = libp2pCid.CidError

const
  CIDv0* = libp2pCid.CIDv0
  CIDv1* = libp2pCid.CIDv1

  LogosStorageManifest* = multiCodec("logos-storage-manifest")
  LogosStorageBlock* = multiCodec("logos-storage-block")
  LogosStorageTree* = multiCodec("logos-storage-tree")
  Sha256Code* = multiCodec("sha2-256")
  Sha256DigestSize* = 32
  Base32Alphabet* = "abcdefghijklmnopqrstuvwxyz234567"

  Base32DecodeTable*: array[256, int8] = block:
    var t: array[256, int8]
    for i in 0..255:
      t[i] = -1
    for i, c in Base32Alphabet:
      t[ord(c)] = int8(i)
      t[ord(c) - 32] = int8(i)  # uppercase
    t

proc wrap*(code: MultiCodec, digest: openArray[byte]): BResult[MultiHash] =
  let mhResult = MultiHash.init(code, digest)
  if mhResult.isErr:
    return err(multihashError("Failed to create multihash"))
  ok(mhResult.get())

proc newCidV1*(codec: MultiCodec, mh: MultiHash): BResult[Cid] =
  let cidResult = Cid.init(libp2pCid.CIDv1, codec, mh)
  if cidResult.isErr:
    return err(cidError("Failed to create CID: " & $cidResult.error))
  ok(cidResult.get())

proc toBytes*(c: Cid): seq[byte] =
  c.data.buffer

proc mhash*(c: Cid): Result[MultiHash, CidError] =
  libp2pCid.mhash(c)

proc cidFromBytes*(data: openArray[byte]): BResult[Cid] =
  let cidResult = Cid.init(data)
  if cidResult.isErr:
    return err(cidError("Failed to parse CID: " & $cidResult.error))
  ok(cidResult.get())

proc base32Encode*(data: openArray[byte]): string =
  if data.len == 0:
    return ""

  result = ""
  var
    buffer: uint64 = 0
    bits = 0

  for b in data:
    buffer = (buffer shl 8) or b.uint64
    bits += 8
    while bits >= 5:
      bits -= 5
      let idx = (buffer shr bits) and 0x1F
      result.add(Base32Alphabet[idx.int])

  if bits > 0:
    let idx = (buffer shl (5 - bits)) and 0x1F
    result.add(Base32Alphabet[idx.int])

proc base32Decode*(s: string): BResult[seq[byte]] =
  if s.len == 0:
    return ok(newSeq[byte]())

  var
    buffer: uint64 = 0
    bits = 0
    res: seq[byte] = @[]

  for c in s:
    let idx = Base32DecodeTable[ord(c)]
    if idx < 0:
      return err(cidError("Invalid base32 character: " & $c))

    buffer = (buffer shl 5) or idx.uint64
    bits += 5

    if bits >= 8:
      bits -= 8
      res.add(((buffer shr bits) and 0xFF).byte)

  ok(res)

proc `$`*(c: Cid): string =
  "b" & base32Encode(c.data.buffer)

proc cidFromString*(s: string): BResult[Cid] =
  if s.len < 2:
    return err(cidError("CID string too short"))

  if s[0] == 'b':
    let decoded = ?base32Decode(s[1 .. ^1])
    return cidFromBytes(decoded)
  else:
    let cidResult = Cid.init(s)
    if cidResult.isErr:
      return err(cidError("Failed to parse CID: " & $cidResult.error))
    ok(cidResult.get())

proc `<`*(a, b: Cid): bool =
  let
    aData = a.data.buffer
    bData = b.data.buffer
    minLen = min(aData.len, bData.len)
  for i in 0 ..< minLen:
    if aData[i] < bData[i]: return true
    elif aData[i] > bData[i]: return false
  aData.len < bData.len

proc cmp*(a, b: Cid): int =
  if a < b: -1
  elif b < a: 1
  else: 0

proc hash*(c: Cid): Hash {.inline.} =
  hash(c.data.buffer)
