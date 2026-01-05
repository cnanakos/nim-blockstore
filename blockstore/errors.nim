import std/strformat
import results

type
  BlockstoreErrorKind* = enum
    IoError = "IO error"
    SerializationError = "Serialization error"
    DeserializationError = "Deserialization error"
    CidError = "CID error"
    MultihashError = "Multihash error"
    DatabaseError = "Database error"
    InvalidBlock = "Invalid block data"
    BlockNotFound = "Block not found"
    MerkleTreeError = "Merkle tree error"
    DatasetNotFound = "Dataset not found"
    QuotaExceeded = "Quota exceeded"
    InvalidProof = "Invalid merkle proof"
    InvalidProofHashLength = "Invalid merkle proof hash length"
    ManifestEncodingError = "Manifest encoding error"
    ManifestDecodingError = "Manifest decoding error"
    BackendMismatch = "Backend mismatch"
    InvalidOperation = "Invalid operation"

  BlockstoreError* = object
    kind*: BlockstoreErrorKind
    msg*: string

type
  BlockstoreResult*[T] = Result[T, BlockstoreError]
  BResult*[T] = BlockstoreResult[T]

proc newBlockstoreError*(kind: BlockstoreErrorKind, msg: string = ""): BlockstoreError =
  BlockstoreError(kind: kind, msg: msg)

proc ioError*(msg: string): BlockstoreError =
  newBlockstoreError(IoError, msg)

proc serializationError*(msg: string): BlockstoreError =
  newBlockstoreError(SerializationError, msg)

proc deserializationError*(msg: string): BlockstoreError =
  newBlockstoreError(DeserializationError, msg)

proc cidError*(msg: string): BlockstoreError =
  newBlockstoreError(CidError, msg)

proc multihashError*(msg: string): BlockstoreError =
  newBlockstoreError(MultihashError, msg)

proc databaseError*(msg: string): BlockstoreError =
  newBlockstoreError(DatabaseError, msg)

proc invalidBlockError*(): BlockstoreError =
  newBlockstoreError(InvalidBlock)

proc blockNotFoundError*(cid: string): BlockstoreError =
  newBlockstoreError(BlockNotFound, cid)

proc merkleTreeError*(msg: string): BlockstoreError =
  newBlockstoreError(MerkleTreeError, msg)

proc datasetNotFoundError*(): BlockstoreError =
  newBlockstoreError(DatasetNotFound)

proc quotaExceededError*(): BlockstoreError =
  newBlockstoreError(QuotaExceeded)

proc invalidProofError*(): BlockstoreError =
  newBlockstoreError(InvalidProof)

proc invalidProofHashLengthError*(): BlockstoreError =
  newBlockstoreError(InvalidProofHashLength)

proc manifestEncodingError*(msg: string): BlockstoreError =
  newBlockstoreError(ManifestEncodingError, msg)

proc manifestDecodingError*(msg: string): BlockstoreError =
  newBlockstoreError(ManifestDecodingError, msg)

proc backendMismatchError*(msg: string): BlockstoreError =
  newBlockstoreError(BackendMismatch, msg)

proc invalidOperationError*(msg: string): BlockstoreError =
  newBlockstoreError(InvalidOperation, msg)

proc `$`*(e: BlockstoreError): string =
  if e.msg.len > 0:
    fmt"{e.kind}: {e.msg}"
  else:
    $e.kind
