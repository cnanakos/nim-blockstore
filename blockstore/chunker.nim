import std/[os, options]
import chronos
import chronos/threadsync
import taskpools
import results

import ./errors
import ./blocks as blk

when defined(posix):
  import std/posix

when defined(windows):
  import std/winlean

const
  DefaultChunkSize* = 64 * 1024
  MinPoolSize* = 2 #TODO cnanakos: figure what happens when 1

type
  ChunkerConfig* = object
    chunkSize*: int

  ReadResult = object
    bytesRead: int
    hasError: bool
    error: string

  AsyncChunker* = ref object
    config: ChunkerConfig
    pool: Taskpool
    ownsPool: bool

  AsyncChunkStream* = ref object
    filePath: string
    fd: cint
    chunkSize: int
    offset: int64
    index: int
    finished: bool
    pool: Taskpool
    buffer: seq[byte]

  SyncChunker* = ref object
    config: ChunkerConfig

  SyncChunkIterator* = ref object
    file: File
    chunkSize: int
    buffer: seq[byte]
    index: int
    finished: bool


proc newChunkerConfig*(chunkSize: int = DefaultChunkSize): ChunkerConfig =
  ChunkerConfig(chunkSize: chunkSize)

proc defaultChunkerConfig*(): ChunkerConfig =
  ChunkerConfig(chunkSize: DefaultChunkSize)

proc newAsyncChunker*(pool: Taskpool): AsyncChunker =
  AsyncChunker(
    config: defaultChunkerConfig(),
    pool: pool,
    ownsPool: false
  )

proc newAsyncChunker*(pool: Taskpool, config: ChunkerConfig): AsyncChunker =
  AsyncChunker(
    config: config,
    pool: pool,
    ownsPool: false
  )

proc chunkSize*(chunker: AsyncChunker): int {.inline.} =
  chunker.config.chunkSize

proc shutdown*(chunker: AsyncChunker) =
  if chunker.ownsPool:
    chunker.pool.shutdown()

proc readChunkWorker(fd: cint, offset: int64, size: int,
                     buffer: ptr byte,
                     signal: ThreadSignalPtr,
                     resultPtr: ptr ReadResult) {.gcsafe.} =
  when defined(posix):
    let bytesRead = pread(fd, buffer, size, offset.Off)
    if bytesRead < 0:
      resultPtr[].hasError = true
      resultPtr[].error = "Read error: " & $strerror(errno)
    else:
      resultPtr[].bytesRead = bytesRead.int
      resultPtr[].hasError = false
  elif defined(windows):
    var
      overlapped: OVERLAPPED
      bytesRead: DWORD
    overlapped.Offset = cast[DWORD](offset and 0xFFFFFFFF'i64)
    overlapped.OffsetHigh = cast[DWORD](offset shr 32)
    let success = readFile(fd.Handle, buffer, size.DWORD, addr bytesRead, addr overlapped)
    if success == 0:
      resultPtr[].hasError = true
      resultPtr[].error = "Read error"
    else:
      resultPtr[].bytesRead = bytesRead.int
      resultPtr[].hasError = false
  else:
    {.error: "Unsupported platform".}

  discard signal.fireSync()

proc chunkFile*(chunker: AsyncChunker, filePath: string): Future[BResult[AsyncChunkStream]] {.async.} =
  if not fileExists(filePath):
    return err(ioError("File not found: " & filePath))

  when defined(posix):
    let fd = open(filePath.cstring, O_RDONLY)
    if fd < 0:
      return err(ioError("Cannot open file: " & filePath))
  elif defined(windows):
    let fd = createFileA(filePath.cstring, GENERIC_READ, FILE_SHARE_READ,
                         nil, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0)
    if fd == INVALID_HANDLE_VALUE:
      return err(ioError("Cannot open file: " & filePath))
  else:
    {.error: "Unsupported platform".}

  let stream = AsyncChunkStream(
    filePath: filePath,
    fd: fd.cint,
    chunkSize: chunker.config.chunkSize,
    offset: 0,
    index: 0,
    finished: false,
    pool: chunker.pool,
    buffer: newSeq[byte](chunker.config.chunkSize)
  )
  return ok(stream)

proc currentIndex*(stream: AsyncChunkStream): int {.inline.} =
  stream.index

proc isFinished*(stream: AsyncChunkStream): bool {.inline.} =
  stream.finished

proc nextBlock*(stream: AsyncChunkStream): Future[Option[BResult[blk.Block]]] {.async.} =
  if stream.finished:
    return none(BResult[blk.Block])

  let signalResult = ThreadSignalPtr.new()
  if signalResult.isErr:
    stream.finished = true
    return some(BResult[blk.Block].err(ioError("Failed to create signal")))

  let signal = signalResult.get()
  var readResult: ReadResult

  stream.pool.spawn readChunkWorker(stream.fd, stream.offset, stream.chunkSize,
                                     addr stream.buffer[0], signal, addr readResult)

  try:
    await signal.wait()
  except AsyncError as e:
    discard signal.close()
    stream.finished = true
    return some(BResult[blk.Block].err(ioError("Signal wait failed: " & e.msg)))
  except CancelledError:
    discard signal.close()
    stream.finished = true
    return some(BResult[blk.Block].err(ioError("Operation cancelled")))

  discard signal.close()

  if readResult.hasError:
    stream.finished = true
    return some(BResult[blk.Block].err(ioError(readResult.error)))

  if readResult.bytesRead == 0:
    stream.finished = true
    return none(BResult[blk.Block])

  let
    data = stream.buffer[0 ..< readResult.bytesRead]
    blockResult = blk.newBlock(data)

  stream.offset += readResult.bytesRead
  stream.index += 1

  return some(blockResult)

proc close*(stream: AsyncChunkStream) =
  if not stream.finished:
    when defined(posix):
      discard posix.close(stream.fd)
    elif defined(windows):
      discard closeHandle(stream.fd.Handle)
  stream.finished = true

proc newSyncChunker*(): SyncChunker =
  SyncChunker(config: defaultChunkerConfig())

proc newSyncChunker*(config: ChunkerConfig): SyncChunker =
  SyncChunker(config: config)

proc chunkFile*(chunker: SyncChunker, filePath: string): BResult[SyncChunkIterator] =
  if not fileExists(filePath):
    return err(ioError("File not found: " & filePath))

  var file: File
  if not open(file, filePath, fmRead):
    return err(ioError("Cannot open file: " & filePath))

  let iter = SyncChunkIterator(
    file: file,
    chunkSize: chunker.config.chunkSize,
    buffer: newSeq[byte](chunker.config.chunkSize),
    index: 0,
    finished: false
  )
  return ok(iter)

proc currentIndex*(iter: SyncChunkIterator): int {.inline.} =
  iter.index

proc isFinished*(iter: SyncChunkIterator): bool {.inline.} =
  iter.finished

proc nextBlock*(iter: SyncChunkIterator): Option[BResult[blk.Block]] =
  if iter.finished:
    return none(BResult[blk.Block])

  try:
    let bytesRead = iter.file.readBytes(iter.buffer, 0, iter.chunkSize)

    if bytesRead == 0:
      iter.finished = true
      return none(BResult[blk.Block])

    let
      data = iter.buffer[0 ..< bytesRead]
      blockResult = blk.newBlock(data)
    iter.index += 1
    return some(blockResult)

  except IOError as e:
    iter.finished = true
    return some(BResult[blk.Block].err(ioError(e.msg)))

proc close*(iter: SyncChunkIterator) =
  iter.file.close()
  iter.finished = true

proc chunkData*(data: openArray[byte], chunkSize: int = DefaultChunkSize): seq[BResult[blk.Block]] =
  result = @[]
  var offset = 0
  while offset < data.len:
    let
      endOffset = min(offset + chunkSize, data.len)
      chunk = data[offset ..< endOffset]
    result.add(blk.newBlock(@chunk))
    offset = endOffset
