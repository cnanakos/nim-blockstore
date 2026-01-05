import std/os
import results
import ./errors

when defined(posix):
  import std/posix
  proc c_free(p: pointer) {.importc: "free", header: "<stdlib.h>".}

when defined(linux):
  const O_DIRECT* = cint(0o40000)

when defined(macosx):
  const F_NOCACHE* = cint(48)

const
  PageSize* = 4096
  MinChunkSize* = PageSize

type
  AlignedBuffer* = object
    data*: ptr UncheckedArray[byte]
    size*: int
    capacity*: int

  DirectFile* = ref object
    fd: cint
    path: string
    offset: int64

proc isPowerOfTwo*(x: uint32): bool {.inline.} =
  x > 0 and (x and (x - 1)) == 0

proc alignUp*(size: int, alignment: int = PageSize): int {.inline.} =
  (size + alignment - 1) and not (alignment - 1)

proc newAlignedBuffer*(size: int): AlignedBuffer =
  let alignedSize = alignUp(size)
  when defined(posix):
    var p: pointer
    let rc = posix_memalign(addr p, PageSize.csize_t, alignedSize.csize_t)
    if rc != 0:
      raise newException(OutOfMemDefect, "Failed to allocate aligned memory")
    result.data = cast[ptr UncheckedArray[byte]](p)
  else:
    let
      raw = alloc0(alignedSize + PageSize)
      aligned = (cast[int](raw) + PageSize - 1) and not (PageSize - 1)
    result.data = cast[ptr UncheckedArray[byte]](aligned)

  result.size = 0
  result.capacity = alignedSize
  zeroMem(result.data, alignedSize)

proc free*(buf: var AlignedBuffer) =
  if buf.data != nil:
    when defined(posix):
      c_free(buf.data)
    else:
      dealloc(buf.data)
    buf.data = nil
    buf.size = 0
    buf.capacity = 0

proc copyFrom*(buf: var AlignedBuffer, data: openArray[byte]) =
  if data.len > buf.capacity:
    raise newException(ValueError, "Data exceeds buffer capacity")

  if data.len > 0:
    copyMem(buf.data, unsafeAddr data[0], data.len)

  if data.len < buf.capacity:
    zeroMem(addr buf.data[data.len], buf.capacity - data.len)

  buf.size = data.len

proc clear*(buf: var AlignedBuffer) =
  zeroMem(buf.data, buf.capacity)
  buf.size = 0

proc openForWrite*(path: string): BResult[DirectFile] =
  when defined(linux):
    let
      flags = O_WRONLY or O_CREAT or O_TRUNC or O_DIRECT
      fd = posix.open(path.cstring, flags, 0o644)
    if fd < 0:
      return err(ioError("Failed to open file for direct I/O: " & path & " (errno: " & $errno & ")"))
    ok(DirectFile(fd: fd, path: path, offset: 0))

  elif defined(macosx):
    let
      flags = O_WRONLY or O_CREAT or O_TRUNC
      fd = posix.open(path.cstring, flags, 0o644)
    if fd < 0:
      return err(ioError("Failed to open file: " & path))
    if fcntl(fd, F_NOCACHE, 1) < 0:
      discard posix.close(fd)
      return err(ioError("Failed to set F_NOCACHE: " & path))
    ok(DirectFile(fd: fd, path: path, offset: 0))

  elif defined(posix):
    err(ioError("Direct I/O not supported on this platform"))

  else:
    err(ioError("Direct I/O not supported on this platform"))

proc writeAligned*(f: DirectFile, buf: AlignedBuffer): BResult[int] =
  when defined(posix):
    let
      toWrite = buf.capacity
      written = posix.write(f.fd, cast[pointer](buf.data), toWrite)
    if written < 0:
      return err(ioError("Direct write failed (errno: " & $errno & ")"))
    if written != toWrite:
      return err(ioError("Incomplete direct write: " & $written & "/" & $toWrite))
    f.offset += written
    ok(written.int)
  else:
    err(ioError("Direct I/O not supported"))

proc truncateFile*(f: DirectFile, size: int64): BResult[void] =
  when defined(posix):
    if ftruncate(f.fd, size.Off) < 0:
      return err(ioError("Failed to truncate file (errno: " & $errno & ")"))
    ok()
  else:
    err(ioError("Truncate not supported"))

proc currentOffset*(f: DirectFile): int64 {.inline.} =
  f.offset

proc close*(f: DirectFile) =
  if f != nil and f.fd >= 0:
    when defined(posix):
      discard posix.close(f.fd)
    f.fd = -1

proc sync*(f: DirectFile): BResult[void] =
  when defined(posix):
    if fsync(f.fd) < 0:
      return err(ioError("Failed to sync file"))
    ok()
  else:
    ok()

proc writeBlockDirect*(path: string, data: openArray[byte]): BResult[void] =
  let parentPath = parentDir(path)
  if parentPath.len > 0:
    try:
      createDir(parentPath)
    except OSError as e:
      return err(ioError("Failed to create directory: " & e.msg))

  let fileResult = openForWrite(path)
  if fileResult.isErr:
    return err(fileResult.error)

  let f = fileResult.value
  defer: f.close()

  let alignedSize = alignUp(data.len)
  var buf = newAlignedBuffer(alignedSize)
  defer: buf.free()

  buf.copyFrom(data)

  let writeResult = f.writeAligned(buf)
  if writeResult.isErr:
    return err(writeResult.error)

  let truncResult = f.truncateFile(data.len.int64)
  if truncResult.isErr:
    return err(truncResult.error)

  when defined(macosx):
    let syncResult = f.sync()
    if syncResult.isErr:
      return err(syncResult.error)

  ok()
