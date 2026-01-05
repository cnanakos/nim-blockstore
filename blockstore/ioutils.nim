import std/os
import results
import ./errors
import ./directio

when defined(posix):
  import std/posix

export PageSize, MinChunkSize, isPowerOfTwo, alignUp
export AlignedBuffer, newAlignedBuffer, free, copyFrom, clear

type
  IOMode* = enum
    ioDirect
    ioBuffered

  SyncPolicyKind* = enum
    spNone
    spEveryWrite
    spEveryN

  SyncPolicy* = object
    case kind*: SyncPolicyKind
    of spNone: discard
    of spEveryWrite: discard
    of spEveryN: n*: int

  WriteHandle* = ref object
    case mode*: IOMode
    of ioDirect:
      directFile: DirectFile
      alignedBuf: AlignedBuffer
    of ioBuffered:
      file: File
    path: string
    offset: int64
    chunkSize: int
    syncPolicy: SyncPolicy
    writeCount: int

proc syncNone*(): SyncPolicy =
  SyncPolicy(kind: spNone)

proc syncEveryWrite*(): SyncPolicy =
  SyncPolicy(kind: spEveryWrite)

proc syncEveryN*(n: int): SyncPolicy =
  SyncPolicy(kind: spEveryN, n: n)

proc openForWrite*(path: string, mode: IOMode, chunkSize: int,
                   syncPolicy: SyncPolicy = syncNone()): BResult[WriteHandle] =
  let parentPath = parentDir(path)
  if parentPath.len > 0:
    try:
      createDir(parentPath)
    except OSError as e:
      return err(ioError("Failed to create directory: " & e.msg))

  case mode
  of ioDirect:
    let dfResult = directio.openForWrite(path)
    if dfResult.isErr:
      return err(dfResult.error)

    let alignedSize = alignUp(chunkSize)
    var buf = newAlignedBuffer(alignedSize)

    ok(WriteHandle(
      mode: ioDirect,
      directFile: dfResult.value,
      alignedBuf: buf,
      path: path,
      offset: 0,
      chunkSize: chunkSize,
      syncPolicy: syncPolicy,
      writeCount: 0
    ))

  of ioBuffered:
    try:
      let f = open(path, fmWrite)
      ok(WriteHandle(
        mode: ioBuffered,
        file: f,
        path: path,
        offset: 0,
        chunkSize: chunkSize,
        syncPolicy: syncPolicy,
        writeCount: 0
      ))
    except IOError as e:
      err(ioError("Failed to open file: " & e.msg))

proc shouldSync(h: WriteHandle): bool {.inline.} =
  case h.syncPolicy.kind
  of spNone: false
  of spEveryWrite: true
  of spEveryN: h.writeCount mod h.syncPolicy.n == 0

proc syncFile(h: WriteHandle): BResult[void] =
  case h.mode
  of ioDirect:
    when defined(macosx):
      let syncResult = h.directFile.sync()
      if syncResult.isErr:
        return err(syncResult.error)
    ok()
  of ioBuffered:
    try:
      h.file.flushFile()
      when defined(posix):
        if fsync(h.file.getFileHandle().cint) < 0:
          return err(ioError("Sync failed"))
      ok()
    except IOError as e:
      err(ioError("Sync failed: " & e.msg))

proc writeBlock*(h: WriteHandle, data: openArray[byte]): BResult[int] =
  case h.mode
  of ioDirect:
    h.alignedBuf.copyFrom(data)
    let writeResult = h.directFile.writeAligned(h.alignedBuf)
    if writeResult.isErr:
      return err(writeResult.error)

    h.offset += data.len.int64
    h.writeCount += 1

    if h.shouldSync():
      let syncResult = h.syncFile()
      if syncResult.isErr:
        return err(syncResult.error)

    ok(data.len)

  of ioBuffered:
    if h.syncPolicy.kind == spNone:
      let written = h.file.writeBytes(data, 0, data.len)
      h.offset += written.int64
      return ok(written)

    try:
      let written = h.file.writeBytes(data, 0, data.len)
      if written != data.len:
        return err(ioError("Incomplete write: " & $written & "/" & $data.len))
      h.offset += written.int64
      h.writeCount += 1

      if h.shouldSync():
        let syncResult = h.syncFile()
        if syncResult.isErr:
          return err(syncResult.error)

      ok(written)
    except IOError as e:
      err(ioError("Write failed: " & e.msg))

proc currentOffset*(h: WriteHandle): int64 {.inline.} =
  h.offset

proc finalize*(h: WriteHandle, actualSize: int64): BResult[void] =
  case h.mode
  of ioDirect:
    let truncResult = h.directFile.truncateFile(actualSize)
    if truncResult.isErr:
      return err(truncResult.error)

    when defined(macosx):
      let syncResult = h.directFile.sync()
      if syncResult.isErr:
        return err(syncResult.error)

    ok()

  of ioBuffered:
    try:
      h.file.flushFile()
      when defined(posix):
        if fsync(h.file.getFileHandle().cint) < 0:
          return err(ioError("Sync failed"))
      ok()
    except IOError as e:
      err(ioError("Finalize failed: " & e.msg))

proc close*(h: WriteHandle) =
  case h.mode
  of ioDirect:
    h.directFile.close()
    var buf = h.alignedBuf
    buf.free()
  of ioBuffered:
    try:
      h.file.close()
    except CatchableError:
      discard

proc writeBlockToFile*(path: string, data: openArray[byte], mode: IOMode): BResult[void] =
  let parentPath = parentDir(path)
  if parentPath.len > 0:
    try:
      createDir(parentPath)
    except OSError as e:
      return err(ioError("Failed to create directory: " & e.msg))

  case mode
  of ioDirect:
    directio.writeBlockDirect(path, data)

  of ioBuffered:
    try:
      var f = open(path, fmWrite)
      defer: f.close()

      let written = f.writeBytes(data, 0, data.len)
      if written != data.len:
        return err(ioError("Incomplete write"))

      ok()
    except IOError as e:
      err(ioError("Write failed: " & e.msg))

proc writeBlockBuffered*(path: string, data: openArray[byte]): BResult[File] =
  let parentPath = parentDir(path)
  if parentPath.len > 0:
    try:
      createDir(parentPath)
    except OSError as e:
      return err(ioError("Failed to create directory: " & e.msg))

  try:
    var f = open(path, fmWrite)
    let written = f.writeBytes(data, 0, data.len)
    if written != data.len:
      f.close()
      return err(ioError("Incomplete write"))
    ok(f)
  except IOError as e:
    err(ioError("Write failed: " & e.msg))

proc syncAndCloseFile*(f: File): BResult[void] =
  try:
    f.flushFile()
    when defined(posix):
      if fsync(f.getFileHandle().cint) < 0:
        f.close()
        return err(ioError("Sync failed"))
    when defined(windows):
      import std/winlean
      if flushFileBuffers(f.getFileHandle()) == 0:
        f.close()
        return err(ioError("Sync failed"))
    f.close()
    ok()
  except IOError as e:
    try: f.close()
    except CatchableError: discard
    err(ioError("Sync failed: " & e.msg))

proc validateChunkSize*(chunkSize: uint32): BResult[void] =
  if chunkSize < PageSize.uint32:
    return err(ioError("Chunk size must be >= " & $PageSize & " bytes"))
  if not isPowerOfTwo(chunkSize):
    return err(ioError("Chunk size must be power of 2"))
  ok()

proc syncDir*(dirPath: string): BResult[void] =
  when defined(posix):
    let fd = posix.open(dirPath.cstring, O_RDONLY)
    if fd < 0:
      return err(ioError("Failed to open directory for sync: " & dirPath))
    if fsync(fd) < 0:
      discard posix.close(fd)
      return err(ioError("Failed to sync directory: " & dirPath))
    discard posix.close(fd)
    ok()
  else:
    ok()

proc atomicRename*(srcPath: string, dstPath: string): BResult[void] =
  try:
    moveFile(srcPath, dstPath)
    ?syncDir(parentDir(dstPath))
    ok()
  except OSError as e:
    err(ioError("Failed to rename file: " & e.msg))
  except Exception as e:
    err(ioError("Failed to rename file: " & e.msg))
