import std/os
import results

import ./errors
import ./cid

const
  Base32Chars* = "abcdefghijklmnopqrstuvwxyz234567"
  TmpDirName* = "tmp"

proc initShardDirectories*(baseDir: string): BResult[void] =
  let marker = baseDir / ".shards_initialized"

  if fileExists(marker):
    return ok()

  try:
    createDir(baseDir)
    discard existsOrCreateDir(baseDir / TmpDirName)

    for c1 in Base32Chars:
      let level1 = baseDir / $c1
      discard existsOrCreateDir(level1)
      for c2 in Base32Chars:
        let level2 = level1 / $c2
        discard existsOrCreateDir(level2)

    writeFile(marker, "")
    ok()
  except OSError as e:
    err(ioError(e.msg))

proc cleanupTmpDir*(baseDir: string) =
  let tmpDir = baseDir / TmpDirName
  if dirExists(tmpDir):
    try:
      removeDir(tmpDir)
      createDir(tmpDir)
    except OSError:
      discard

proc getTmpPath*(baseDir: string, name: string, ext: string = ""): string =
  baseDir / TmpDirName / (name & ext)

proc getShardedPathStr*(baseDir: string, cidStr: string, ext: string = ""): string =
  let len = cidStr.len
  let d1 = cidStr[len - 2 .. len - 2]
  let d2 = cidStr[len - 1 .. len - 1]
  baseDir / d1 / d2 / (cidStr & ext)

proc getShardedPath*(baseDir: string, c: Cid, ext: string = ""): string =
  getShardedPathStr(baseDir, $c, ext)
