## Compile with:
##   -d:useConstantine to use Constantine's SHA256 implementation
##   -d:useBlake3 to use BLAKE3 (hashlib) for benchmarking
## Default uses nimcrypto SHA256
when defined(useBlake3):
  import hashlib/misc/blake3

  proc sha256Hash*(data: openArray[byte]): array[32, byte] =
    ## Compute BLAKE3 hash (32 bytes, same size as SHA256)
    var ctx: Context[BLAKE3]
    ctx.init()
    ctx.update(data)
    ctx.final(result)

elif defined(useConstantine):
  import constantine/hashes

  proc sha256Hash*(data: openArray[byte]): array[32, byte] =
    ## Compute SHA2-256 hash using Constantine
    result = hashes.sha256.hash(data)

else:
  import nimcrypto/sha2

  proc sha256Hash*(data: openArray[byte]): array[32, byte] =
    ## Compute SHA2-256 hash using nimcrypto
    var ctx: sha256
    ctx.init()
    ctx.update(data)
    result = ctx.finish().data
    ctx.clear()
