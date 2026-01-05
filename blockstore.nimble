# Package

version       = "0.1.0"
author        = "Status Research & Development GmbH"
description   = "Nim blockstore"
license       = "Apache License 2.0 or MIT"
srcDir        = "blockstore"

requires "nim >= 2.2.4"
requires "nimcrypto >= 0.6.0"
requires "leveldbstatic >= 0.1.0"
requires "results >= 0.4.0"
requires "chronos >= 4.0.0"
requires "libp2p >= 1.14.1 & < 2.0.0"
requires "constantine >= 0.2.0"
requires "taskpools >= 0.0.5"
requires "hashlib >= 1.0.0"

task test, "Run the test suite":
  exec "nim c -r tests/test_block.nim"
  exec "nim c -r tests/test_merkle.nim"
  exec "nim c -r tests/test_chunker.nim"
  exec "nim c -r tests/test_dataset.nim"

task test_constantine, "Run the test suite":
  exec "nim c -d:useConstantine -r tests/test_block.nim"
  exec "nim c -d:useConstantine -r tests/test_merkle.nim"
  exec "nim c -d:useConstantine -r tests/test_chunker.nim"
  exec "nim c -d:useConstantine -r tests/test_dataset.nim"

task test_blake3, "Run the test suite":
  exec "nim c -d:useBlake3 -r tests/test_block.nim"
  exec "nim c -d:useBlake3 -r tests/test_merkle.nim"
  exec "nim c -d:useBlake3 -r tests/test_chunker.nim"
  exec "nim c -d:useBlake3 -r tests/test_dataset.nim"

task test_clean, "Clean test binaries":
  exec "rm tests/test_block"
  exec "rm tests/test_merkle"
  exec "rm tests/test_chunker"
  exec "rm tests/test_dataset"

task benchmark, "Compile dataset benchmark":
  exec "nim c --hints:off -d:release -r tests/bench_dataset.nim"

task benchmark_constantine, "Compile dataset benchmark with constantine":
  exec "nim c --hints:off -d:release -d:useConstantine -r tests/bench_dataset.nim"

task benchmark_blake3, "Compile dataset benchmark with BLAKE3":
  exec "nim c --hints:off -d:release -d:useBlake3 -r tests/bench_dataset.nim"

task benchmark_merkle, "Compile merkle benchmark":
  exec "nim c --hints:off -d:release -r tests/bench_merkle.nim"

task benchmark_merkle_constantine, "Compile merkle benchmark":
  exec "nim c --hints:off -d:release -d:useConstantine -r tests/bench_merkle.nim"

task benchmark_merkle_blake3, "Compile merkle benchmark":
  exec "nim c --hints:off -d:release -d:useBlake3 -r tests/bench_merkle.nim"

const
  nimcacheBase = ".nimcache"
  coverageFlags = "--passC:\"-fprofile-arcs -ftest-coverage\" --passL:\"-fprofile-arcs -ftest-coverage\""
  coverageDir = "coverage_report"

proc runCoverage(testFile: string, reportName: string) =
  let nimcacheDir = nimcacheBase & "/" & reportName
  exec "nim c " & coverageFlags & " --nimcache:" & nimcacheDir & " -r tests/" & testFile & ".nim"
  exec "lcov --capture --directory " & nimcacheDir & " --output-file " & reportName & ".info --quiet"
  exec "lcov --extract " & reportName & ".info '*@sblockstore@s*' --output-file " & reportName & "_filtered.info --quiet"
  exec "genhtml " & reportName & "_filtered.info --output-directory " & coverageDir & "/" & reportName & " --quiet"
  exec "rm -f " & reportName & ".info " & reportName & "_filtered.info"
  echo "Coverage report: " & coverageDir & "/" & reportName & "/index.html"

task coverage, "Run all tests with coverage and generate HTML report":
  mkDir(coverageDir)
  mkDir(nimcacheBase)
  exec "nim c " & coverageFlags & " --nimcache:" & nimcacheBase & "/test_block -r tests/test_block.nim"
  exec "nim c " & coverageFlags & " --nimcache:" & nimcacheBase & "/test_merkle -r tests/test_merkle.nim"
  exec "nim c " & coverageFlags & " --nimcache:" & nimcacheBase & "/test_chunker -r tests/test_chunker.nim"
  exec "lcov --capture --directory " & nimcacheBase & "/test_block --directory " & nimcacheBase & "/test_merkle --directory " & nimcacheBase & "/test_chunker --output-file all_coverage.info --quiet"
  exec "lcov --extract all_coverage.info '*@sblockstore@s*' --output-file blockstore_coverage.info --quiet"
  exec "genhtml blockstore_coverage.info --output-directory " & coverageDir & "/all --quiet"
  exec "rm -f all_coverage.info blockstore_coverage.info"
  echo "Coverage report: " & coverageDir & "/all/index.html"

task coverage_merkle, "Run merkle tests with coverage":
  mkDir(coverageDir)
  mkDir(nimcacheBase)
  runCoverage("test_merkle", "test_merkle")

task coverage_block, "Run block tests with coverage":
  mkDir(coverageDir)
  mkDir(nimcacheBase)
  runCoverage("test_block", "test_block")

task coverage_chunker, "Run chunker tests with coverage":
  mkDir(coverageDir)
  mkDir(nimcacheBase)
  runCoverage("test_chunker", "test_chunker")

task coverage_bench_merkle, "Run merkle benchmark with coverage":
  mkDir(coverageDir)
  mkDir(nimcacheBase)
  let nimcacheDir = nimcacheBase & "/bench_merkle"
  exec "nim c " & coverageFlags & " --nimcache:" & nimcacheDir & " -r tests/bench_merkle.nim --size=100MB"
  exec "lcov --capture --directory " & nimcacheDir & " --output-file bench_merkle.info --quiet"
  exec "lcov --extract bench_merkle.info '*@sblockstore@s*' --output-file bench_merkle_filtered.info --quiet"
  exec "genhtml bench_merkle_filtered.info --output-directory " & coverageDir & "/bench_merkle --quiet"
  exec "rm -f bench_merkle.info bench_merkle_filtered.info"
  echo "Coverage report: " & coverageDir & "/bench_merkle/index.html"

task coverage_clean, "Clean coverage data and reports":
  exec "rm -rf " & coverageDir
  exec "rm -rf " & nimcacheBase
  echo "Coverage data cleaned"
