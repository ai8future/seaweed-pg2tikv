Date Created: 2026-03-21T23:00:00Z
TOTAL_SCORE: 58/100

# seaweed-pg2tikv Refactoring & Code Quality Report

**Agent:** Claude Code (Claude:Opus 4.6)
**Scope:** main.go, audit.go, count_keys.go, seaweed-cleanup-go/main.go

---

## Executive Summary

The codebase is a functional, production-tested migration suite with strong operational features (resume, graceful shutdown, parallel workers, retry logic). The core algorithms are solid and the code demonstrates good engineering judgment around safety and correctness. However, significant structural issues — primarily massive code duplication between files and zero test coverage — bring the overall quality score down substantially.

---

## Critical Issues

### 1. Massive Code Duplication Between main.go and audit.go (-15 pts)

This is the single largest quality problem. The following code is **copy-pasted verbatim** between `main.go` and `audit.go`:

| Duplicated Element | Lines (each file) |
|---|---|
| `FilerConfig` struct | 4 |
| `Postgres2Config` struct + `ConnectionString()` | 30 |
| `TiKVConfig` struct + `PDAddrList()` | 16 |
| `hashToBytes()` | 4 |
| `generateTiKVKey()` | 10 |
| `applyPathPrefix()` | 8 |
| `Entry` struct | 6 |
| `MinInt64` constant | 1 |
| `validTableName` regex | 1 |
| `Version` constant | 1 |

**Total: ~81 lines duplicated.** Any bug fix to config parsing, key generation, or path prefix logic must be applied in both files — a guaranteed source of future inconsistency.

**Recommended fix:** Extract shared types and functions into a `shared/` or `internal/config/` package. Alternatively, restructure the project using `cmd/` layout (see #3).

### 2. Zero Test Coverage (-12 pts)

There are no test files anywhere in the project. Functions like `generateTiKVKey()`, `applyPathPrefix()`, `hashToBytes()`, `prefixEndKey()`, `parseProtoFields()`, and the progress tracker are all pure functions or have minimal dependencies — they're ideal candidates for unit testing.

The `ProgressTracker` in particular has non-trivial contiguous-sequence tracking logic that is critical for correctness (a bug here could cause data loss on resume). It should have thorough test coverage.

### 3. Non-Standard Project Structure (-8 pts)

All three tools (`main.go`, `audit.go`, `count_keys.go`) declare `package main` with duplicate symbol names (`Version`, `FilerConfig`, `showVersion`, etc.). They cannot be compiled together — each must be built individually:

```
go build -o pg2tikv main.go
go build -o audit audit.go
go build -o count-keys count_keys.go
```

This is fragile and non-obvious. The standard Go pattern for multi-binary projects:

```
cmd/
  pg2tikv/main.go
  audit/main.go
  count-keys/main.go
internal/
  config/config.go      # shared FilerConfig, Postgres2Config, TiKVConfig
  tikv/keys.go          # shared hashToBytes, generateTiKVKey, applyPathPrefix
```

### 4. Version Constants Are Inconsistent and Hard-Coded (-5 pts)

| File | Version Constant |
|---|---|
| `main.go` | `1.2.0` |
| `audit.go` | `1.2.2` |
| `count_keys.go` | `1.2.0` |
| `VERSION` file | `1.2.6` (per git status) |

The source code versions are stale and don't match each other or the VERSION file. These should be injected at build time via `-ldflags`:

```
go build -ldflags "-X main.Version=$(cat VERSION)" -o pg2tikv main.go
```

---

## Moderate Issues

### 5. TLS Validation Pattern Repeated 3x (-3 pts)

The pattern of validating CA/cert/key paths with `os.Stat()` is repeated in `main.go:470-484`, `audit.go:785-799`, and `count_keys.go:65-79`. Should be a shared function:

```go
func validateTLSPaths(ca, cert, key string) error { ... }
```

### 6. seaweed-cleanup-go: Long Functions and Repeated Retry Pattern (-5 pts)

- `main()` is 406 lines long (842-1248)
- `trySmartdataMigration()` is 135 lines
- The 3-attempt retry-with-sleep pattern appears at least 6 times:

```go
for attempt := 0; attempt < 3; attempt++ {
    // do thing
    if err == nil {
        break
    }
    time.Sleep(time.Duration(attempt+1) * time.Second)
}
```

A simple retry helper would reduce this:

```go
func withRetry(attempts int, fn func() error) error { ... }
```

### 7. Unused Variables Left in Code (-2 pts)

In `seaweed-cleanup-go/main.go`:
- Line 882: `_ = *dryRun` — the `dryRun` flag is parsed but never actually checked
- Line 1058: `_ = lastRateCount` — assigned but suppressed
- Line 1062: `_ = d` — dupes counter loaded but suppressed

These suggest dead code paths or incomplete implementations.

### 8. No Build System (-3 pts)

No Makefile, no build script, no CI/CD configuration. Given that the project has three separate binaries (plus the cleanup tool), a Makefile would document the build process and ensure version injection.

### 9. Hard-Coded Domain Values in seaweed-cleanup-go (-2 pts)

Bucket names (`rootseek-heic`, `rootseek-ocr`, etc.) and extension-to-bucket mappings are hard-coded. While acceptable for a single-use migration tool, a config file would make this more maintainable if bucket mappings change.

---

## Minor Issues

### 10. Backoff Strategy Isn't Truly Exponential (-1 pt)

In `commitBatch()` (main.go:261,281), the backoff calculates as:
```go
backoff := time.Duration(retry+1) * time.Duration(retryBaseDelayMs) * time.Millisecond
```
This is **linear** (500ms, 1000ms, 1500ms), not exponential. True exponential would be `baseDelay * 2^retry`. The comment says "exponential backoff" but the code is linear. The linear approach is actually fine for this use case, but the comment is misleading.

### 11. audit.go: Atomic Operations May Be Unnecessary (-1 pt)

In audit.go's result collector goroutine (lines 886-938), `atomic.AddInt64` and `atomic.LoadInt64` are used for counters, but the collector is a single goroutine. The atomics are only needed because the progress reporter reads them from a different goroutine. This works correctly but could be cleaner with the reporter reading from a channel or the collector owning a struct with a mutex (similar to how `ProgressTracker` works in main.go).

### 12. count_keys.go: Transaction Not Properly Scoped (-1 pt)

In `countAllKeys()` (line 147), `txn.Rollback()` is called after the loop but should be deferred immediately after `client.Begin()` to handle early returns from errors properly.

---

## Strengths (What's Working Well)

- **Security**: SQL injection prevention via regex whitelist, TLS support, input validation
- **Resilience**: Retry with backoff, graceful shutdown (SIGINT/SIGTERM), resume from checkpoint
- **Correctness**: ProgressTracker ensures safe resume points only advance past contiguous completed batches
- **Observability**: Regular progress reporting with rates, elapsed time, and dirhash position
- **Documentation**: Comprehensive README with examples, performance benchmarks, and edge case documentation
- **Concurrency**: Clean producer-consumer pattern with proper channel lifecycle management
- **Safety**: seaweed-cleanup-go runs pre-flight safety checks before destructive operations
- **Protobuf Analysis**: audit.go's field-level diff analysis is well-designed and thorough

---

## Scoring Breakdown

| Category | Max | Score | Notes |
|---|---|---|---|
| Code Duplication | 20 | 5 | ~81 lines duplicated between main.go and audit.go |
| Testing | 15 | 3 | Zero tests for any component |
| Project Structure | 10 | 2 | Non-standard layout, requires per-file builds |
| Version Management | 5 | 0 | Hard-coded, inconsistent across files |
| Error Handling | 15 | 14 | Thorough, with retry logic and graceful shutdown |
| Security | 10 | 9 | SQL injection prevention, TLS, input validation |
| Concurrency | 10 | 9 | Clean patterns, proper sync, context cancellation |
| Code Clarity | 10 | 8 | Well-commented, clear naming, good structure within files |
| Build/CI | 5 | 2 | No Makefile or CI; builds are manual and non-obvious |
| Operational Quality | 10 | 9 | Resume, progress, graceful shutdown, safety checks |

**Total: 58/100** — deducted primarily for systemic duplication, zero test coverage, and structural issues. The core logic and operational qualities are strong.

---

## Recommended Refactoring Priority

1. **HIGH**: Extract shared code (config, key generation, entry types) into an internal package
2. **HIGH**: Add unit tests for key generation, path prefix, progress tracker, and protobuf parsing
3. **MEDIUM**: Adopt `cmd/` project layout for multi-binary builds
4. **MEDIUM**: Inject version via build flags instead of hard-coded constants
5. **LOW**: Add a Makefile with build targets for each binary
6. **LOW**: Extract retry helper for seaweed-cleanup-go
