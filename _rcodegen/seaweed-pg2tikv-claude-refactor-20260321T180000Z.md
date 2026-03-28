Date Created: 2026-03-21T18:00:00Z
TOTAL_SCORE: 56/100

# Refactoring Report: seaweed-pg2tikv

**Agent**: Claude Code (Claude:Opus 4.6)
**Scope**: Code quality, duplication, and maintainability review
**Files Reviewed**: main.go (812 LOC), audit.go (1050 LOC), count_keys.go (189 LOC), seaweed-cleanup-go/main.go (1247 LOC)
**Total LOC**: ~3,298 (excluding generated protobuf)

---

## Executive Summary

The codebase is a functional suite of SeaweedFS migration and maintenance utilities. The code demonstrates solid Go concurrency patterns, good error handling, and thoughtful operational features (graceful shutdown, progress tracking, state-based resume). However, it suffers from **severe code duplication** between main.go and audit.go, an **unusual build structure** (multiple main() functions in one package directory), **no tests**, and **monolithic function design**. These issues create real maintenance risk: bugs fixed in one file can easily be missed in the duplicated copy.

---

## Category Scores

### 1. Structure & Organization: 12/25

**Multiple `main()` definitions in the same package directory**
- `main.go`, `audit.go`, and `count_keys.go` are all `package main` with their own `func main()`. They live in the same directory but cannot be compiled together — they must be built with explicit file selection (e.g., `go build -o audit audit.go`). This is an unusual and fragile pattern that confuses tooling (gopls, IDE navigation) and newcomers.
- **Recommendation**: Each binary should live in its own `cmd/` subdirectory (e.g., `cmd/pg2tikv/main.go`, `cmd/audit/main.go`, `cmd/count-keys/main.go`) with shared code in an `internal/` package.

**seaweed-cleanup-go is a separate module**
- Properly isolated with its own `go.mod`, which is good. However, it's a 1,247-line single file with no decomposition into packages or even separate files.
- **Recommendation**: Break into at least `filer.go` (HTTP/gRPC client), `verify.go` (duplicate verification), `migrate.go` (migration logic), and `main.go` (CLI orchestration).

**Binary artifacts in version control**
- Three large compiled binaries (seaweed-count-keys-linux-amd64, seaweed-pg2tikv-audit-linux-amd64, seaweed-pg2tikv-linux-amd64 totaling ~104MB) are tracked in the repo. These should be in `.gitignore` and distributed via releases or a CI artifact store.

### 2. Code Duplication: 10/25

This is the most significant issue in the codebase.

**main.go ↔ audit.go: ~150+ lines of verbatim duplication**

| Duplicated Element | Lines (approx) |
|---|---|
| `FilerConfig` struct | 4 |
| `Postgres2Config` struct + `ConnectionString()` | 30 |
| `TiKVConfig` struct + `PDAddrList()` | 18 |
| `hashToBytes()` | 5 |
| `generateTiKVKey()` | 11 |
| `applyPathPrefix()` | 8 |
| `Entry` struct | 6 |
| `validTableName` regex | 1 |
| `MinInt64` constant | 1 |
| TLS path validation block | 15 |
| TiKV TLS config setup | 8 |
| Postgres connection setup | 8 |
| Config parsing blocks | 12 |
| Config printing blocks | 10 |
| Path prefix validation | 5 |
| **Total** | **~142 lines** |

This is dangerous duplication. The `Version` constants already demonstrate the drift risk: main.go has `Version = "1.2.0"` while audit.go has `Version = "1.2.2"`, showing they've been independently maintained.

**count_keys.go also duplicates**:
- TLS validation pattern (3x os.Stat checks) — same as main.go and audit.go
- PD address splitting logic (functionally identical to `PDAddrList()`)
- Its own `Version = "1.2.0"` constant

**seaweed-cleanup-go retry pattern duplication**:
The 3-attempt retry-with-sleep pattern appears at least 6 times:
- `verifyDuplicate()` line 218-228
- `migrateOrphan()` lines 308-318
- `migrateOrphan()` lines 357-368 (gRPC lookup)
- `migrateOrphan()` lines 376-388 (gRPC create)
- `trySmartdataMigration()` lines 488-497
- `trySmartdataMigration()` lines 514-525 and 534-545

A simple `withRetry(ctx, maxAttempts, fn)` helper would eliminate all of this.

### 3. Maintainability: 15/25

**No tests**
- Zero test files across the entire codebase. While integration-heavy database migration tools are harder to unit test, the pure functions are highly testable:
  - `hashToBytes()`, `generateTiKVKey()`, `applyPathPrefix()` — deterministic key generation
  - `prefixEndKey()` — prefix boundary calculation
  - `parseProtoFields()`, `fieldValuesEqual()`, `diffFields()` — protobuf comparison logic
  - `getFileExt()`, `isSmartdataFile()`, `chunksEqual()`, `shardHash()` — simple utility functions
  - `loadState()` / `saveState()` — state serialization
  - `ProgressTracker` — contiguous sequence tracking

**Monolithic main() functions**
- `main.go:main()` is ~400 lines with config parsing, validation, connection setup, goroutine orchestration, signal handling, and reporting all inlined.
- `audit.go:main()` is ~300 lines with the same pattern.
- `seaweed-cleanup-go/main.go:main()` is ~400 lines.
- These are difficult to navigate, modify, or test in isolation.

**Version management fragility**
- Version is defined as a Go constant in 3 separate files, plus a `VERSION` file, plus inline in file header comments. Five places to update for a version bump. The values already disagree (1.2.0, 1.2.0, 1.2.2).
- **Recommendation**: Use `-ldflags` to inject version from the `VERSION` file at build time, or have all binaries read from one shared constant.

**Good: Clear operational design**
- State file resume, graceful shutdown (double Ctrl-C), progress reporting, and dry-run modes are all well-implemented and show operational maturity.
- SQL injection prevention via regex validation before string interpolation.
- SHA1 usage is properly documented as non-cryptographic.

### 4. Code Quality: 19/25

**Strengths**:
- Clean, idiomatic Go — channels, goroutines, sync.WaitGroup, context cancellation used correctly
- `ProgressTracker` with contiguous sequence tracking is well-designed for safe resume points
- `sleepCtx()` properly respects context cancellation during backoff
- Protobuf diff analysis in audit.go is thorough with recursive field comparison, timestamp formatting, and chunk-level diffing
- Proper resource cleanup with defer patterns

**Minor Issues**:
- `_ = *dryRun // used implicitly` (seaweed-cleanup-go/main.go:882) — this variable is set but never read. The comment claims it's "used implicitly" but it isn't.
- `_ = d` and `_ = lastRateCount` (seaweed-cleanup-go/main.go:1058,1062) — dead assignments suppressed with blank identifier
- `commitBatch()` has a redundant final `return fmt.Errorf("max retries exceeded")` that can never be reached (the loop already handles all exit paths)
- audit.go `auditWorker()` applies `applyPathPrefix()` at line 709, but the caller in main already sends entries with raw directories — this is correct but the symmetry with main.go's pattern where prefix is applied during row scanning (line 718) means the two files apply the prefix at different pipeline stages, which could cause confusion.

**Hardcoded values in seaweed-cleanup-go**:
- `candidateBuckets` and `extToBucket` are hardcoded. These should be CLI flags or configuration, especially since this is a tool meant for operational use.
- `sampleFiles = 0` is a constant that was clearly once configurable (the name implies it). Making it a constant with a comment is fine, but it suggests the design changed and the old approach wasn't fully cleaned up.

---

## Top 5 Refactoring Recommendations (Priority Order)

1. **Extract shared types/functions into an `internal/common` package**: Eliminate the ~150 lines of duplication between main.go and audit.go. This is the highest-value change — it prevents future bug divergence and cuts ~10% of total LOC.

2. **Move each binary to `cmd/` subdirectory**: `cmd/pg2tikv/`, `cmd/audit/`, `cmd/count-keys/`. Fixes the multiple-main() problem and makes the build system standard (`go build ./cmd/...`).

3. **Add unit tests for pure functions**: Start with key generation (`generateTiKVKey`, `applyPathPrefix`), protobuf diffing, and `ProgressTracker`. These are the highest-risk areas where a regression would cause data corruption or missed mismatches.

4. **Extract a retry helper in seaweed-cleanup-go**: A single `retry(ctx context.Context, maxAttempts int, fn func() error) error` would replace 6+ duplicated retry loops.

5. **Unify version management**: Use build-time `-ldflags` injection from the `VERSION` file, or at minimum a single source constant imported by all binaries.

---

## Summary Table

| Category | Score | Key Issue |
|---|---|---|
| Structure & Organization | 12/25 | Multiple main() in one dir, no cmd/ layout |
| Code Duplication | 10/25 | ~150 lines copied between main.go and audit.go |
| Maintainability | 15/25 | No tests, monolithic functions, version drift |
| Code Quality | 19/25 | Clean Go idioms, good concurrency, minor dead code |
| **Total** | **56/100** | |

The codebase works well as a set of operational utilities and demonstrates strong Go concurrency skills. The main drag on the score is structural: the duplication between main.go and audit.go is the kind of tech debt that causes real incidents (a fix applied to one file but not the other). Addressing recommendations #1 and #2 alone would likely bring the score into the 75+ range.
