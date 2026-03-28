# Changelog

All notable changes to this project will be documented in this file.

## [1.2.7] - 2026-03-27

### Tests
- Added unit tests for all three binaries (39 test cases total): main.go (16 tests), audit.go (19 tests), count_keys.go (3 tests + property test)
- Covers critical paths: TiKV key generation, path prefix handling (prior production bug site), SQL injection prevention, state save/load with old format migration, ProgressTracker (sequential, out-of-order, failure, concurrent), protobuf parsing, audit field diffing, key range prefix end calculation
- Run per-binary: `go test -v main.go main_test.go`, `go test -v audit.go audit_test.go`, `go test -v count_keys.go count_keys_test.go`

Agent: Claude Code (Claude:Opus 4.6)

## [1.2.6] - 2026-03-07

### Features
- Added `--shard N/M` flag to seaweed-cleanup: deterministically partitions directories by FNV hash so multiple instances process non-overlapping sets (e.g., `--shard 0/3`, `--shard 1/3`, `--shard 2/3`). Works with both sequential and `--random` modes.

Agent: Claude Code (Claude:Opus 4.6)

## [1.2.5] - 2026-03-07

### Features
- Added `--random` flag to seaweed-cleanup: shuffles directory processing order so multiple instances can run in parallel without duplicating work. Pre-collects all root directory names, shuffles them, then processes. Resume cursor is suppressed in random mode since it's not meaningful.

Agent: Claude Code (Claude:Opus 4.6)

## [1.2.4] - 2026-03-07

### Features
- Added `--version` flag and startup banner showing version number for easy binary identification

Agent: Claude Code (Claude:Opus 4.6)

## [1.2.3] - 2026-03-07

### Features
- **Orphan migration via gRPC:** Added `--migrate` flag to move root-level orphan entries (files with no bucket copy) into the correct S3 buckets. Uses SeaweedFS filer gRPC `CreateEntry` API to create metadata at the bucket path with the same chunks, then removes root metadata with `skipChunkDeletion=true`. Extension-to-bucket mapping: `.heic`→rootseek-heic, `.json.gz`→rootseek-ocr, `.lp_newspaper`→rootseek-newspaper, `.jp2`→rootseek-jp2, `.parquet`→rootseek-fast-parquet.
- **Migration safety check:** Pre-flight test migrates one orphan file, verifies it's readable at the bucket path, deletes the root copy, and confirms the bucket copy survives.
- **Combined modes:** `--delete` and `--migrate` can be used together to handle both duplicates and orphans in a single pass.
- Added compiled SeaweedFS filer gRPC proto stubs (`filer_pb2.py`, `filer_pb2_grpc.py`) and source proto.
- Auto-derives filer gRPC address from HTTP URL (port+10000), overridable with `--filer-grpc`.

Agent: Claude Code (Claude:Opus 4.6)

## [1.2.2] - 2026-03-06

### Features
- **Deep protobuf mismatch analysis:** Audit now recursively decodes chunks (FileChunk sub-fields: file_id, fid, offset, size, modified_ts_ns) and extended map entries (key-value pairs like Seaweed-X-Amz-Implicit-Dir) to show exactly what differs
- Chunks show sub-field diffs like `chunks[0].fid.volume_id`, `chunks[0].modified_ts_ns`
- Extended entries show readable keys: `extended["Seaweed-X-Amz-Implicit-Dir"] (added in tikv)`
- FileId sub-messages (volume_id, file_key, cookie) decoded within chunk fid/source_fid fields
- Nanosecond timestamps (modified_ts_ns) formatted as ISO 8601

Agent: Claude Code (Claude:Opus 4.6)

## [1.2.1] - 2026-03-06

### Features
- **Audit mismatch analysis:** Audit now decodes protobuf fields to identify exactly which SeaweedFS Entry fields differ between Postgres and TiKV (e.g., attributes.mtime, attributes.crtime, chunks, file_size)
- Mismatch summary always printed at end showing differing fields and their frequency
- Mismatch patterns show which combinations of fields differ together
- `--show-missing` now shows field names that differ per entry (e.g., `MISMATCH #1: /path/file.jpg [attributes.mtime, attributes.crtime]`)
- `--verbose` now shows actual field values with timestamps formatted as ISO 8601

Agent: Claude Code (Claude:Opus 4.6)

## [1.2.0] - 2026-02-19

### Bug Fixes
- **CRITICAL:** Added --path-prefix flag for per-bucket table migration. SeaweedFS postgres2 stores paths relative to the bucket root in per-bucket tables, but TiKV needs absolute paths. Without --path-prefix, bucket table entries were written with wrong keys, causing corrupted directory listings (root directories disappear, subdirectories appear at wrong level).

### Features
- Added --delete flag to seaweed-count-keys for wiping all keys with a given prefix (for migration recovery)

### Usage
- For filemeta (root table): no --path-prefix needed
- For bucket tables: use --path-prefix=/buckets/<bucket-name>
- Flag added to both seaweed-pg2tikv and seaweed-pg2tikv-audit

Agent: Claude Code (Claude:Opus 4.6)

## [1.1.5] - 2026-02-15

### Performance
- Skip mod(abs(dirhash), N) clause in Postgres query when partition-mod=1 (single instance), allowing Postgres to use index on (dirhash, name) instead of full table scan + sort per page

Agent: Claude Code (Claude:Opus 4.6)

## [1.0.5] - 2026-01-28

### Bug Fixes
- Fixed potential division by zero in audit.go when no rows are checked

## [1.0.4] - 2026-01-28

### Security
- Added SQL injection prevention: table names now validated against safe pattern
- Added TLS certificate path validation before use

### Bug Fixes
- Fixed race condition in progress tracking: now uses contiguous batch completion
- Fixed ignored errors in state file read/write operations (now fail loudly)
- Fixed Set errors being ignored in batch commits (now fail batch properly)
- Fixed commit using background context (now respects cancellation)
- Fixed version inconsistency between main.go and audit.go (unified at 1.0.4)
- Added empty batch safety check to prevent potential panic

### Improvements
- Added input validation for partition-mod and partition-id flags
- Made retry parameters configurable (--max-retries, --retry-base-ms)
- Improved key prefix checking in count_keys.go using bytes.HasPrefix
- Added version flag and output to count_keys.go
- Added SHA1 usage documentation (required for SeaweedFS compatibility)
- Progress tracking now only advances on contiguous successful batches

## [1.0.3] - 2026-01-28

- Initial changelog created
- Project structure standardized with AGENTS.md and standard directories

## [1.0.2] - 2026-01-28

- Previous development work

## [1.0.1] - 2026-01-28

- Previous development work

## [1.0.0] - 2026-01-28

- Initial release of seaweed-pg2tikv
- PostgreSQL to TiKV migration tool for SeaweedFS
