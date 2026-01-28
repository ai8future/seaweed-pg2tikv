# Changelog

All notable changes to this project will be documented in this file.

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
