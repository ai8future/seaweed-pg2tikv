# Product Overview: seaweed-pg2tikv

## What This Product Is

seaweed-pg2tikv is a database migration and data integrity toolkit for organizations running SeaweedFS at scale. It solves the specific problem of migrating the SeaweedFS filer metadata layer from PostgreSQL to TiKV -- a transition that SeaweedFS's built-in tooling handles poorly at large data volumes.

The suite also includes post-migration cleanup tooling to resolve data duplication and orphaning problems that arise when a SeaweedFS cluster has been operated with S3-style bucket tables in PostgreSQL.

---

## Why This Product Exists

### The Business Problem

SeaweedFS is a distributed object/file storage system. Its "filer" component maintains a metadata catalog -- a mapping of every file path to its storage location (volume IDs, chunk references, timestamps, permissions, etc.). This metadata can be backed by various databases. PostgreSQL is a common choice for small-to-medium deployments.

At scale (hundreds of millions to billions of files), PostgreSQL becomes a bottleneck for filer metadata. TiKV -- a distributed, horizontally-scalable key-value store -- is better suited for this workload. Organizations reaching this scale need to migrate their filer metadata from PostgreSQL to TiKV without extended downtime or data loss.

### Why Not Use SeaweedFS's Built-In Migration?

SeaweedFS ships a `weed filer.meta.backup` command for metadata migration. This command is fundamentally inadequate for large datasets because:

- It uses single-threaded BFS traversal with only 5 workers.
- It processes the directory tree recursively, which is inherently serialized.
- At 1 billion rows, the built-in tool takes approximately 3 weeks. This product achieves the same migration in approximately 5 hours -- a 100x improvement.

The performance gap makes the built-in tool unusable for production migrations where downtime windows are limited.

---

## What The Product Does

The suite consists of four tools, each serving a distinct role in the migration lifecycle:

### 1. seaweed-pg2tikv (Migration Engine)

The core tool. It reads metadata rows directly from PostgreSQL tables and writes them into TiKV using the correct key format that SeaweedFS's TiKV filer store expects.

**Key business logic:**

- **Direct database-to-database transfer.** Bypasses SeaweedFS entirely. Reads raw rows from PostgreSQL (dirhash, directory, name, meta) and writes them as TiKV key-value pairs using SeaweedFS's native key format: `[prefix] + SHA1(directory) + filename`.

- **Path prefix reconstruction for bucket tables.** SeaweedFS's postgres2 driver supports "SupportBucketTable" mode, where each S3 bucket gets its own PostgreSQL table with paths relative to the bucket root. For example, `/buckets/photos/img.jpg` is stored in the `photos` table as directory=`/`, name=`img.jpg`. TiKV uses absolute paths in a single namespace. The tool's `--path-prefix` flag reconstructs the full path before generating TiKV keys. Without this, bucket entries end up under wrong directory hashes, corrupting the entire directory tree. This was a critical bug discovered and fixed in v1.2.0.

- **Parallel write workers.** Configurable worker pool (default 10) processes batches of entries concurrently into TiKV. Each worker commits a batch as a single TiKV transaction.

- **Horizontal scaling via partitioning.** The workload can be split across multiple independent migration instances using `--partition-mod` and `--partition-id`. Each instance processes a modular subset of rows based on `mod(abs(dirhash), N) = id`. When running a single instance, the mod clause is omitted entirely so PostgreSQL can use its primary key index for efficient pagination.

- **Resumable state management.** Progress is saved to a JSON state file tracking the last successfully committed (dirhash, name) cursor position. If interrupted (Ctrl+C, crash, network failure), re-running the same command resumes from the last checkpoint. The progress tracker uses contiguous batch sequence tracking -- it only advances the saved position when all preceding batches have completed, preventing gaps from out-of-order batch completion.

- **Transaction retry with exponential backoff.** TiKV transaction conflicts ("prewrite encounters lock") are automatically retried up to 15 times with configurable exponential backoff. This handles the contention that occurs when multiple workers or instances write to overlapping key ranges.

- **Graceful shutdown.** Catches SIGINT/SIGTERM, stops accepting new work, drains in-flight batches, saves state, and exits cleanly. A second interrupt force-exits.

- **SQL injection prevention.** Table names are validated against a strict regex before being interpolated into SQL queries, since PostgreSQL parameterized queries do not support dynamic table names.

### 2. seaweed-pg2tikv-audit (Verification Engine)

Post-migration verification tool that compares data between PostgreSQL and TiKV to confirm migration completeness and correctness.

**Key business logic:**

- **Two verification modes.** "Sample" mode selects a random subset of PostgreSQL rows (default 10,000) and checks their presence and correctness in TiKV -- useful as a fast smoke test. "Complete" mode scans every row in the PostgreSQL table -- thorough but proportionally slow.

- **Byte-level metadata comparison.** For each row, the tool generates the same TiKV key the migration tool would have produced, reads the value from TiKV, and does a byte-for-byte comparison of the protobuf metadata blob.

- **Deep protobuf field analysis on mismatches.** When metadata does not match byte-for-byte, the tool decodes the SeaweedFS Entry protobuf structure and identifies exactly which fields differ: attributes (file_size, mtime, crtime, mime, file_mode), chunks (file_id, volume_id, offset, size, modified_ts_ns), extended attributes (map entries like Seaweed-X-Amz-Implicit-Dir), and more. This is critical for diagnosing whether mismatches are benign (e.g., a live filer updating metadata between migration and audit) or indicate corruption.

- **Mismatch pattern aggregation.** At the end of an audit run, the tool summarizes which field combinations differ and how frequently, enabling operators to make informed decisions about whether mismatches are acceptable.

- **Machine-readable exit codes.** Exit 0 means all checked rows passed. Exit 1 means missing or mismatched data was detected. This enables scripted migration workflows.

### 3. seaweed-count-keys (TiKV Key Counter)

Operational utility for counting (and optionally bulk-deleting) keys in TiKV by prefix.

**Key business logic:**

- **Migration validation.** After migration, operators can count all keys with the SeaweedFS prefix and compare against `SELECT COUNT(*)` from PostgreSQL to confirm row-level completeness.

- **Bulk delete for migration recovery.** The `--delete` flag performs a server-side `DeleteRange` operation to wipe all keys with a given prefix. This is used when a migration produced corrupted data (e.g., missing `--path-prefix`) and needs to be re-run from scratch.

### 4. seaweed-cleanup (Post-Migration Data Cleanup)

A separate Go application within the repository that addresses a different but related problem: cleaning up duplicate and orphaned metadata entries in a live SeaweedFS filer after migration or during normal S3-bucket operations.

**Key business logic:**

- **Duplicate detection.** When SeaweedFS stores files in S3 buckets, it may create metadata entries both at the root level (`/dirname/file.ext`) and in the bucket path (`/buckets/bucket-name/dirname/file.ext`). The cleanup tool identifies root-level entries that have identical chunk references (same volume IDs, same data) in a bucket -- confirmed duplicates.

- **Safe metadata-only deletion of duplicates.** When a root-level entry is confirmed as a duplicate, the tool deletes only the metadata pointer at the root path using `skipChunkDeletion=true`. This removes the redundant directory listing entry without touching the underlying data chunks, which remain accessible through the bucket path.

- **Orphan migration via gRPC.** Root-level entries that do NOT exist in any bucket are "orphans." The tool migrates these by looking up the entry's full metadata via SeaweedFS's gRPC API, creating a new entry at the correct bucket path (determined by file extension mapping: .heic -> rootseek-heic, .json.gz -> rootseek-ocr, .jp2 -> rootseek-jp2, .parquet -> rootseek-fast-parquet, .lp_newspaper -> rootseek-newspaper), and then deleting the root metadata. This preserves the underlying data chunks.

- **Smartdata subdirectory handling.** Directories containing date-based subdirectories (YYYY-MM) with smartdata files (s3.sdc.*.json.gz) are detected and migrated to the `/buckets/smartdata` bucket with their subdirectory structure preserved.

- **Pre-flight safety checks.** Before any destructive operation (delete or migrate), the tool automatically finds a test case, performs the operation on a single entry, and verifies that the bucket copy remains readable afterward. The operation only proceeds if this safety check passes.

- **Parallel processing with sharding.** Multiple instances can run concurrently using either `--random` (shuffled order to reduce overlap) or `--shard N/M` (deterministic FNV-hash-based partitioning for zero overlap). Both approaches enable scaling the cleanup across multiple machines.

- **Resumable cursor.** In sequential mode, the tool outputs a resume cursor that can be passed via `--start-after` to pick up where a previous run stopped.

---

## The Migration Workflow (Business Process)

The tools support a structured migration process:

1. **Inventory.** Operators query PostgreSQL to list all filer tables and their row counts -- the main `filemeta` table plus any per-bucket tables.

2. **Migration.** Each table is migrated independently. The root `filemeta` table is migrated without a path prefix. Each bucket-specific table is migrated with `--path-prefix=/buckets/<bucket-name>`. Large tables can be parallelized across multiple instances using partitioning.

3. **Verification.** A sample audit confirms high-level correctness. A key count confirms row-level completeness. Optionally, a complete audit confirms byte-level correctness for every row.

4. **Cutover.** Writes are stopped, a final migration pass catches any rows written since the initial migration, a final audit confirms correctness, the filer configuration is switched from PostgreSQL to TiKV, and filers are restarted.

5. **Post-migration cleanup.** The cleanup tool scans for and resolves duplicate/orphan metadata entries that may exist in the filer namespace.

---

## Scale and Performance Characteristics

The product is designed for datasets in the hundreds of millions to billions of rows:

| Dataset Size | Built-in SeaweedFS Tool | seaweed-pg2tikv |
|--------------|------------------------|-----------------|
| 1M rows      | ~30 minutes            | ~2 seconds      |
| 100M rows    | ~50 hours              | ~30 minutes     |
| 1B rows      | ~3 weeks               | ~5 hours        |

Real-world usage shown in the codebase references tables with 440 million rows being migrated and verified.

---

## Configuration and Integration

The tools are designed to integrate directly into existing SeaweedFS deployments:

- **Configuration reuse.** Both the migration and audit tools read SeaweedFS's native `filer.toml` configuration format, so operators can point them at the same config files their filers already use. No separate configuration layer is needed.

- **TLS support.** All tools support TLS connections to both PostgreSQL (via sslmode/sslcert/sslkey/sslrootcert) and TiKV (via ca_path/cert_path/key_path), matching the security configurations used in production TiKV clusters.

- **Pre-built Linux binaries.** The repository includes pre-compiled linux-amd64 binaries for deployment to production servers without requiring a Go build environment.

---

## Key Data Format Details

The product must faithfully translate between two different key schemes:

- **PostgreSQL:** Rows are keyed by `(dirhash, name)` where dirhash is `MD5(directory)[0:8]` cast to int64. The directory path is stored as a separate column. Metadata is a protobuf blob.

- **TiKV:** Keys are `[keyPrefix] + SHA1(directory) + filename`. Values are the same protobuf blob, copied verbatim.

The migration tool reads the directory and filename from PostgreSQL, computes the SHA1-based TiKV key, and writes the protobuf metadata unchanged. The SHA1 hash function is mandated by SeaweedFS's TiKV filer implementation -- it is not a security function but a consistent key derivation scheme.

---

## Risks and Edge Cases Addressed

The codebase reflects several production-discovered issues:

- **Negative dirhash values.** Early versions started scanning from dirhash=0, missing approximately half of all rows (those with negative MD5-derived hash values). Fixed in v1.0.2 to start from minimum int64.

- **Path prefix corruption.** The most critical bug: migrating per-bucket tables without path prefix reconstruction silently produces a working but corrupted filer where files appear under wrong directories. The fix required adding the `--path-prefix` flag and documenting it prominently.

- **Transaction conflicts under parallel load.** TiKV's optimistic concurrency control can cause "prewrite encounters lock" errors when multiple workers touch overlapping key ranges. The automatic retry with exponential backoff handles this transparently.

- **State file from wrong table.** The tool detects when a state file was created for a different table and resets rather than silently applying the wrong cursor.

- **Incomplete progress tracking.** The contiguous batch sequence tracker ensures that the saved resume point never skips past failed batches, which would leave gaps in the migrated data.
