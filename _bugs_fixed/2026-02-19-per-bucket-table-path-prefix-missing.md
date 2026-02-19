# Per-Bucket Table Path Prefix Missing

**Severity:** Critical
**Fixed in:** v1.2.0

## Problem

Migrating per-bucket PostgreSQL tables to TiKV produced corrupted directory structure. Root directories disappeared and subdirectories from bucket tables appeared at the root level in the SeaweedFS file browser.

## Root Cause

SeaweedFS's postgres2 filer store uses `FilerStorePathTranslator` to strip the bucket prefix before storing entries in per-bucket tables. For example, a file at `/buckets/photos/subdir/image.jpg` is stored in the `photos` table as `directory=/subdir, name=image.jpg`.

When reading back, postgres2 adds the prefix back. But TiKV uses a single key-value namespace with keys derived from the full absolute path: `SHA1("/buckets/photos/subdir") + "image.jpg"`.

The migration tool was reading the relative path from the bucket table and using it directly for TiKV key generation, producing `SHA1("/subdir") + "image.jpg"` instead of the correct `SHA1("/buckets/photos/subdir") + "image.jpg"`. This placed all bucket entries under the wrong directory hashes in TiKV.

## Fix

Added `--path-prefix` flag to both `seaweed-pg2tikv` and `seaweed-pg2tikv-audit`. When migrating per-bucket tables, users must specify `--path-prefix=/buckets/<bucket-name>` to reconstruct the full absolute path before generating TiKV keys.
