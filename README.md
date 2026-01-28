# seaweed-pg2tikv

Fast, parallel migration tool for SeaweedFS filer metadata from PostgreSQL to TiKV.

## Overview

When migrating SeaweedFS from PostgreSQL to TiKV, the built-in `weed filer.meta.backup` command can be extremely slow for large datasets (billions of rows). It uses a single-threaded BFS traversal with only 5 workers.

**seaweed-pg2tikv** reads directly from PostgreSQL and writes to TiKV in parallel, achieving 10-100x faster migration speeds.

## Tools Included

| Tool | Purpose |
|------|---------|
| `seaweed-pg2tikv` | Migrate data from Postgres to TiKV |
| `seaweed-pg2tikv-audit` | Verify migration by comparing Postgres to TiKV |
| `seaweed-count-keys` | Count keys in TiKV with a given prefix |

## Performance

| Dataset Size | filer.meta.backup | seaweed-pg2tikv |
|--------------|-------------------|-----------------|
| 1M rows | ~30 minutes | ~2 seconds |
| 100M rows | ~50 hours | ~30 minutes |
| 1B rows | ~3 weeks | ~5 hours |

## Installation

### Pre-built Binaries

Download from your build machine:
```bash
scp seaweed-pg2tikv-linux-amd64 user@server:~/
scp seaweed-pg2tikv-audit-linux-amd64 user@server:~/
scp seaweed-count-keys-linux-amd64 user@server:~/
chmod +x seaweed-pg2tikv-linux-amd64 seaweed-pg2tikv-audit-linux-amd64 seaweed-count-keys-linux-amd64
```

### Build from Source

Requires Go 1.21+:

```bash
git clone <repo>
cd seaweed-pg2tikv

# Install dependencies
go mod tidy

# Build for Linux
GOOS=linux GOARCH=amd64 go build -o seaweed-pg2tikv-linux-amd64 main.go
GOOS=linux GOARCH=amd64 go build -o seaweed-pg2tikv-audit-linux-amd64 audit.go
GOOS=linux GOARCH=amd64 go build -o seaweed-count-keys-linux-amd64 count_keys.go

# Build for current platform
go build -o seaweed-pg2tikv main.go
go build -o seaweed-pg2tikv-audit audit.go
go build -o seaweed-count-keys count_keys.go
```

## Configuration

seaweed-pg2tikv reads standard SeaweedFS filer.toml configuration files.

### PostgreSQL Configuration

```toml
# postgres_filer.toml
[postgres2]
enabled = true
hostname = "192.168.8.201"
port = 5432
username = "seaweedfs"
password = "secret"
database = "filer"
sslmode = "disable"
# Optional SSL settings:
# sslcert = "/path/to/client.crt"
# sslkey = "/path/to/client.key"
# sslrootcert = "/path/to/ca.crt"
```

### TiKV Configuration

```toml
# tikv_filer.toml
[tikv]
enabled = true
pdaddrs = "192.168.8.131:2379,192.168.8.137:2379,192.168.8.142:2379"
keyPrefix = "seaweedfs"
enable_1pc = false
# Optional TLS settings:
# ca_path = "/path/to/ca.pem"
# cert_path = "/path/to/client.pem"
# key_path = "/path/to/client-key.pem"
```

You can use a single file with both sections or separate files.

---

## seaweed-pg2tikv - Migration Tool

### Basic Usage

```bash
./seaweed-pg2tikv-linux-amd64 \
  --pg-config=/etc/seaweedfs/filer.toml \
  --tikv-config=/path/to/tikv_filer.toml \
  --table="filemeta"
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--pg-config` | (required) | Path to PostgreSQL filer.toml |
| `--tikv-config` | (required) | Path to TiKV filer.toml |
| `--table` | `filemeta` | PostgreSQL table name |
| `--workers` | `10` | Parallel TiKV write workers |
| `--batch` | `1000` | Entries per TiKV transaction |
| `--state` | `migrate_state_{table}.json` | Progress state file |
| `--partition-mod` | `1` | Total partitions for parallel instances |
| `--partition-id` | `0` | This instance's partition (0 to mod-1) |
| `--dry-run` | `false` | Parse configs and exit without migrating |
| `--version` | | Show version and exit |

### Examples

**Single table migration:**
```bash
./seaweed-pg2tikv-linux-amd64 \
  --pg-config=/etc/seaweedfs/filer.toml \
  --tikv-config=./backup_filer.toml \
  --table="filemeta" \
  --workers=10 \
  --batch=1000
```

**Bucket-specific table (postgres2 with SupportBucketTable=true):**
```bash
./seaweed-pg2tikv-linux-amd64 \
  --pg-config=/etc/seaweedfs/filer.toml \
  --tikv-config=./backup_filer.toml \
  --table="my-bucket-name"
```

**Parallel migration with 5 instances:**
```bash
# Run on 5 different terminals or machines
./seaweed-pg2tikv-linux-amd64 --table="big-table" --partition-mod=5 --partition-id=0 &
./seaweed-pg2tikv-linux-amd64 --table="big-table" --partition-mod=5 --partition-id=1 &
./seaweed-pg2tikv-linux-amd64 --table="big-table" --partition-mod=5 --partition-id=2 &
./seaweed-pg2tikv-linux-amd64 --table="big-table" --partition-mod=5 --partition-id=3 &
./seaweed-pg2tikv-linux-amd64 --table="big-table" --partition-mod=5 --partition-id=4 &
wait
```

**Parallel migration script:**
```bash
#!/bin/bash
# migrate_parallel.sh

TABLE="rootseek-heic"
PARTITIONS=5
WORKERS=10
BATCH=1000

PG_CONFIG="/etc/seaweedfs/filer.toml"
TIKV_CONFIG="./backup_filer.toml"

for i in $(seq 0 $((PARTITIONS - 1))); do
  echo "Starting partition $i of $PARTITIONS..."
  ./seaweed-pg2tikv-linux-amd64 \
    --pg-config="$PG_CONFIG" \
    --tikv-config="$TIKV_CONFIG" \
    --table="$TABLE" \
    --partition-mod=$PARTITIONS \
    --partition-id=$i \
    --state="state_${TABLE}_${i}.json" \
    --workers=$WORKERS \
    --batch=$BATCH &
done

echo "All $PARTITIONS partitions started. Waiting..."
wait
echo "Migration complete."
```

### Output

```
2026/01/28 12:00:00 seaweed-pg2tikv version 1.0.3
2026/01/28 12:00:00 === Configuration ===
2026/01/28 12:00:00 Postgres: seaweedfs@192.168.8.201:5432/filer (table: rootseek-ocr)
2026/01/28 12:00:00 TiKV PD:  192.168.8.131:2379 (prefix: "seaweedfs", 1PC: false)
2026/01/28 12:00:00 Workers:  10, Batch: 1000, Partition: 0/1
2026/01/28 12:00:00 Connected to Postgres
2026/01/28 12:00:00 Connected to TiKV
2026/01/28 12:00:00 Starting from dirhash=-9223372036854775808 name=""
2026/01/28 12:00:10 Progress: 50000 OK, 0 FAILED (5000/sec) | elapsed: 10s | dirhash: 12345
...
2026/01/28 12:30:00 All 440457397 rows read from Postgres, waiting for workers...
2026/01/28 12:30:05 === Migration Complete ===
2026/01/28 12:30:05 Rows read from Postgres: 440457397
2026/01/28 12:30:05 Successfully written to TiKV: 440457397
2026/01/28 12:30:05 Failed to write: 0
2026/01/28 12:30:05 State saved to: migrate_state_rootseek-ocr.json
2026/01/28 12:30:05 seaweed-pg2tikv version 1.0.3
```

### Resume After Interruption

seaweed-pg2tikv saves progress to a state file. If interrupted, simply run the same command again - it will resume from where it left off.

```bash
# First run (interrupted)
./seaweed-pg2tikv-linux-amd64 --table="big-table" ...
# ^C

# Resume
./seaweed-pg2tikv-linux-amd64 --table="big-table" ...
# Continues from last position
```

To start fresh, delete the state file:
```bash
rm migrate_state_big-table.json
./seaweed-pg2tikv-linux-amd64 --table="big-table" ...
```

---

## seaweed-pg2tikv-audit - Verification Tool

Verify that migration completed successfully by comparing Postgres data to TiKV.

### Basic Usage

```bash
./seaweed-pg2tikv-audit-linux-amd64 \
  --pg-config=/etc/seaweedfs/filer.toml \
  --tikv-config=./backup_filer.toml \
  --table="filemeta"
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--pg-config` | (required) | Path to PostgreSQL filer.toml |
| `--tikv-config` | (required) | Path to TiKV filer.toml |
| `--table` | `filemeta` | PostgreSQL table name |
| `--mode` | `sample` | `sample` (random) or `complete` (full scan) |
| `--sample-size` | `10000` | Rows to check in sample mode |
| `--workers` | `10` | Parallel verification workers |
| `--show-missing` | `false` | Print details of missing/mismatched entries |
| `--version` | | Show version and exit |

### Examples

**Quick random sample (recommended first):**
```bash
./seaweed-pg2tikv-audit-linux-amd64 \
  --pg-config=/etc/seaweedfs/filer.toml \
  --tikv-config=./backup_filer.toml \
  --table="rootseek-ocr" \
  --mode=sample \
  --sample-size=10000
```

**Full verification (thorough but slow):**
```bash
./seaweed-pg2tikv-audit-linux-amd64 \
  --pg-config=/etc/seaweedfs/filer.toml \
  --tikv-config=./backup_filer.toml \
  --table="rootseek-ocr" \
  --mode=complete
```

**Show missing entries:**
```bash
./seaweed-pg2tikv-audit-linux-amd64 \
  --pg-config=/etc/seaweedfs/filer.toml \
  --tikv-config=./backup_filer.toml \
  --table="rootseek-ocr" \
  --mode=sample \
  --show-missing
```

### Output

```
2026/01/28 13:00:00 seaweed-pg2tikv-audit version 1.0.1
2026/01/28 13:00:00 === Configuration ===
2026/01/28 13:00:00 Postgres: seaweedfs@192.168.8.201:5432/filer (table: rootseek-ocr)
2026/01/28 13:00:00 TiKV PD:  192.168.8.131:2379 (prefix: "seaweedfs")
2026/01/28 13:00:00 Mode: sample, Workers: 10
2026/01/28 13:00:00 Sample size: 10000
2026/01/28 13:00:00 Connected to Postgres
2026/01/28 13:00:00 Total rows in Postgres: 440457397
2026/01/28 13:00:00 Connected to TiKV
2026/01/28 13:00:00 Running random sample of 10000 rows...
2026/01/28 13:00:05 Progress: 10000 checked, 10000 found, 10000 matched, 0 missing, 0 mismatched (2000/sec)

=== Audit Complete ===
Mode: sample
Postgres rows: 440457397
Rows checked: 10000
Found in TiKV: 10000 (100.00%)
Matched exactly: 10000 (100.00%)
Missing from TiKV: 0 (0.00%)
Mismatched data: 0 (0.00%)
Time: 5s

AUDIT PASSED - All checked rows found and matched
```

### Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All checked rows found and matched |
| `1` | Missing or mismatched data detected |

---

## seaweed-count-keys - TiKV Key Counter

Count keys in TiKV with a given prefix to verify total migrated entries.

### Usage

```bash
./seaweed-count-keys-linux-amd64 \
  --pd="192.168.8.131:2379,192.168.8.137:2379,192.168.8.142:2379" \
  --prefix="seaweedfs"
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--pd` | `localhost:2379` | PD addresses (comma-separated) |
| `--prefix` | (required) | Key prefix to count |
| `--ca` | | CA certificate path (for TLS) |
| `--cert` | | Client certificate path (for TLS) |
| `--key` | | Client key path (for TLS) |

### Output

```
2026/01/28 14:00:00 Connected to TiKV at 192.168.8.131:2379
2026/01/28 14:00:00 Counting keys with prefix: "seaweedfs"
2026/01/28 14:00:05 Counted 100000000 keys (20000000/sec)...
2026/01/28 14:00:10 Counted 200000000 keys (20000000/sec)...
...
2026/01/28 14:05:00 === Count Complete ===
2026/01/28 14:05:00 Prefix: "seaweedfs"
2026/01/28 14:05:00 Total keys: 440457397
2026/01/28 14:05:00 Time: 5m0s
```

---

## Migration Workflow

### Step 1: List Tables to Migrate

```sql
-- Connect to Postgres
psql -h 192.168.8.201 -U seaweedfs -d filer

-- List all tables
SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename;

-- Count rows per table
SELECT 'filemeta' as table_name, COUNT(*) FROM filemeta
UNION ALL
SELECT 'my-bucket', COUNT(*) FROM "my-bucket";
```

### Step 2: Migrate Each Table

```bash
# Main filemeta table
./seaweed-pg2tikv-linux-amd64 --table="filemeta" ...

# Each bucket table
./seaweed-pg2tikv-linux-amd64 --table="bucket-a" ...
./seaweed-pg2tikv-linux-amd64 --table="bucket-b" ...
```

### Step 3: Verify Migration

```bash
# Quick sample check
./seaweed-pg2tikv-audit-linux-amd64 --table="filemeta" --mode=sample

# Count total keys
./seaweed-count-keys-linux-amd64 --prefix="seaweedfs"

# Compare to Postgres total
psql -c "SELECT SUM(c) FROM (SELECT COUNT(*) c FROM filemeta UNION ALL SELECT COUNT(*) FROM \"bucket-a\") t"
```

### Step 4: Cutover

1. Stop writes to filer
2. Run final migration pass (idempotent)
3. Run audit verification
4. Update filer.toml to use TiKV
5. Restart filers

---

## Troubleshooting

### "prewrite encounters lock" Errors

TiKV transaction conflicts. The tool retries automatically (15 attempts). If too many failures:
- Reduce `--workers` (try 5)
- Reduce `--batch` (try 500)
- Reduce parallel partitions

### Migration Only Gets Half the Rows

Older versions (< 1.0.2) started from `dirhash=0`, skipping negative values. Update to v1.0.2+ and delete the state file:
```bash
rm migrate_state_*.json
./seaweed-pg2tikv-linux-amd64 --version  # Should show 1.0.2 or higher
```

### "0 rows read from Postgres"

State file thinks migration is complete. Delete it:
```bash
rm migrate_state_mytable.json
```

### Connection Refused to TiKV

Check PD addresses and ensure TiKV cluster is running:
```bash
tiup cluster display tikv-prod
```

### Slow Performance

- Increase `--workers` (up to 50)
- Increase `--batch` (up to 5000)
- Run multiple partitions in parallel
- Check network latency between Postgres, migration host, and TiKV

---

## Key Format

### PostgreSQL Schema

```sql
CREATE TABLE filemeta (
  dirhash   BIGINT,           -- MD5(directory)[0:8] as int64
  name      VARCHAR(65535),   -- filename
  directory VARCHAR(65535),   -- parent directory path
  meta      bytea,            -- protobuf-encoded metadata
  PRIMARY KEY (dirhash, name)
);
```

### TiKV Key Format

```
[keyPrefix] + SHA1(directory) + filename
```

- `keyPrefix`: From config (e.g., "seaweedfs")
- `SHA1(directory)`: 20-byte hash of parent directory
- `filename`: Raw filename bytes

### Value Format

Both Postgres and TiKV store the same protobuf-encoded metadata blob.

---

## Version History

| Version | Changes |
|---------|---------|
| 1.0.3 | Auto-generate state filename from table name |
| 1.0.2 | Fix: Start from minimum int64 to include negative dirhash values |
| 1.0.1 | Add retry logic, track OK/FAILED separately, show version |
| 1.0.0 | Initial release |

---

## License

MIT License - See SeaweedFS project for details.
