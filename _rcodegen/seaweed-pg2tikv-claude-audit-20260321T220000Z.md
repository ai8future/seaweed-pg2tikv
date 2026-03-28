Date Created: 2026-03-21T22:00:00Z
TOTAL_SCORE: 64/100

# Code Audit: seaweed-pg2tikv

**Auditor:** Claude Code (Claude:Opus 4.6)
**Scope:** Full codebase audit including security, code quality, architecture, and reliability
**Files audited:** main.go, audit.go, count_keys.go, seaweed-cleanup-go/main.go, go.mod (x2)

---

## Executive Summary

seaweed-pg2tikv is an internal migration toolset for moving SeaweedFS filer metadata from PostgreSQL to TiKV. It consists of three executables in the main module (migration, audit, key counter) and a separate cleanup tool. The code demonstrates strong operational safety (pre-flight checks, graceful shutdown, retry logic) but suffers from severe structural duplication, no tests, an unclear build process, and version drift across files.

---

## Score Breakdown

| Category                     | Score  | Max  | Notes |
|------------------------------|--------|------|-------|
| Code Quality & Style         | 15     | 30   | Massive duplication, no tests, but clean within files |
| Security                     | 20     | 25   | Good SQL injection prevention, TLS validation |
| Reliability & Error Handling | 20     | 25   | Excellent retry logic, minor goroutine leak |
| Architecture & Maintainability | 9    | 20   | No shared packages, unclear build process |
| **TOTAL**                    | **64** | **100** | |

---

## CRITICAL: Build Breakage Risk

All three files (`main.go`, `audit.go`, `count_keys.go`) declare `package main` with their own `func main()`, `const Version`, `const MinInt64`, and duplicate type definitions (`FilerConfig`, `Postgres2Config`, etc.). Running `go build .` or `go build ./...` will fail with compilation errors. These files must be built individually:

```
go build -o seaweed-pg2tikv main.go
go build -o seaweed-pg2tikv-audit audit.go
go build -o seaweed-count-keys count_keys.go
```

This is fragile and undocumented. The standard Go approach is `cmd/` subdirectories with shared code in `internal/` or `pkg/`.

---

## CRITICAL: Version Constant Drift

| File | Version Constant | VERSION file |
|------|-----------------|--------------|
| main.go:28 | `1.2.0` | `1.2.6` |
| audit.go:29 | `1.2.2` | `1.2.6` |
| count_keys.go:20 | `1.2.0` | `1.2.6` |
| seaweed-cleanup-go/main.go:37 | `1.3.0` | N/A |

The `--version` flag reports stale versions. Users cannot determine which binary they are running.

### Patch: Sync version constants

```diff
--- a/main.go
+++ b/main.go
@@ -25,7 +25,7 @@ import (
 	"github.com/tikv/client-go/v2/txnkv"
 )

-const Version = "1.2.0"
+const Version = "1.2.6"

 const MinInt64 = -9223372036854775808
```

```diff
--- a/audit.go
+++ b/audit.go
@@ -26,7 +26,7 @@ import (
 	"google.golang.org/protobuf/encoding/protowire"
 )

-const Version = "1.2.2"
+const Version = "1.2.6"
 const MinInt64 = -9223372036854775808
```

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -17,7 +17,7 @@ import (
 	"github.com/tikv/client-go/v2/txnkv"
 )

-const Version = "1.2.0"
+const Version = "1.2.6"
```

---

## Security Findings

### SEC-1: SQL Injection Prevention (GOOD)

Table names are validated via regex `^[a-zA-Z_][a-zA-Z0-9_\-]*$` and inserted with Go's `%q` (double-quoted identifier). Both main.go and audit.go consistently apply this. **No issue.**

### SEC-2: Connection String Value Injection (LOW)

`ConnectionString()` concatenates config values directly into the libpq connection string. If a TOML config value contains spaces or special characters (e.g., a password with spaces), the connection string parsing may break or misinterpret values. libpq key=value format requires single-quoting for values with spaces.

```diff
--- a/main.go
+++ b/main.go
@@ -56,8 +56,8 @@ type Postgres2Config struct {
 func (c *Postgres2Config) ConnectionString() string {
 	connStr := fmt.Sprintf(
-		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
-		c.Hostname, c.Port, c.Username, c.Password, c.Database, c.SSLMode,
+		"host='%s' port=%d user='%s' password='%s' dbname='%s' sslmode='%s'",
+		c.Hostname, c.Port, c.Username, c.Password, c.Database, c.SSLMode,
 	)
```

Same fix needed in audit.go (identical function).

### SEC-3: gRPC Without TLS in Cleanup Tool (LOW)

`seaweed-cleanup-go/main.go:936` uses `insecure.NewCredentials()`. Acceptable for internal tooling on a private network, but should be noted for environments where the filer gRPC port is exposed.

### SEC-4: No Confirmation on Destructive Delete (MEDIUM)

`count_keys.go:93-94` executes `deleteAllKeys()` via server-side `DeleteRange` without any user confirmation prompt. A typo in `--prefix` could wipe unintended key ranges.

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -91,6 +91,13 @@ func main() {

 	if *deleteKeys {
+		fmt.Printf("WARNING: About to delete ALL keys with prefix %q\n", *prefix)
+		fmt.Printf("Type 'yes' to confirm: ")
+		var confirm string
+		fmt.Scanln(&confirm)
+		if confirm != "yes" {
+			log.Fatalf("Aborted.")
+		}
 		deleteAllKeys(addrs)
 	} else {
 		countAllKeys(addrs)
```

### SEC-5: State File Permissions (INFO)

`main.go:152` writes state file with mode `0644` (world-readable). The state file contains only dirhash/name/counts, no credentials. **Not a risk**, but tightening to `0600` is trivial.

---

## Code Quality Findings

### CQ-1: Massive Code Duplication Between main.go and audit.go (HIGH)

The following are fully duplicated between both files:

| Duplicated Element | Lines (~) |
|---|---|
| `FilerConfig` struct | 5 |
| `Postgres2Config` struct + `ConnectionString()` | 22 |
| `TiKVConfig` struct + `PDAddrList()` | 16 |
| `hashToBytes()` | 4 |
| `generateTiKVKey()` | 10 |
| `applyPathPrefix()` | 8 |
| `Entry` struct | 6 |
| `validTableName` regex | 1 |
| `MinInt64` const | 1 |
| TLS path validation block | 12 |
| Config parsing/validation | ~30 |
| **Total duplicated** | **~115 lines** |

### Recommended fix: Extract to shared package

Create `internal/common/` with shared types and functions. This eliminates duplication and ensures changes propagate to all tools.

```diff
--- /dev/null
+++ b/internal/common/config.go
@@ -0,0 +1,85 @@
+package common
+
+import (
+	"crypto/sha1"
+	"fmt"
+	"io"
+	"regexp"
+	"strings"
+)
+
+const MinInt64 = -9223372036854775808
+
+var ValidTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_\-]*$`)
+
+type FilerConfig struct {
+	Postgres2 Postgres2Config `toml:"postgres2"`
+	TiKV      TiKVConfig     `toml:"tikv"`
+}
+
+type Postgres2Config struct {
+	Enabled     bool   `toml:"enabled"`
+	Hostname    string `toml:"hostname"`
+	Port        int    `toml:"port"`
+	Username    string `toml:"username"`
+	Password    string `toml:"password"`
+	Database    string `toml:"database"`
+	Schema      string `toml:"schema"`
+	SSLMode     string `toml:"sslmode"`
+	SSLCert     string `toml:"sslcert"`
+	SSLKey      string `toml:"sslkey"`
+	SSLRootCert string `toml:"sslrootcert"`
+}
+
+func (c *Postgres2Config) ConnectionString() string {
+	connStr := fmt.Sprintf(
+		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
+		c.Hostname, c.Port, c.Username, c.Password, c.Database, c.SSLMode,
+	)
+	if c.SSLCert != "" { connStr += fmt.Sprintf(" sslcert=%s", c.SSLCert) }
+	if c.SSLKey != "" { connStr += fmt.Sprintf(" sslkey=%s", c.SSLKey) }
+	if c.SSLRootCert != "" { connStr += fmt.Sprintf(" sslrootcert=%s", c.SSLRootCert) }
+	if c.Schema != "" { connStr += fmt.Sprintf(" search_path=%s", c.Schema) }
+	return connStr
+}
+
+type TiKVConfig struct {
+	Enabled   bool   `toml:"enabled"`
+	PDAddrs   string `toml:"pdaddrs"`
+	KeyPrefix string `toml:"keyPrefix"`
+	Enable1PC bool   `toml:"enable_1pc"`
+	CAPath    string `toml:"ca_path"`
+	CertPath  string `toml:"cert_path"`
+	KeyPath   string `toml:"key_path"`
+	VerifyCN  string `toml:"verify_cn"`
+}
+
+func (c *TiKVConfig) PDAddrList() []string {
+	addrs := strings.Split(c.PDAddrs, ",")
+	for i := range addrs { addrs[i] = strings.TrimSpace(addrs[i]) }
+	return addrs
+}
+
+type Entry struct {
+	Dirhash   int64
+	Directory string
+	Name      string
+	Meta      []byte
+}
+
+func HashToBytes(dir string) []byte {
+	h := sha1.New()
+	io.WriteString(h, dir)
+	return h.Sum(nil)
+}
+
+func GenerateTiKVKey(prefix []byte, directory, filename string) []byte {
+	key := HashToBytes(directory)
+	key = append(key, []byte(filename)...)
+	if len(prefix) > 0 {
+		result := make([]byte, len(prefix)+len(key))
+		copy(result, prefix)
+		copy(result[len(prefix):], key)
+		return result
+	}
+	return key
+}
+
+func ApplyPathPrefix(prefix, directory string) string {
+	if prefix == "" { return directory }
+	if directory == "/" { return prefix }
+	return prefix + directory
+}
```

Then restructure as:
```
cmd/
  pg2tikv/main.go
  audit/main.go
  count-keys/main.go
internal/
  common/config.go
seaweed-cleanup-go/
  main.go
```

### CQ-2: No Tests (HIGH)

Zero test files exist in the entire project. At minimum, the following should have unit tests:
- `generateTiKVKey()` - correctness is critical for data integrity
- `applyPathPrefix()` - edge cases matter (root dir, trailing slashes)
- `prefixEndKey()` - boundary conditions (all 0xff bytes)
- `ProgressTracker` - contiguous sequence logic
- `parseProtoFields()` - protobuf parsing correctness

### CQ-3: Unused Variable in seaweed-cleanup-go (LOW)

`seaweed-cleanup-go/main.go:1058`:
```go
_ = lastRateCount
```
and line 1062:
```go
_ = d
```

These are dead variables assigned but never meaningfully used.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -1001,7 +1001,6 @@ func main() {
 	// Rate tracking
 	startTime := time.Now()
 	var lastRateTime time.Time
-	var lastRateCount int64

 	// Collector goroutine
 	done := make(chan struct{})
@@ -1047,12 +1046,10 @@ func main() {
 			if t%500 == 0 {
 				now := time.Now()
 				elapsed := now.Sub(startTime).Seconds()
 				avgRate := float64(t) / elapsed
 				recentElapsed := now.Sub(lastRateTime).Seconds()
 				recentRate := float64(500) / recentElapsed
 				lastRateTime = now
-				lastRateCount = t

-				d := atomic.LoadInt64(&dupes)
 				del := atomic.LoadInt64(&deleted)
 				mig := atomic.LoadInt64(&migrated)
 				orph := atomic.LoadInt64(&notDupes)
 				errs := atomic.LoadInt64(&errors)
-				_ = lastRateCount

 				fmt.Printf("\n%s%s--- Progress: %d checked | %d deleted | %d migrated | %d orphans | %d errors | %.1f/s avg | %.1f/s now ---%s%s\n\n",
 					colorBold, colorMagenta, t, del, mig, orph, errs, avgRate, recentRate, colorReset, colorReset)
-				_ = d
 			}
```

---

## Reliability Findings

### REL-1: Progress Reporter Goroutine Leak (MEDIUM)

`main.go:598-631` starts a goroutine with a ticker that is only stopped via `ctx.Done()`. After `cancel()` is deferred, the goroutine will eventually exit, but there's no `sync.WaitGroup` or channel to ensure it completes before the program accesses final state. In practice this is benign since the goroutine only writes to the state file (which is also written at the end of main), but it's a correctness issue.

```diff
--- a/main.go
+++ b/main.go
@@ -596,8 +596,10 @@ func main() {
 	// Progress reporter
 	startTime := time.Now()
+	reporterDone := make(chan struct{})
 	go func() {
+		defer close(reporterDone)
 		ticker := time.NewTicker(10 * time.Second)
 		defer ticker.Stop()
 		var lastCount int64
@@ -631,6 +633,7 @@ func main() {
 	}()

 	// ... (after all workers finish) ...
+	<-reporterDone
```

### REL-2: Audit Complete Mode Has No Pagination (MEDIUM)

`audit.go:951-957` runs a complete table scan in a single query:
```go
query = fmt.Sprintf(`SELECT dirhash, directory, name, meta FROM %q ORDER BY dirhash, name`, *table)
```

For tables with millions of rows, this loads the entire result set through the driver's cursor. While Go's `database/sql` uses row-by-row scanning, some driver configurations may buffer. The migration tool (`main.go`) handles this correctly with paginated queries.

### REL-3: Error Swallowing in Scan (LOW)

`main.go:712-714` and `audit.go:969-972` log scan errors and `continue`, which means the row is silently skipped. For a migration tool, a scan error could mean data loss if not investigated.

### REL-4: `dryRun` Variable Shadowed in seaweed-cleanup-go (INFO)

`seaweed-cleanup-go/main.go:882`:
```go
_ = *dryRun // used implicitly
```

The `dryRun` flag is declared but never explicitly checked. Instead, the code relies on the fact that if neither `--delete` nor `--migrate` is set, nothing destructive happens. This works but is confusing - a `--dry-run` flag that does nothing suggests it controls behavior when combined with `--delete`/`--migrate`, but it doesn't.

---

## Architecture Findings

### ARCH-1: No Makefile or Build Instructions (HIGH)

There is no Makefile, Dockerfile, or README with build instructions. The non-standard multi-main-in-one-package layout means a new developer cannot figure out how to build without reading all the code. A simple Makefile would solve this:

```makefile
.PHONY: all clean

all: seaweed-pg2tikv seaweed-pg2tikv-audit seaweed-count-keys

seaweed-pg2tikv: main.go
	go build -o $@ $<

seaweed-pg2tikv-audit: audit.go
	go build -o $@ $<

seaweed-count-keys: count_keys.go
	go build -o $@ $<

clean:
	rm -f seaweed-pg2tikv seaweed-pg2tikv-audit seaweed-count-keys
```

### ARCH-2: Go Version Mismatch Between Modules (LOW)

- `go.mod`: `go 1.21`
- `seaweed-cleanup-go/go.mod`: `go 1.24.0`

This is fine for separate modules, but the main module's `go 1.21` is quite old relative to the cleanup tool's `go 1.24.0`. Consider upgrading the main module.

---

## Positive Observations

These aspects of the codebase are well-done and should be preserved:

1. **Retry with exponential backoff** (`main.go:230-294`): Proper context-aware retry with configurable parameters.
2. **Contiguous progress tracking** (`main.go:335-395`): Only advances resume point on contiguous batch completion. This prevents skipping failed batches on restart.
3. **Pre-flight safety checks** (`seaweed-cleanup-go/main.go:612-840`): Both delete and migrate operations verify safety on a real entry before proceeding with bulk operations.
4. **Context-aware sleep** (`main.go:218-227`): `sleepCtx` properly handles cancellation during backoff.
5. **Graceful shutdown** (`main.go:522-524`): Signal handling with double-interrupt for force exit.
6. **SQL injection prevention**: Regex + identifier quoting consistently applied.
7. **Protobuf field analysis** (`audit.go:160-663`): Thorough deep-diff of protobuf entries for mismatch diagnosis.
8. **Shard-based parallelism** (`seaweed-cleanup-go`): FNV hashing for deterministic work partitioning.

---

## Priority Summary

| Priority | Finding | Impact |
|----------|---------|--------|
| CRITICAL | Version constants out of sync with VERSION file | Users get wrong version info |
| CRITICAL | Multi-main build fragility (no Makefile/docs) | New developers can't build |
| HIGH | ~115 lines duplicated between main.go and audit.go | Maintenance burden, divergence risk |
| HIGH | Zero test coverage | No regression safety net |
| MEDIUM | No confirmation on DeleteRange in count_keys.go | Accidental data destruction |
| MEDIUM | Progress reporter goroutine leak | Benign but incorrect |
| MEDIUM | Audit complete mode no pagination | Memory pressure on large tables |
| LOW | Connection string value injection | Breaks on passwords with spaces |
| LOW | Unused variables in cleanup tool | Code noise |
| LOW | gRPC without TLS in cleanup tool | Expected for internal use |
| INFO | State file world-readable (0644) | No sensitive data in state |
| INFO | dryRun flag does nothing in cleanup | Confusing UX |

---

*End of audit report.*
