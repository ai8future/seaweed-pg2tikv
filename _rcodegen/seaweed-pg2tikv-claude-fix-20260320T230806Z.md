Date Created: 2026-03-20T23:08:06Z
TOTAL_SCORE: 64/100

# Code Audit Report: seaweed-pg2tikv

**Agent:** Claude Code (Claude:Opus 4.6)
**Scope:** Full codebase audit — main.go, audit.go, count_keys.go, seaweed-cleanup-go/main.go
**Date:** 2026-03-20

---

## Score Breakdown

| Category         | Score   | Notes                                                      |
|------------------|---------|------------------------------------------------------------|
| Correctness      | 20/30   | Works in practice, but several logic bugs and dead code    |
| Security         | 15/20   | SQL injection mitigated; password quoting & gRPC issues    |
| Code Quality     | 10/25   | Massive duplication, no tests, non-standard layout         |
| Reliability      | 11/15   | Good retry/state mgmt; dangerous nil in DeleteRange        |
| Documentation    | 8/10    | Good inline comments; stale embedded versions              |
| **TOTAL**        | **64/100** |                                                         |

---

## CRITICAL Issues

### C1: Build-Breaking Project Structure — Multiple `main()` in Same Package Directory

**Files:** `main.go`, `count_keys.go`, `audit.go`

All three files declare `package main` with their own `main()` function, `const Version`, and duplicate type definitions (`FilerConfig`, `Postgres2Config`, `TiKVConfig`, `Entry`, `hashToBytes`, `generateTiKVKey`, `applyPathPrefix`, `MinInt64`, `validTableName`). Running `go build`, `go vet`, or `go test` in this directory will fail with compilation errors.

This must be built as individual files: `go build -o bin main.go`. This is fragile, non-standard, and prevents use of Go's standard toolchain.

**Recommended fix:** Refactor to `cmd/` subdirectories and a shared `internal/` package:

```
cmd/
  seaweed-pg2tikv/main.go
  seaweed-pg2tikv-audit/main.go
  seaweed-count-keys/main.go
internal/
  config/config.go       # FilerConfig, Postgres2Config, TiKVConfig
  tikv/keys.go           # hashToBytes, generateTiKVKey, applyPathPrefix
  types/entry.go         # Entry, constants
```

This eliminates ~200 lines of duplicated code and enables `go build ./...`, `go vet ./...`, and `go test ./...`.

---

### C2: `prefixEndKey` Returns `nil` — Passed to `DeleteRange` Can Delete Everything

**File:** `count_keys.go:34-45`

When all prefix bytes are `0xFF`, `prefixEndKey` returns `nil`. This `nil` is passed directly to `rawkv.DeleteRange(ctx, prefixBytes, nil)` on line 179. In TiKV's raw API, a nil end key means "everything from start to the end of the keyspace". This could **delete the entire TiKV database**.

```go
// count_keys.go:34-45
func prefixEndKey(prefix []byte) []byte {
    end := make([]byte, len(prefix))
    copy(end, prefix)
    for i := len(end) - 1; i >= 0; i-- {
        if end[i] < 0xff {
            end[i]++
            return end[:i+1]
        }
    }
    // All 0xff bytes — no upper bound possible (extremely unlikely for a text prefix)
    return nil  // <-- DANGEROUS: nil end key = delete to end of keyspace
}
```

**Patch:**

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -155,6 +155,12 @@ func deleteAllKeys(addrs []string) {
 	ctx := context.Background()
 	prefixBytes := []byte(*prefix)
 	endKey := prefixEndKey(prefixBytes)
+	if endKey == nil {
+		log.Fatalf("Cannot compute end key for prefix %q (all 0xFF bytes). "+
+			"This would delete everything from the prefix to the end of the keyspace. "+
+			"Use a more specific prefix.", *prefix)
+	}
+

 	log.Printf("Connecting to TiKV (raw client) at %s", *pdAddrs)
```

---

### C3: `maxMismatches` Flag Logs But Does Not Actually Stop Processing

**File:** `audit.go:918-920`

When the max mismatch threshold is reached, a log line is printed but the audit continues scanning all remaining rows. The `entryChan` is never closed and the query keeps running.

```go
// audit.go:918-920
if *maxMismatches > 0 && m >= int64(*maxMismatches) {
    log.Printf("Reached max mismatches (%d), stopping early", *maxMismatches)
    // BUG: nothing actually stops — all rows will still be processed
}
```

**Patch:**

```diff
--- a/audit.go
+++ b/audit.go
@@ -818,6 +818,9 @@ func main() {
 	ctx, cancel := context.WithCancel(context.Background())
 	defer cancel()

+	// Used to signal early termination from collector
+	mismatchCancel := cancel
+
 	// Connect to Postgres
@@ -916,6 +919,7 @@ func main() {
 				if *maxMismatches > 0 && m >= int64(*maxMismatches) {
 					log.Printf("Reached max mismatches (%d), stopping early", *maxMismatches)
+					mismatchCancel()
 				}
```

This will cause the context to cancel, stopping the query row iteration and workers.

---

## HIGH Severity Issues

### H1: Postgres Connection String Password Injection

**Files:** `main.go:56-74`, `audit.go:56-74`

The `ConnectionString()` method uses `fmt.Sprintf` to build a space-delimited libpq connection string. If the password contains spaces, single quotes, or backslashes, the connection string will be malformed or could be interpreted as additional connection parameters.

```go
connStr := fmt.Sprintf(
    "host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
    c.Hostname, c.Port, c.Username, c.Password, c.Database, c.SSLMode,
)
```

A password like `my pass` becomes `password=my pass` which libpq parses as `password=my` plus an unknown parameter `pass`.

**Patch:**

```diff
--- a/main.go
+++ b/main.go
@@ -54,8 +54,12 @@ type Postgres2Config struct {
 }

+func quoteConnValue(s string) string {
+	s = strings.ReplaceAll(s, `\`, `\\`)
+	s = strings.ReplaceAll(s, `'`, `\'`)
+	return "'" + s + "'"
+}
+
 func (c *Postgres2Config) ConnectionString() string {
 	connStr := fmt.Sprintf(
-		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
-		c.Hostname, c.Port, c.Username, c.Password, c.Database, c.SSLMode,
+		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
+		c.Hostname, c.Port, c.Username, quoteConnValue(c.Password), c.Database, c.SSLMode,
 	)
```

Apply the same fix to `audit.go`.

---

### H2: Embedded Version Constants Are Stale

**Files:** `main.go:28`, `count_keys.go:20`, `audit.go:29`

| File           | Embedded Version | VERSION file |
|----------------|-----------------|--------------|
| main.go        | `1.2.0`         | `1.2.6`      |
| count_keys.go  | `1.2.0`         | `1.2.6`      |
| audit.go       | `1.2.2`         | `1.2.6`      |

These constants are manually maintained and have drifted from the actual VERSION file. The `--version` flag reports incorrect versions.

**Patch:**

```diff
--- a/main.go
+++ b/main.go
@@ -25,7 +25,7 @@
 )

-const Version = "1.2.0"
+const Version = "1.2.6"

--- a/count_keys.go
+++ b/count_keys.go
@@ -18,7 +18,7 @@
 )

-const Version = "1.2.0"
+const Version = "1.2.6"

--- a/audit.go
+++ b/audit.go
@@ -27,7 +27,7 @@
 )

-const Version = "1.2.2"
+const Version = "1.2.6"
```

Better long-term fix: inject the version at build time via `ldflags`:
```bash
go build -ldflags "-X main.Version=$(cat VERSION)" -o seaweed-pg2tikv main.go
```

---

### H3: Linear Backoff Mislabeled as Exponential

**File:** `main.go:281`

```go
// Exponential backoff                         <-- label says exponential
backoff := time.Duration(retry+1) * time.Duration(retryBaseDelayMs) * time.Millisecond
// retry 0: 500ms, retry 1: 1000ms, retry 2: 1500ms  <-- this is LINEAR
```

True exponential would be:
```go
backoff := time.Duration(1<<uint(retry)) * time.Duration(retryBaseDelayMs) * time.Millisecond
// retry 0: 500ms, retry 1: 1000ms, retry 2: 2000ms, retry 3: 4000ms
```

This same linear pattern is also on lines 238 and 261.

**Patch:**

```diff
--- a/main.go
+++ b/main.go
@@ -235,7 +235,7 @@ func commitBatch(ctx context.Context, tikvClient *txnkv.Client, prefix []byte, e
 		if err != nil {
 			log.Printf("[worker-%02d] begin txn error (attempt %d/%d): %v", workerID, retry+1, maxRetryCount, err)
-			if !sleepCtx(ctx, time.Duration(retry+1)*time.Duration(retryBaseDelayMs)*time.Millisecond) {
+			if !sleepCtx(ctx, time.Duration(1<<uint(retry))*time.Duration(retryBaseDelayMs)*time.Millisecond) {
 				return ctx.Err()
 			}
@@ -258,7 +258,7 @@ func commitBatch(ctx context.Context, tikvClient *txnkv.Client, prefix []byte, e
 		if setErr != nil {
 			txn.Rollback()
-			backoff := time.Duration(retry+1) * time.Duration(retryBaseDelayMs) * time.Millisecond
+			backoff := time.Duration(1<<uint(retry)) * time.Duration(retryBaseDelayMs) * time.Millisecond
 			if retry < maxRetryCount-1 {
@@ -278,7 +278,7 @@ func commitBatch(ctx context.Context, tikvClient *txnkv.Client, prefix []byte, e
 		txn.Rollback()

-		// Exponential backoff
-		backoff := time.Duration(retry+1) * time.Duration(retryBaseDelayMs) * time.Millisecond
+		// Exponential backoff with base delay
+		backoff := time.Duration(1<<uint(retry)) * time.Duration(retryBaseDelayMs) * time.Millisecond
```

**Note:** With 15 max retries and true exponential backoff starting at 500ms, retry 14 would be `2^14 * 500ms = 8192s (~2.3 hours)`. You may want to cap the backoff:

```go
backoff := time.Duration(1<<uint(retry)) * time.Duration(retryBaseDelayMs) * time.Millisecond
if backoff > 30*time.Second {
    backoff = 30 * time.Second
}
```

---

## MEDIUM Severity Issues

### M1: `dryRun` Flag Is Dead Code in seaweed-cleanup-go

**File:** `seaweed-cleanup-go/main.go:879-882`

```go
if !*doDelete && !*doMigrate {
    *dryRun = true
}
_ = *dryRun // used implicitly
```

The `_ = *dryRun` comment "used implicitly" is incorrect. `dryRun` is never checked anywhere. The `processEntry` function receives `doDelete` and `doMigrate` directly. The flag exists on the CLI but has no effect.

**Patch:** Either remove the flag or actually check it:

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -876,9 +876,8 @@ func main() {

-	if !*doDelete && !*doMigrate {
-		*dryRun = true
-	}
-	_ = *dryRun // used implicitly
+	if *dryRun {
+		*doDelete = false
+		*doMigrate = false
+	}
```

---

### M2: No Signal Handling in count_keys.go

**File:** `count_keys.go`

The count operation iterates over potentially millions of TiKV keys with no way to gracefully interrupt. Ctrl-C will terminate the process abruptly. The delete operation also has no cancellation mechanism beyond the raw context.

**Patch:**

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -7,6 +7,8 @@ import (
 	"flag"
 	"fmt"
 	"log"
+	"os"
+	"os/signal"
 	"strings"
 	"time"
@@ -98,7 +100,11 @@ func main() {

 func countAllKeys(addrs []string) {
-	client, err := txnkv.NewClient(addrs)
+	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
+	defer stop()
+
+	client, err := txnkv.NewClient(addrs)
 	if err != nil {
```

---

### M3: `ORDER BY RANDOM()` Performance in Audit Sample Mode

**File:** `audit.go:944-949`

```go
query = fmt.Sprintf(`
    SELECT dirhash, directory, name, meta
    FROM %q
    ORDER BY RANDOM()
    LIMIT %d`,
    *table, *sampleSize)
```

`ORDER BY RANDOM()` requires Postgres to scan the entire table, assign a random value to each row, sort, and return the top N. For a table with millions of rows, this is extremely slow.

**Patch (use TABLESAMPLE):**

```diff
--- a/audit.go
+++ b/audit.go
@@ -941,10 +941,14 @@ func main() {
 	var query string
 	if *mode == "sample" {
+		// Calculate sample percentage to get approximately sampleSize rows
+		// Add 20% buffer since TABLESAMPLE is approximate
+		samplePct := float64(*sampleSize) / float64(totalRows) * 120.0
+		if samplePct > 100 {
+			samplePct = 100
+		}
 		query = fmt.Sprintf(`
 			SELECT dirhash, directory, name, meta
-			FROM %q
-			ORDER BY RANDOM()
-			LIMIT %d`,
-			*table, *sampleSize)
+			FROM %q TABLESAMPLE SYSTEM (%.4f)
+			LIMIT %d`,
+			*table, samplePct, *sampleSize)
```

---

### M4: Unchecked Errors from `http.NewRequest` in Safety Checks

**File:** `seaweed-cleanup-go/main.go:686,804,816`

```go
req, _ := http.NewRequest("GET", testURL, nil)    // line 686
readReq, _ := http.NewRequest("GET", testURL, nil) // line 804
delReq, _ := http.NewRequest("DELETE", delURL, nil) // line 816
```

While `http.NewRequest` rarely fails for programmatically-constructed URLs, discarding errors in safety-critical code paths is bad practice.

**Patch:**

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -683,7 +683,11 @@ func runDeleteSafetyCheck(filerURL, startAfter string) bool {
 	// Verify data readable
 	testURL := encodePath(filerURL, bucketFilePath)
-	req, _ := http.NewRequest("GET", testURL, nil)
+	req, err := http.NewRequest("GET", testURL, nil)
+	if err != nil {
+		fmt.Fprintf(os.Stderr, "  Failed to create request: %v\n", err)
+		return false
+	}
```

Apply similar fixes at lines 804 and 816.

---

### M5: Insecure gRPC Connection

**File:** `seaweed-cleanup-go/main.go:935-937`

```go
conn, err := grpc.NewClient(grpcAddr,
    grpc.WithTransportCredentials(insecure.NewCredentials()),
)
```

The gRPC connection uses plaintext with no TLS. For a tool that migrates and deletes data, this exposes operations to MITM attacks on untrusted networks. Consider adding a `--tls` flag or at minimum documenting the security implication.

---

### M6: `lastRateCount` Variable Assigned But Never Used

**File:** `seaweed-cleanup-go/main.go:1004,1051,1058`

```go
var lastRateCount int64          // line 1004
lastRateCount = t                // line 1051 — assigned
_ = lastRateCount               // line 1058 — explicitly suppressed
```

This is dead code.

**Patch:**

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -1001,7 +1001,6 @@ func main() {
 	startTime := time.Now()
 	var lastRateTime time.Time
-	var lastRateCount int64

 	// Collector goroutine
@@ -1048,8 +1047,6 @@ func main() {
 				recentRate := float64(500) / recentElapsed
 				lastRateTime = now
-				lastRateCount = t

@@ -1055,8 +1052,6 @@ func main() {
 				errs := atomic.LoadInt64(&errors)
-				_ = lastRateCount

```

---

### M7: No Confirmation Prompt for Destructive Delete Operation

**File:** `count_keys.go:93-94`

The `--delete` flag immediately performs a server-side `DeleteRange` with no confirmation. A typo in the prefix could delete critical data.

**Patch:**

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -91,6 +91,14 @@ func main() {

 	if *deleteKeys {
+		fmt.Printf("WARNING: About to delete ALL keys with prefix %q\n", *prefix)
+		fmt.Printf("This operation is IRREVERSIBLE. Type 'yes' to proceed: ")
+		var confirm string
+		fmt.Scanln(&confirm)
+		if confirm != "yes" {
+			fmt.Println("Aborted.")
+			return
+		}
 		deleteAllKeys(addrs)
```

---

## LOW Severity Issues

### L1: No Test Files

There are zero test files (`*_test.go`) in the entire project. Critical functions like `generateTiKVKey`, `applyPathPrefix`, `prefixEndKey`, `parseProtoFields`, and `commitBatch` have no unit tests. For a data migration tool, correctness guarantees from tests are essential.

### L2: Binary Files Checked Into Git

The following compiled binaries are tracked in the repository:
- `seaweed-count-keys-linux-amd64`
- `seaweed-pg2tikv-linux-amd64`
- `seaweed-pg2tikv-audit-linux-amd64`
- `seaweed-cleanup-go/seaweed-cleanup`

Also `__pycache__/` directory.

**Patch (.gitignore additions):**

```diff
--- a/.gitignore
+++ b/.gitignore
@@ -end of file
+seaweed-pg2tikv-linux-amd64
+seaweed-pg2tikv-audit-linux-amd64
+seaweed-count-keys-linux-amd64
+seaweed-cleanup-go/seaweed-cleanup
+__pycache__/
```

### L3: Duplicate `d` Variable Loaded But Suppressed

**File:** `seaweed-cleanup-go/main.go:1053,1062`

```go
d := atomic.LoadInt64(&dupes)     // loaded
_ = d                              // suppressed
```

This is dead code from a previous iteration.

### L4: State File Permission 0644

**File:** `main.go:152`

`os.WriteFile(filename, data, 0644)` makes the state file world-readable. While it doesn't contain secrets, 0600 would be more appropriate for operational state.

### L5: Missing Timeout for Safety Check gRPC Calls

**File:** `seaweed-cleanup-go/main.go:771`

```go
ctx := context.Background()  // no timeout!
lookupResp, err := grpcClient.LookupDirectoryEntry(ctx, ...)
```

If the filer is unresponsive, this hangs indefinitely. The migration retry loops use timeouts correctly (`context.WithTimeout`), but the safety check calls do not.

### L6: `sampleFiles` Constant Is Misleading

**File:** `seaweed-cleanup-go/main.go:47`

```go
const sampleFiles = 0 // 0 = verify ALL files (no sampling)
```

This constant is never actually used to control sampling behavior in `verifyDuplicate` — that function always checks all files. The constant and its comment are confusing dead documentation.

---

## Positive Observations

1. **Good SQL injection prevention** — Table names are validated against a regex before use in queries, and `%q` quoting is used (though `%q` alone would not prevent injection for table names in all cases, the regex is the real guard).

2. **Robust state management** — The migration tool's checkpoint/resume system with contiguous batch tracking is well-designed and handles edge cases (negative dirhash, old state files, partition mismatch).

3. **Graceful shutdown** — Signal handling with double-Ctrl-C force-exit is a good UX pattern.

4. **Safety checks before destructive operations** — The cleanup tool performs pre-flight safety checks before deletion and migration, which is excellent for production safety.

5. **Context-aware retries** — The `sleepCtx` pattern properly integrates retry backoff with context cancellation.

6. **Good protobuf diff analysis** — The audit tool's field-level mismatch analysis with human-readable field names and timestamp formatting is production-quality observability.

---

## Summary

The codebase is a **functional production tool** with solid retry/resume semantics and good operational safety checks. The main concerns are:

1. **Project structure** prevents standard Go tooling (`go build`, `go vet`, `go test`)
2. **Massive code duplication** (~200 lines) between main.go and audit.go
3. **One potentially catastrophic bug** (`nil` end key in DeleteRange)
4. **Several logic bugs** (maxMismatches, linear-not-exponential backoff, dead dryRun flag)
5. **No test coverage** for a data migration tool
6. **Stale version constants** across all binaries
