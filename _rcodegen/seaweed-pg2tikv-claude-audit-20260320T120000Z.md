Date Created: 2026-03-20T12:00:00Z
TOTAL_SCORE: 62/100

# seaweed-pg2tikv Code Audit Report

**Auditor:** Claude:Opus 4.6 (Claude Code)
**Files Audited:** main.go (813 LOC), audit.go (1051 LOC), count_keys.go (190 LOC), seaweed-cleanup-go/main.go (1248 LOC)
**Module:** seaweed-pg2tikv (Go 1.21)

---

## Score Breakdown

| Category | Score | Weight | Weighted |
|----------|-------|--------|----------|
| Security | 65/100 | 25% | 16.25 |
| Code Quality & Maintainability | 50/100 | 25% | 12.50 |
| Reliability & Error Handling | 78/100 | 25% | 19.50 |
| Architecture & Build | 55/100 | 25% | 13.75 |
| **TOTAL** | | | **62/100** |

---

## CRITICAL FINDINGS

### C1: Insecure gRPC Transport in seaweed-cleanup-go (SECURITY)

**File:** `seaweed-cleanup-go/main.go:935-937`
**Severity:** HIGH
**Description:** The gRPC client uses insecure (plaintext) credentials for all connections, including those performing destructive operations (CREATE, DELETE). In a production environment, this exposes metadata migration/deletion operations to MITM attacks.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -23,6 +23,8 @@ import (

 	"google.golang.org/grpc"
 	"google.golang.org/grpc/credentials/insecure"
+	"google.golang.org/grpc/credentials"
+	"crypto/tls"
 )

 var candidateBuckets = []string{
@@ -842,6 +844,8 @@ func main() {
 	filerGRPC := flag.String("filer-grpc", "", "Filer gRPC address (default: derived from -filer, port+10000)")
 	doDelete := flag.Bool("delete", false, "Delete confirmed duplicate root metadata")
 	doMigrate := flag.Bool("migrate", false, "Migrate orphan entries to correct buckets via gRPC")
+	grpcTLS := flag.Bool("grpc-tls", false, "Use TLS for gRPC connection")
+	grpcInsecure := flag.Bool("grpc-insecure", false, "Explicitly allow insecure gRPC (required if --grpc-tls is not set)")
 	dryRun := flag.Bool("dry-run", false, "Only report, don't delete or migrate")
 	batchSize := flag.Int("batch-size", 1000, "Entries per page")
 	startAfter := flag.String("start-after", "", "Resume from this directory name")
@@ -932,9 +936,18 @@ func main() {
 	var grpcClient pb.SeaweedFilerClient
 	if *doMigrate {
-		conn, err := grpc.NewClient(grpcAddr,
-			grpc.WithTransportCredentials(insecure.NewCredentials()),
-		)
+		var transportCreds grpc.DialOption
+		if *grpcTLS {
+			transportCreds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{}))
+		} else if *grpcInsecure {
+			transportCreds = grpc.WithTransportCredentials(insecure.NewCredentials())
+		} else {
+			fmt.Fprintln(os.Stderr, "Error: gRPC requires --grpc-tls or --grpc-insecure flag")
+			os.Exit(1)
+		}
+		conn, err := grpc.NewClient(grpcAddr, transportCreds)
 		if err != nil {
 			fmt.Fprintf(os.Stderr, "Failed to connect gRPC: %v\n", err)
 			os.Exit(1)
```

---

### C2: No Confirmation for Destructive DeleteRange (SECURITY/SAFETY)

**File:** `count_keys.go:93-97, 157-188`
**Severity:** HIGH
**Description:** `--delete` flag triggers an irreversible server-side `DeleteRange` across all keys matching a prefix with zero confirmation prompt. A typo in the prefix could wipe production data.

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -90,6 +90,15 @@ func main() {

 	if *deleteKeys {
+		fmt.Printf("WARNING: About to DELETE ALL keys with prefix %q\n", *prefix)
+		fmt.Printf("This is IRREVERSIBLE. Type 'yes' to confirm: ")
+		var confirm string
+		fmt.Scanln(&confirm)
+		if confirm != "yes" {
+			fmt.Println("Aborted.")
+			return
+		}
 		deleteAllKeys(addrs)
 	} else {
 		countAllKeys(addrs)
```

---

### C3: Massive Code Duplication Across Build Targets (QUALITY)

**File:** main.go + audit.go + count_keys.go
**Severity:** HIGH
**Description:** The following types and functions are duplicated verbatim between main.go and audit.go (all in `package main`):

- `FilerConfig`, `Postgres2Config`, `TiKVConfig` structs and methods (~80 lines each)
- `hashToBytes`, `generateTiKVKey`, `applyPathPrefix` functions
- `Entry` struct
- `const MinInt64`, `const Version`
- `var validTableName` regex
- CLI flag declarations (partial overlap)

These files cannot be compiled together due to symbol collisions, requiring per-file builds (`go build main.go`, `go build audit.go`). This is fragile and non-standard.

**Recommended fix:** Extract shared code into a `pkg/` or `internal/` package, and split binaries into `cmd/pg2tikv/`, `cmd/audit/`, `cmd/count-keys/` directories.

```diff
--- /dev/null
+++ b/cmd/pg2tikv/main.go
@@ -0,0 +1,3 @@
+// Move migration-specific code here
+// Import shared types from internal/config, internal/tikv
+package main

--- /dev/null
+++ b/internal/config/config.go
@@ -0,0 +1,80 @@
+package config
+
+import (
+	"fmt"
+	"strings"
+)
+
+type FilerConfig struct {
+	Postgres2 Postgres2Config `toml:"postgres2"`
+	TiKV      TiKVConfig      `toml:"tikv"`
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
+	if c.SSLCert != "" {
+		connStr += fmt.Sprintf(" sslcert=%s", c.SSLCert)
+	}
+	if c.SSLKey != "" {
+		connStr += fmt.Sprintf(" sslkey=%s", c.SSLKey)
+	}
+	if c.SSLRootCert != "" {
+		connStr += fmt.Sprintf(" sslrootcert=%s", c.SSLRootCert)
+	}
+	if c.Schema != "" {
+		connStr += fmt.Sprintf(" search_path=%s", c.Schema)
+	}
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
+	for i := range addrs {
+		addrs[i] = strings.TrimSpace(addrs[i])
+	}
+	return addrs
+}
```

---

## HIGH-SEVERITY FINDINGS

### H1: State File Written with World-Readable Permissions (SECURITY)

**File:** `main.go:152`
**Severity:** MEDIUM
**Description:** Migration state files are created with `0644` permissions. While they don't contain credentials directly, they reveal table names, migration progress, and schema details.

```diff
--- a/main.go
+++ b/main.go
@@ -149,7 +149,7 @@ func saveState(filename string, state *State) error {
 	if err != nil {
 		return fmt.Errorf("failed to marshal state: %w", err)
 	}
-	if err := os.WriteFile(filename, data, 0644); err != nil {
+	if err := os.WriteFile(filename, data, 0600); err != nil {
 		return fmt.Errorf("failed to write state file: %w", err)
 	}
 	return nil
```

---

### H2: Password Potentially Logged in Connection String (SECURITY)

**File:** `main.go:57-59` (duplicated in `audit.go:57-59`)
**Severity:** MEDIUM
**Description:** The `ConnectionString()` method includes the password in the returned string. If the connection string is ever logged (e.g., in a panic or debug scenario), the password is exposed. The `lib/pq` driver supports DSN-style connection strings or environment variables as alternatives.

Note: Currently the connection string is only passed to `sql.Open()` and not logged directly, so the current risk is low. However, a future log statement could leak it.

```diff
--- a/main.go
+++ b/main.go
@@ -487,9 +487,10 @@ func main() {
 	// Print config summary
 	log.Printf("seaweed-pg2tikv version %s", Version)
 	log.Println("=== Configuration ===")
-	log.Printf("Postgres: %s@%s:%d/%s (table: %s)",
+	log.Printf("Postgres: %s@%s:%d/%s (table: %s, ssl: %s)",
 		pgConfig.Postgres2.Username,
 		pgConfig.Postgres2.Hostname,
 		pgConfig.Postgres2.Port,
 		pgConfig.Postgres2.Database,
-		*table)
+		*table,
+		pgConfig.Postgres2.SSLMode)
```

---

### H3: No Tests (QUALITY)

**Severity:** HIGH
**Description:** The project has zero test files. Critical functions like `generateTiKVKey`, `applyPathPrefix`, `prefixEndKey`, `hashToBytes`, and the `ProgressTracker` are all untested. These are pure functions that are easy to test and critical to correctness.

```diff
--- /dev/null
+++ b/main_test.go
@@ -0,0 +1,85 @@
+package main
+
+import (
+	"testing"
+)
+
+func TestApplyPathPrefix(t *testing.T) {
+	tests := []struct {
+		prefix, directory, want string
+	}{
+		{"", "/foo/bar", "/foo/bar"},
+		{"", "/", "/"},
+		{"/buckets/my-bucket", "/", "/buckets/my-bucket"},
+		{"/buckets/my-bucket", "/subdir", "/buckets/my-bucket/subdir"},
+	}
+	for _, tt := range tests {
+		got := applyPathPrefix(tt.prefix, tt.directory)
+		if got != tt.want {
+			t.Errorf("applyPathPrefix(%q, %q) = %q, want %q",
+				tt.prefix, tt.directory, got, tt.want)
+		}
+	}
+}
+
+func TestGenerateTiKVKey(t *testing.T) {
+	// Verify key generation is deterministic
+	key1 := generateTiKVKey(nil, "/test/dir", "file.txt")
+	key2 := generateTiKVKey(nil, "/test/dir", "file.txt")
+	if string(key1) != string(key2) {
+		t.Error("generateTiKVKey is not deterministic")
+	}
+
+	// Verify prefix is prepended
+	prefix := []byte("test_")
+	keyWithPrefix := generateTiKVKey(prefix, "/test/dir", "file.txt")
+	if string(keyWithPrefix[:5]) != "test_" {
+		t.Error("prefix not correctly prepended")
+	}
+}
+
+func TestProgressTracker(t *testing.T) {
+	pt := NewProgressTracker()
+
+	// No resume point initially
+	_, ok := pt.GetSafeResumePoint()
+	if ok {
+		t.Error("expected no safe resume point initially")
+	}
+
+	// Record contiguous successes
+	pt.RecordResult(BatchResult{SeqNum: 0, LastEntry: Entry{Dirhash: 100}, Success: true, Count: 10})
+	pt.RecordResult(BatchResult{SeqNum: 1, LastEntry: Entry{Dirhash: 200}, Success: true, Count: 10})
+
+	entry, ok := pt.GetSafeResumePoint()
+	if !ok || entry.Dirhash != 200 {
+		t.Errorf("expected safe resume at dirhash 200, got %d (ok=%v)", entry.Dirhash, ok)
+	}
+
+	// Gap prevents advancement
+	pt.RecordResult(BatchResult{SeqNum: 3, LastEntry: Entry{Dirhash: 400}, Success: true, Count: 10})
+	entry, _ = pt.GetSafeResumePoint()
+	if entry.Dirhash != 200 {
+		t.Errorf("expected safe resume still at 200, got %d", entry.Dirhash)
+	}
+
+	success, fail, hasFail := pt.GetCounts()
+	if success != 30 || fail != 0 || hasFail {
+		t.Errorf("unexpected counts: success=%d, fail=%d, hasFail=%v", success, fail, hasFail)
+	}
+}
```

Note: This test file would need to be compiled alongside only `main.go` (not `audit.go` or `count_keys.go`) due to the current single-package multi-binary structure.

---

### H4: Binary Files and Python Cache Committed to Repository (QUALITY)

**Severity:** MEDIUM
**Description:** Pre-compiled Linux binaries and Python `__pycache__` artifacts are committed:
- `seaweed-pg2tikv-linux-amd64`
- `seaweed-count-keys-linux-amd64`
- `seaweed-pg2tikv-audit-linux-amd64`
- `seaweed-cleanup-go/seaweed-cleanup`
- `__pycache__/filer_pb2.cpython-312.pyc`
- `__pycache__/filer_pb2_grpc.cpython-312.pyc`

This bloats the repository and risks stale binaries being used instead of freshly compiled ones.

```diff
--- a/.gitignore
+++ b/.gitignore
@@ -1,3 +1,12 @@
+# Compiled binaries
+seaweed-pg2tikv-linux-amd64
+seaweed-count-keys-linux-amd64
+seaweed-pg2tikv-audit-linux-amd64
+seaweed-cleanup-go/seaweed-cleanup
+
+# Python cache
+__pycache__/
+*.pyc
+
 # Add existing patterns here
```

---

### H5: HTTP Requests in seaweed-cleanup-go Lack Context/Timeout Propagation (RELIABILITY)

**File:** `seaweed-cleanup-go/main.go:114-121, 143-153, 157-189`
**Severity:** MEDIUM
**Description:** HTTP requests in `filerGet`, `filerDeleteMetadataOnly`, and `getFileChunks` don't use `context.Context` from the caller. If the user hits Ctrl+C, in-flight HTTP requests won't be cancelled, and the tool hangs waiting for the 30-second HTTP client timeout.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -104,7 +104,7 @@ func encodePath(filerURL, path string) string {
 	return filerURL + strings.Join(segments, "/")
 }

-func filerGet(filerURL, path string, params map[string]string) (*FilerListResponse, error) {
+func filerGet(ctx context.Context, filerURL, path string, params map[string]string) (*FilerListResponse, error) {
 	u := encodePath(filerURL, path)
 	if len(params) > 0 {
 		vals := url.Values{}
@@ -114,7 +114,7 @@ func filerGet(filerURL, path string, params map[string]string) (*FilerListRespon
 		u += "?" + vals.Encode()
 	}

-	req, err := http.NewRequest("GET", u, nil)
+	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
 	if err != nil {
 		return nil, err
 	}
```

(Similar changes needed for `filerDeleteMetadataOnly`, `getFileChunks`, and all call sites.)

---

## MEDIUM-SEVERITY FINDINGS

### M1: Inconsistent Version Constants Across Files

**File:** main.go:28, audit.go:29, count_keys.go:20
**Severity:** MEDIUM
**Description:** Each file declares its own `const Version` with different values (main.go: "1.2.0", audit.go: "1.2.2", count_keys.go: "1.2.0"). There is also a `VERSION` file in the repo root. These can easily drift. A single source of truth should be used, ideally injected via `-ldflags` at build time.

```diff
--- a/main.go
+++ b/main.go
@@ -25,7 +25,8 @@ import (
 	"github.com/tikv/client-go/v2/txnkv"
 )

-const Version = "1.2.0"
+// Version is set at build time via -ldflags "-X main.Version=..."
+var Version = "dev"
```

Combined with a `Makefile`:
```makefile
VERSION := $(shell cat VERSION)
LDFLAGS := -X main.Version=$(VERSION)

build-pg2tikv:
	go build -ldflags "$(LDFLAGS)" -o seaweed-pg2tikv main.go

build-audit:
	go build -ldflags "$(LDFLAGS)" -o seaweed-pg2tikv-audit audit.go

build-count-keys:
	go build -ldflags "$(LDFLAGS)" -o seaweed-count-keys count_keys.go
```

---

### M2: `io.WriteString` Return Value Ignored

**File:** `main.go:164` and `audit.go:117`
**Severity:** LOW
**Description:** `io.WriteString(h, dir)` in `hashToBytes` ignores the error return. While `sha1.New()` never returns write errors, this is technically unchecked.

```diff
--- a/main.go
+++ b/main.go
@@ -162,7 +162,7 @@ func hashToBytes(dir string) []byte {
 	h := sha1.New()
-	io.WriteString(h, dir)
+	_, _ = io.WriteString(h, dir) // sha1 never errors on write
 	return h.Sum(nil)
 }
```

---

### M3: `http.NewRequest` Error Silently Discarded

**File:** `seaweed-cleanup-go/main.go:686, 804, 817`
**Severity:** MEDIUM
**Description:** Multiple calls to `http.NewRequest` assign the error to `_`, meaning malformed URLs would cause nil-pointer panics when `req` is used.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -684,7 +684,10 @@ func runDeleteSafetyCheck(filerURL, startAfter string) bool {
 	// Verify data readable
 	testURL := encodePath(filerURL, bucketFilePath)
-	req, _ := http.NewRequest("GET", testURL, nil)
+	req, err := http.NewRequest("GET", testURL, nil)
+	if err != nil {
+		fmt.Fprintf(os.Stderr, "  Failed to create request: %v\n", err)
+		return false
+	}
 	req.Header.Set("Range", "bytes=0-63")
```

(Same pattern at lines 804 and 817.)

---

### M4: Hardcoded Bucket Names and Extension Mapping

**File:** `seaweed-cleanup-go/main.go:28-45`
**Severity:** MEDIUM
**Description:** Bucket names and file extension mappings are hardcoded. Any change to the bucket structure requires code changes and recompilation. Consider loading from a config file.

---

### M5: Linear Backoff Instead of Exponential

**File:** `main.go:281`
**Severity:** LOW
**Description:** Comment says "Exponential backoff" but implementation is linear: `time.Duration(retry+1) * base` gives 1x, 2x, 3x... rather than exponential 1x, 2x, 4x, 8x...

```diff
--- a/main.go
+++ b/main.go
@@ -278,7 +278,7 @@ func commitBatch(...) error {
 		txn.Rollback()

-		// Exponential backoff
-		backoff := time.Duration(retry+1) * time.Duration(retryBaseDelayMs) * time.Millisecond
+		// Exponential backoff with base delay
+		backoff := time.Duration(1<<uint(retry)) * time.Duration(retryBaseDelayMs) * time.Millisecond
 		if retry < maxRetryCount-1 {
```

---

### M6: `dryRun` Variable Assigned But Never Used in seaweed-cleanup-go

**File:** `seaweed-cleanup-go/main.go:882`
**Severity:** LOW
**Description:** `_ = *dryRun // used implicitly` — this variable is never actually checked to prevent destructive operations. The dry-run behavior is only achieved by not setting `--delete` or `--migrate` flags, making the `--dry-run` flag misleading.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -879,7 +879,10 @@ func main() {
 	if !*doDelete && !*doMigrate {
 		*dryRun = true
 	}
-	_ = *dryRun // used implicitly
+	if *dryRun {
+		*doDelete = false
+		*doMigrate = false
+	}
```

---

### M7: Potential Integer Overflow in Query Limit Calculation

**File:** `main.go:636`
**Severity:** LOW
**Description:** `queryLimit := *batchSize * *workers * 2` — with large values of `batchSize` and `workers`, this could overflow `int`. Unlikely in practice but worth guarding.

---

## POSITIVE OBSERVATIONS

1. **SQL Injection Protection (main.go:33, 430-431, 643-658):** Table names are validated with regex AND quoted with `%q` in `fmt.Sprintf`. Double protection is good.

2. **Graceful Shutdown (main.go:521-524, 772-785):** Clean signal handling with SIGINT/SIGTERM, state save on interrupt, and force-exit on second signal.

3. **Safe Resume Point Tracking (main.go:335-395):** The `ProgressTracker` only advances the resume point through contiguous completed batches, preventing data loss from out-of-order batch completion.

4. **Safety Checks in Cleanup Tool (seaweed-cleanup-go/main.go:612-840):** Both delete and migrate operations have pre-flight safety checks that verify the operation works correctly on a test entry before proceeding.

5. **Path Prefix Handling (main.go:180-192):** Per-bucket table migration correctly transforms relative paths to absolute paths, with dedicated flag and validation.

6. **Context-Aware Sleeping (main.go:218-227):** The `sleepCtx` function properly respects cancellation during backoff waits.

7. **TLS Support (main.go:470-484, 550-561):** TiKV TLS is properly supported with CA, cert, key, and CN verification.

8. **Division by Zero Fix (audit.go:995-1001):** The audit tool checks `finalChecked > 0` before computing percentages.

---

## DEPENDENCY NOTES

- `github.com/tikv/client-go/v2 v2.0.7` — Released 2023. Consider checking for updates.
- `github.com/lib/pq v1.10.9` — Stable, well-maintained.
- `golang.org/x/net v0.8.0`, `golang.org/x/sys v0.6.0` — From early 2023. May have known CVEs. Run `govulncheck` to verify.
- `google.golang.org/grpc v1.54.0` — From 2023. Several security patches have been released since.

---

## SUMMARY

The core migration logic in `main.go` is well-engineered with solid retry logic, safe resume points, graceful shutdown, and SQL injection protection. The audit tool provides thorough field-level protobuf diffing. The cleanup tool has good safety checks.

The major weaknesses are:
1. **Structural**: Massive code duplication across files in a single-package multi-binary anti-pattern
2. **Security**: Insecure gRPC transport, no delete confirmation on `count_keys`, world-readable state files
3. **Quality**: Zero tests, binary artifacts in repo, stale Python cache, no build automation
4. **Reliability**: Missing context propagation in HTTP calls, linear backoff mislabeled as exponential, unused dry-run flag
