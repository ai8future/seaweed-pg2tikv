Date Created: 2026-03-21T23:00:00Z
TOTAL_SCORE: 65/100

# seaweed-pg2tikv Code Audit Report

**Agent:** Claude Code (Claude:Opus 4.6)
**Scope:** All Go source files in seaweed-pg2tikv and seaweed-cleanup-go
**Files analyzed:** main.go, audit.go, count_keys.go, seaweed-cleanup-go/main.go

---

## Executive Summary

The project is a well-structured data migration tool suite for moving SeaweedFS filer metadata from PostgreSQL to TiKV. The core migration logic is solid, with proper retry handling, graceful shutdown, and safe progress tracking. However, several structural and code quality issues bring the score down significantly:

- **Critical build structure problem**: Three binaries share a directory with duplicate symbols
- **Pervasive code duplication** between main.go and audit.go (~150 lines)
- **Version constant drift** across files
- **Dead code** in seaweed-cleanup-go
- **Suppressed errors** in HTTP request creation

---

## Issues Found

### CRITICAL: Build Structure — Three `main()` Functions with Duplicate Symbols (Severity: Critical)

**Files:** main.go, audit.go, count_keys.go

All three files declare `package main` in the same directory and define overlapping symbols:

| Symbol | main.go | audit.go | count_keys.go |
|--------|---------|----------|---------------|
| `const Version` | "1.2.0" | "1.2.2" | "1.2.0" |
| `const MinInt64` | Yes | Yes | — |
| `var validTableName` | Yes | Yes | — |
| `type FilerConfig` | Yes | Yes | — |
| `type Postgres2Config` | Yes | Yes | — |
| `type TiKVConfig` | Yes | Yes | — |
| `type Entry` | Yes | Yes | — |
| `func hashToBytes` | Yes | Yes | — |
| `func generateTiKVKey` | Yes | Yes | — |
| `func applyPathPrefix` | Yes | Yes | — |
| `func main()` | Yes | Yes | Yes |
| `var showVersion` | Yes | Yes | Yes |
| `var table` | Yes | Yes | — |

**Impact:** `go build ./...`, `go vet ./...`, and `go test ./...` all fail from the project root. Each binary must be built individually (e.g., `go build -o pg2tikv main.go`). This is non-standard, fragile, and prevents standard Go toolchain use.

**Recommended fix:** Restructure into a `cmd/` layout:

```
cmd/
  pg2tikv/main.go
  pg2tikv-audit/main.go
  count-keys/main.go
internal/
  config/config.go       # shared FilerConfig, Postgres2Config, TiKVConfig
  tikv/keys.go           # shared hashToBytes, generateTiKVKey, applyPathPrefix
```

```diff
--- /dev/null
+++ b/internal/config/config.go
@@ -0,0 +1,85 @@
+package config
+
+import (
+	"fmt"
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

```diff
--- /dev/null
+++ b/internal/tikv/keys.go
@@ -0,0 +1,34 @@
+package tikv
+
+import (
+	"crypto/sha1"
+	"io"
+)
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
+	if prefix == "" {
+		return directory
+	}
+	if directory == "/" {
+		return prefix
+	}
+	return prefix + directory
+}
```

---

### HIGH: Version Constant Drift (Severity: High)

**Files:** main.go:28, audit.go:29, count_keys.go:20

The Version constants are out of sync:
- `main.go`: `"1.2.0"`
- `audit.go`: `"1.2.2"`
- `count_keys.go`: `"1.2.0"`

Meanwhile `VERSION` file says `1.2.6` and `CHANGELOG.md` records up to `1.2.6`.

**Impact:** Users running `--version` get stale version numbers. Debugging production issues becomes harder when you can't trust the reported version.

```diff
--- a/main.go
+++ b/main.go
@@ -26,7 +26,7 @@
-const Version = "1.2.0"
+const Version = "1.2.6"
```

```diff
--- a/audit.go
+++ b/audit.go
@@ -27,7 +27,7 @@
-const Version = "1.2.2"
+const Version = "1.2.6"
```

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -18,7 +18,7 @@
-const Version = "1.2.0"
+const Version = "1.2.6"
```

---

### MEDIUM: Dead Code in seaweed-cleanup-go (Severity: Medium)

**File:** seaweed-cleanup-go/main.go

Several variables are assigned but never used meaningfully:

1. **`lastRateCount`** (lines 1004, 1051, 1058): Assigned at line 1051, immediately discarded with `_ = lastRateCount` at line 1058.

2. **`d` (dupes count)** (line 1053, 1062): Loaded from atomic counter but discarded with `_ = d` at line 1062. It was likely intended for the progress output but never included.

3. **`dryRun`** (line 882): Set to `true` when neither `--delete` nor `--migrate` is specified, then immediately discarded with `_ = *dryRun`. The variable serves no functional purpose.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -879,9 +879,6 @@
 	if !*doDelete && !*doMigrate {
 		*dryRun = true
 	}
-	_ = *dryRun // used implicitly
```

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -1001,8 +1001,6 @@
 	// Rate tracking
 	startTime := time.Now()
-	var lastRateTime time.Time
-	var lastRateCount int64
+	var lastRateTime time.Time
```

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -1048,9 +1048,7 @@
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
```

---

### MEDIUM: Suppressed Errors in HTTP Request Creation (Severity: Medium)

**File:** seaweed-cleanup-go/main.go

Multiple `http.NewRequest` calls ignore the error return:

- **Line 686:** `req, _ := http.NewRequest("GET", testURL, nil)` — in `runDeleteSafetyCheck`
- **Line 804:** `readReq, _ := http.NewRequest("GET", testURL, nil)` — in `runMigrateSafetyCheck`
- **Line 816:** `delReq, _ := http.NewRequest("DELETE", delURL, nil)` — in `runMigrateSafetyCheck`
- **Line 827:** `readReq2, _ := http.NewRequest("GET", testURL, nil)` — in `runMigrateSafetyCheck`

While `http.NewRequest` rarely fails (only on invalid method or URL), suppressing errors in safety-check functions is particularly risky since these functions are designed to prevent data loss.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -684,7 +684,10 @@
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

(Similar pattern for lines 804, 816, 827)

---

### MEDIUM: Non-Cancellable Delete Operation in count_keys.go (Severity: Medium)

**File:** count_keys.go:158

The `deleteAllKeys` function uses `context.Background()` with no signal handling. If a user presses Ctrl+C during a `DeleteRange` operation, the operation cannot be cancelled.

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -155,7 +155,13 @@

 func deleteAllKeys(addrs []string) {
-	ctx := context.Background()
+	ctx, cancel := context.WithCancel(context.Background())
+	defer cancel()
+
+	sigChan := make(chan os.Signal, 1)
+	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
+	go func() {
+		<-sigChan
+		log.Println("Interrupt received, cancelling delete...")
+		cancel()
+	}()
+
 	prefixBytes := []byte(*prefix)
```

This would also require adding `"os/signal"` and `"syscall"` to the imports.

---

### LOW: No io.ReadAll Size Limits (Severity: Low)

**File:** seaweed-cleanup-go/main.go

Multiple calls to `io.ReadAll(resp.Body)` (lines 130, 174, 694, 811, 834) read the entire response into memory without any size limit. A misbehaving or compromised filer could return an extremely large response causing OOM.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -128,7 +128,7 @@
 	}

-	body, err := io.ReadAll(resp.Body)
+	body, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB max
 	if err != nil {
 		return nil, err
 	}
```

---

### LOW: Insecure-Only gRPC in seaweed-cleanup-go (Severity: Low)

**File:** seaweed-cleanup-go/main.go:935-936

The gRPC connection always uses `insecure.NewCredentials()`. There is no flag for TLS, unlike the main pg2tikv tool which fully supports TLS for TiKV connections. If the filer gRPC endpoint requires TLS, the cleanup tool cannot connect.

---

### INFO: Code Duplication Between main.go and audit.go (~150 Lines)

The following code blocks are fully duplicated:
- `FilerConfig`, `Postgres2Config`, `TiKVConfig` structs and methods (~80 lines)
- `hashToBytes`, `generateTiKVKey`, `applyPathPrefix` functions (~30 lines)
- `Entry` struct (~6 lines)
- TLS validation logic (~15 lines)
- Table name validation regex (~1 line)
- Path prefix validation (~4 lines)

This duplication increases maintenance burden and creates version drift risk (as evidenced by the Version constant issue above).

---

### INFO: seaweed-cleanup-go — `rand.Shuffle` Determinism Note

**File:** seaweed-cleanup-go/main.go:1110

`rand.Shuffle` is called without explicit seeding. In Go 1.20+, `math/rand` auto-seeds from the runtime, so this works correctly with Go 1.21 (specified in the seaweed-cleanup-go go.mod, which I didn't check but the root go.mod says 1.21). If the module were ever downgraded below 1.20, the shuffle would produce the same order every run.

---

## What's Done Well

1. **SQL injection prevention**: Table names validated with regex + parameterized queries for values
2. **TLS certificate validation**: Paths validated with `os.Stat` before use
3. **Graceful shutdown in main.go**: Proper signal handling with two-interrupt force-exit pattern
4. **Contiguous batch progress tracking**: The `ProgressTracker` correctly only advances the resume point on contiguous successful batches, preventing data loss on partial failures
5. **Retry logic**: Exponential backoff with context-aware sleeping (`sleepCtx`)
6. **Safety checks in seaweed-cleanup-go**: Pre-flight verification before destructive operations (delete/migrate)
7. **Pagination with resume**: Both migration and audit support resuming from saved state
8. **Per-bucket path prefix handling**: Clean solution to the relative-vs-absolute path issue
9. **Protobuf field-level diff analysis**: The audit tool provides detailed mismatch analysis

---

## Score Breakdown

| Category | Deduction | Reason |
|----------|-----------|--------|
| Build structure | -15 | Three binaries in one directory, can't `go build ./...` |
| Version drift | -5 | Three different version strings across files |
| Code duplication | -5 | ~150 lines duplicated between main.go and audit.go |
| Dead code | -3 | Unused variables in seaweed-cleanup-go |
| Suppressed errors | -2 | `http.NewRequest` errors ignored in safety checks |
| No read limits | -2 | `io.ReadAll` without size bounds |
| Non-cancellable delete | -2 | count_keys.go delete has no signal handling |
| Insecure-only gRPC | -1 | No TLS option for cleanup tool gRPC |

**TOTAL_SCORE: 65/100**
