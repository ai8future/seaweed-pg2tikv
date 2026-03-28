Date Created: 2026-03-21T23:00:00Z
TOTAL_SCORE: 68/100

Agent: Claude Code (Claude:Opus 4.6)

---

# seaweed-pg2tikv Combined Analysis Report

## Codebase Overview

This codebase contains three Go binaries for SeaweedFS metadata migration and management:

- **seaweed-pg2tikv** (`main.go`, ~812 lines) - PostgreSQL to TiKV migration tool
- **seaweed-pg2tikv-audit** (`audit.go`, ~1050 lines) - Migration verification tool
- **seaweed-count-keys** (`count_keys.go`, ~189 lines) - TiKV key counter/deleter
- **seaweed-cleanup** (`seaweed-cleanup-go/main.go`, ~1247 lines) - Orphan cleanup via HTTP/gRPC

**Key Strengths:** Solid worker/batch patterns, good state management with safe resume points, SQL injection protection, TLS path validation, graceful shutdown handling.

**Key Weaknesses:** Zero test coverage, massive code duplication between binaries, a few silent error-swallowing patterns, `--dry-run` flag is non-functional, version constants drift across files.

---

## Section 1: AUDIT - Security and Code Quality Issues

### 1.1 [MEDIUM] gRPC connection uses insecure credentials with no TLS option

**File:** `seaweed-cleanup-go/main.go:935-937`

The gRPC client always uses `insecure.NewCredentials()`. Unlike the pg2tikv tools which support TLS configuration, the cleanup tool has no TLS option for gRPC communication with the filer.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -24,6 +24,7 @@ import (

 	"google.golang.org/grpc"
 	"google.golang.org/grpc/credentials/insecure"
+	"google.golang.org/grpc/credentials"
+	"crypto/tls"
+	"crypto/x509"
 )

@@ -843,6 +844,8 @@ func main() {
 	filerURL := flag.String("filer", "", "Filer URL, e.g. http://localhost:8888")
 	filerGRPC := flag.String("filer-grpc", "", "Filer gRPC address (default: derived from -filer, port+10000)")
+	grpcCA := flag.String("grpc-ca", "", "CA certificate for gRPC TLS (optional)")
+	grpcCert := flag.String("grpc-cert", "", "Client certificate for gRPC TLS (optional)")
+	grpcKey := flag.String("grpc-key", "", "Client key for gRPC TLS (optional)")
 	doDelete := flag.Bool("delete", false, "Delete confirmed duplicate root metadata")

@@ -932,9 +935,26 @@ func main() {
 	var grpcClient pb.SeaweedFilerClient
 	if *doMigrate {
-		conn, err := grpc.NewClient(grpcAddr,
-			grpc.WithTransportCredentials(insecure.NewCredentials()),
-		)
+		var dialOpt grpc.DialOption
+		if *grpcCA != "" {
+			caCert, err := os.ReadFile(*grpcCA)
+			if err != nil {
+				fmt.Fprintf(os.Stderr, "Failed to read CA cert: %v\n", err)
+				os.Exit(1)
+			}
+			pool := x509.NewCertPool()
+			pool.AppendCertsFromPEM(caCert)
+			tlsCfg := &tls.Config{RootCAs: pool}
+			if *grpcCert != "" && *grpcKey != "" {
+				cert, err := tls.LoadX509KeyPair(*grpcCert, *grpcKey)
+				if err != nil {
+					fmt.Fprintf(os.Stderr, "Failed to load client cert: %v\n", err)
+					os.Exit(1)
+				}
+				tlsCfg.Certificates = []tls.Certificate{cert}
+			}
+			dialOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg))
+		} else {
+			dialOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
+		}
+		conn, err := grpc.NewClient(grpcAddr, dialOpt)
 		if err != nil {
```

### 1.2 [LOW] HTTP response bodies read without size limits

**File:** `seaweed-cleanup-go/main.go:131,174`

`io.ReadAll(resp.Body)` without limiting the response size. A misbehaving filer could return extremely large responses and exhaust memory.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -128,7 +128,7 @@ func filerGet(filerURL, path string, params map[string]string) (*FilerListRespon
 		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
 	}

-	body, err := io.ReadAll(resp.Body)
+	body, err := io.ReadAll(io.LimitReader(resp.Body, 50*1024*1024)) // 50MB max
 	if err != nil {
 		return nil, err
 	}
@@ -171,7 +171,7 @@ func getFileChunks(filerURL, filePath string) ([]string, error) {
 		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
 	}

-	body, err := io.ReadAll(resp.Body)
+	body, err := io.ReadAll(io.LimitReader(resp.Body, 50*1024*1024)) // 50MB max
 	if err != nil {
 		return nil, err
 	}
```

### 1.3 [LOW] State file written with world-readable permissions

**File:** `main.go:152`

State file is written with 0644 permissions. On shared systems, other users could read the migration state which includes table names and progress.

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

### 1.4 [LOW] Postgres connection string values not escaped

**File:** `main.go:56-74`, `audit.go:56-74`

The `ConnectionString()` method interpolates password and username directly into the DSN. If these contain spaces or special characters, the connection will fail or parse incorrectly.

```diff
--- a/main.go
+++ b/main.go
@@ -54,9 +54,19 @@ type Postgres2Config struct {
 }

+func escapeConnValue(v string) string {
+	if v == "" {
+		return "''"
+	}
+	if !strings.ContainsAny(v, " '\\") {
+		return v
+	}
+	return "'" + strings.ReplaceAll(strings.ReplaceAll(v, "\\", "\\\\"), "'", "\\'") + "'"
+}
+
 func (c *Postgres2Config) ConnectionString() string {
 	connStr := fmt.Sprintf(
-		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
-		c.Hostname, c.Port, c.Username, c.Password, c.Database, c.SSLMode,
+		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
+		escapeConnValue(c.Hostname), c.Port, escapeConnValue(c.Username),
+		escapeConnValue(c.Password), escapeConnValue(c.Database), escapeConnValue(c.SSLMode),
 	)
```

---

## Section 2: TESTS - Proposed Unit Tests

### 2.1 Tests for key generation functions (main.go)

```diff
--- /dev/null
+++ b/main_test.go
@@ -0,0 +1,133 @@
+package main
+
+import (
+	"bytes"
+	"encoding/json"
+	"os"
+	"path/filepath"
+	"testing"
+)
+
+func TestHashToBytes(t *testing.T) {
+	// Same input should always produce same output
+	a := hashToBytes("/some/path")
+	b := hashToBytes("/some/path")
+	if !bytes.Equal(a, b) {
+		t.Error("hashToBytes not deterministic")
+	}
+
+	// Different inputs should produce different outputs
+	c := hashToBytes("/other/path")
+	if bytes.Equal(a, c) {
+		t.Error("hashToBytes collision on different inputs")
+	}
+
+	// SHA1 output should be 20 bytes
+	if len(a) != 20 {
+		t.Errorf("expected 20 bytes, got %d", len(a))
+	}
+}
+
+func TestGenerateTiKVKey(t *testing.T) {
+	tests := []struct {
+		name      string
+		prefix    []byte
+		directory string
+		filename  string
+	}{
+		{"no prefix", nil, "/dir", "file.txt"},
+		{"empty prefix", []byte{}, "/dir", "file.txt"},
+		{"with prefix", []byte("pfx:"), "/dir", "file.txt"},
+		{"root dir", []byte("pfx:"), "/", "file.txt"},
+	}
+
+	for _, tt := range tests {
+		t.Run(tt.name, func(t *testing.T) {
+			key := generateTiKVKey(tt.prefix, tt.directory, tt.filename)
+			if len(key) == 0 {
+				t.Error("generated empty key")
+			}
+			if len(tt.prefix) > 0 && !bytes.HasPrefix(key, tt.prefix) {
+				t.Error("key does not start with prefix")
+			}
+			// Key should end with the filename
+			if !bytes.HasSuffix(key, []byte(tt.filename)) {
+				t.Error("key does not end with filename")
+			}
+		})
+	}
+}
+
+func TestApplyPathPrefix(t *testing.T) {
+	tests := []struct {
+		prefix, directory, expected string
+	}{
+		{"", "/some/path", "/some/path"},
+		{"", "/", "/"},
+		{"/buckets/my-bucket", "/", "/buckets/my-bucket"},
+		{"/buckets/my-bucket", "/subdir", "/buckets/my-bucket/subdir"},
+		{"/buckets/my-bucket", "/a/b/c", "/buckets/my-bucket/a/b/c"},
+	}
+
+	for _, tt := range tests {
+		result := applyPathPrefix(tt.prefix, tt.directory)
+		if result != tt.expected {
+			t.Errorf("applyPathPrefix(%q, %q) = %q, want %q",
+				tt.prefix, tt.directory, result, tt.expected)
+		}
+	}
+}
+
+func TestLoadSaveState(t *testing.T) {
+	dir := t.TempDir()
+	stateFile := filepath.Join(dir, "test_state.json")
+
+	// Load non-existent file should return defaults
+	state, err := loadState(stateFile)
+	if err != nil {
+		t.Fatalf("loadState on non-existent: %v", err)
+	}
+	if state.LastDirhash != MinInt64 {
+		t.Errorf("expected MinInt64 dirhash, got %d", state.LastDirhash)
+	}
+
+	// Save and reload
+	state.Table = "test_table"
+	state.LastDirhash = 42
+	state.LastName = "test.txt"
+	state.TotalMigrated = 100
+	if err := saveState(stateFile, state); err != nil {
+		t.Fatalf("saveState: %v", err)
+	}
+
+	loaded, err := loadState(stateFile)
+	if err != nil {
+		t.Fatalf("loadState: %v", err)
+	}
+	if loaded.Table != "test_table" || loaded.LastDirhash != 42 || loaded.LastName != "test.txt" || loaded.TotalMigrated != 100 {
+		t.Errorf("loaded state mismatch: %+v", loaded)
+	}
+
+	// Load corrupted file
+	os.WriteFile(stateFile, []byte("not json"), 0600)
+	_, err = loadState(stateFile)
+	if err == nil {
+		t.Error("expected error on corrupted state file")
+	}
+}
+
+func TestLoadStateOldFormat(t *testing.T) {
+	dir := t.TempDir()
+	stateFile := filepath.Join(dir, "old_state.json")
+
+	// Old state files had dirhash=0 meaning "start from beginning"
+	oldState := State{LastDirhash: 0, LastName: "", TotalMigrated: 0}
+	data, _ := json.Marshal(oldState)
+	os.WriteFile(stateFile, data, 0600)
+
+	state, err := loadState(stateFile)
+	if err != nil {
+		t.Fatal(err)
+	}
+	if state.LastDirhash != MinInt64 {
+		t.Errorf("old format migration failed: got %d, want MinInt64", state.LastDirhash)
+	}
+}
+
+func TestProgressTracker(t *testing.T) {
+	pt := NewProgressTracker()
+
+	// No results yet
+	_, ok := pt.GetSafeResumePoint()
+	if ok {
+		t.Error("expected no safe resume point initially")
+	}
+
+	// Record seq 0 success
+	pt.RecordResult(BatchResult{SeqNum: 0, LastEntry: Entry{Dirhash: 10, Name: "a"}, Success: true, Count: 5})
+	entry, ok := pt.GetSafeResumePoint()
+	if !ok || entry.Dirhash != 10 {
+		t.Errorf("expected dirhash 10, got %v, ok=%v", entry, ok)
+	}
+
+	// Record seq 2 (gap at 1) - should not advance
+	pt.RecordResult(BatchResult{SeqNum: 2, LastEntry: Entry{Dirhash: 30, Name: "c"}, Success: true, Count: 5})
+	entry, ok = pt.GetSafeResumePoint()
+	if !ok || entry.Dirhash != 10 {
+		t.Errorf("should still be at dirhash 10, got %v", entry)
+	}
+
+	// Fill the gap
+	pt.RecordResult(BatchResult{SeqNum: 1, LastEntry: Entry{Dirhash: 20, Name: "b"}, Success: true, Count: 5})
+	entry, ok = pt.GetSafeResumePoint()
+	if !ok || entry.Dirhash != 30 {
+		t.Errorf("expected dirhash 30 after filling gap, got %v", entry)
+	}
+
+	success, fail, hasFail := pt.GetCounts()
+	if success != 15 || fail != 0 || hasFail {
+		t.Errorf("counts wrong: success=%d fail=%d hasFail=%v", success, fail, hasFail)
+	}
+}
```

### 2.2 Tests for count_keys.go

```diff
--- /dev/null
+++ b/count_keys_test.go
@@ -0,0 +1,41 @@
+package main
+
+import (
+	"bytes"
+	"testing"
+)
+
+func TestPrefixEndKey(t *testing.T) {
+	tests := []struct {
+		name     string
+		prefix   []byte
+		expected []byte
+	}{
+		{"simple", []byte("abc"), []byte("abd")},
+		{"single byte", []byte{0x00}, []byte{0x01}},
+		{"trailing ff", []byte{0x01, 0xff}, []byte{0x02}},
+		{"all ff", []byte{0xff, 0xff}, nil},
+		{"empty", []byte{}, nil},
+	}
+
+	for _, tt := range tests {
+		t.Run(tt.name, func(t *testing.T) {
+			result := prefixEndKey(tt.prefix)
+			if !bytes.Equal(result, tt.expected) {
+				t.Errorf("prefixEndKey(%x) = %x, want %x", tt.prefix, result, tt.expected)
+			}
+		})
+	}
+}
```

### 2.3 Tests for seaweed-cleanup-go utility functions

```diff
--- /dev/null
+++ b/seaweed-cleanup-go/main_test.go
@@ -0,0 +1,71 @@
+package main
+
+import (
+	"testing"
+)
+
+func TestGetFileExt(t *testing.T) {
+	tests := []struct {
+		fname, expected string
+	}{
+		{"file.heic", ".heic"},
+		{"file.json.gz", ".json.gz"},
+		{"file.lp_newspaper", ".lp_newspaper"},
+		{"file.jp2", ".jp2"},
+		{"file.parquet", ".parquet"},
+		{"file.txt", ".txt"},
+		{"noext", ""},
+		{"multi.dots.heic", ".heic"},
+		{"archive.tar.json.gz", ".json.gz"},
+	}
+
+	for _, tt := range tests {
+		t.Run(tt.fname, func(t *testing.T) {
+			result := getFileExt(tt.fname)
+			if result != tt.expected {
+				t.Errorf("getFileExt(%q) = %q, want %q", tt.fname, result, tt.expected)
+			}
+		})
+	}
+}
+
+func TestChunksEqual(t *testing.T) {
+	if !chunksEqual([]string{"a", "b"}, []string{"a", "b"}) {
+		t.Error("identical slices should be equal")
+	}
+	if chunksEqual([]string{"a", "b"}, []string{"a", "c"}) {
+		t.Error("different slices should not be equal")
+	}
+	if chunksEqual([]string{"a"}, []string{"a", "b"}) {
+		t.Error("different length slices should not be equal")
+	}
+	if !chunksEqual(nil, nil) {
+		t.Error("nil slices should be equal")
+	}
+}
+
+func TestShardHash(t *testing.T) {
+	// Deterministic
+	a := shardHash("test")
+	b := shardHash("test")
+	if a != b {
+		t.Error("shardHash not deterministic")
+	}
+
+	// Different inputs should generally differ
+	c := shardHash("other")
+	if a == c {
+		t.Error("unexpected collision")
+	}
+}
+
+func TestIsSmartdataFile(t *testing.T) {
+	if !isSmartdataFile("s3.sdc.something.json.gz") {
+		t.Error("should match smartdata pattern")
+	}
+	if isSmartdataFile("other.json.gz") {
+		t.Error("should not match non-smartdata")
+	}
+	if isSmartdataFile("s3.sdc.something.txt") {
+		t.Error("should not match wrong extension")
+	}
+}
+
+func TestEncodePath(t *testing.T) {
+	result := encodePath("http://localhost:8888", "/path/with spaces/file.txt")
+	expected := "http://localhost:8888/path/with%20spaces/file.txt"
+	if result != expected {
+		t.Errorf("encodePath = %q, want %q", result, expected)
+	}
+}
```

---

## Section 3: FIXES - Bugs, Issues, and Code Smells

### 3.1 [HIGH] `--dry-run` flag is completely non-functional in seaweed-cleanup

**File:** `seaweed-cleanup-go/main.go:881-882`

The `--dry-run` flag is accepted but never checked. Setting `--dry-run --delete` will still perform deletions. The flag is silently ignored via `_ = *dryRun`.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -878,8 +878,12 @@ func main() {

 	if !*doDelete && !*doMigrate {
 		*dryRun = true
 	}
-	_ = *dryRun // used implicitly
+	if *dryRun {
+		// Dry run overrides delete and migrate
+		*doDelete = false
+		*doMigrate = false
+	}
```

### 3.2 [MEDIUM] Comment says "exponential backoff" but implementation is linear

**File:** `main.go:280-281`

The backoff formula `(retry+1) * base` is linear, not exponential. This means retries happen faster than intended on later attempts.

```diff
--- a/main.go
+++ b/main.go
@@ -258,7 +258,7 @@ func commitBatch(ctx context.Context, tikvClient *txnkv.Client, prefix []byte, e

 		// Exponential backoff
-		backoff := time.Duration(retry+1) * time.Duration(retryBaseDelayMs) * time.Millisecond
+		backoff := time.Duration(1<<uint(retry)) * time.Duration(retryBaseDelayMs) * time.Millisecond
 		if retry < maxRetryCount-1 {
```

Apply the same fix at line 261 for the set-error retry:

```diff
--- a/main.go
+++ b/main.go
@@ -259,7 +259,7 @@ func commitBatch(ctx context.Context, tikvClient *txnkv.Client, prefix []byte, e
 		if setErr != nil {
 			txn.Rollback()
-			backoff := time.Duration(retry+1) * time.Duration(retryBaseDelayMs) * time.Millisecond
+			backoff := time.Duration(1<<uint(retry)) * time.Duration(retryBaseDelayMs) * time.Millisecond
 			if retry < maxRetryCount-1 {
```

### 3.3 [MEDIUM] ProgressTracker completedSeqs map grows without bound

**File:** `main.go:337-342`

The `completedSeqs` map grows as batches complete but entries are never removed after being incorporated into the contiguous sequence. For very large migrations (millions of batches), this wastes significant memory.

```diff
--- a/main.go
+++ b/main.go
@@ -359,6 +359,10 @@ func (pt *ProgressTracker) RecordResult(result BatchResult) {
 		for {
 			nextSeq := pt.highestContig + 1
 			if _, ok := pt.completedSeqs[nextSeq]; ok {
+				// Clean up entries that have been counted as contiguous
+				delete(pt.completedSeqs, nextSeq)
 				pt.highestContig = nextSeq
 			} else if pt.failedSeqs[nextSeq] {
 				// Stop at failures - don't advance past them
```

Wait — we need to preserve the last entry for `GetSafeResumePoint`. Updated fix:

```diff
--- a/main.go
+++ b/main.go
@@ -356,9 +356,13 @@ func (pt *ProgressTracker) RecordResult(result BatchResult) {
 	if result.Success {
 		pt.completedSeqs[result.SeqNum] = result.LastEntry
 		pt.successCount += int64(result.Count)
+		var lastContigEntry Entry

 		// Update highest contiguous sequence
 		for {
 			nextSeq := pt.highestContig + 1
-			if _, ok := pt.completedSeqs[nextSeq]; ok {
+			if entry, ok := pt.completedSeqs[nextSeq]; ok {
+				lastContigEntry = entry
+				delete(pt.completedSeqs, nextSeq)
 				pt.highestContig = nextSeq
 			} else if pt.failedSeqs[nextSeq] {
@@ -367,6 +371,10 @@ func (pt *ProgressTracker) RecordResult(result BatchResult) {
 				break
 			}
 		}
+		// Store the last contiguous entry for resume point lookup
+		if pt.highestContig >= 0 {
+			pt.lastContigEntry = lastContigEntry
+		}
```

This requires adding `lastContigEntry Entry` to the struct and updating `GetSafeResumePoint` to return `pt.lastContigEntry`.

### 3.4 [MEDIUM] Scan errors silently skip entries without counting them

**File:** `main.go:712-714`, `audit.go:969-972`

When `rows.Scan` fails, the entry is silently skipped with `continue`. The entry is never counted in any progress metric, making it invisible to the operator.

```diff
--- a/main.go
+++ b/main.go
@@ -709,8 +709,10 @@ func main() {
+		var scanErrors int64
 		for rows.Next() {
 			var e Entry
 			if err := rows.Scan(&e.Dirhash, &e.Directory, &e.Name, &e.Meta); err != nil {
 				log.Printf("Scan error: %v", err)
+				scanErrors++
 				continue
 			}
@@ -802,6 +804,9 @@ func main() {
 	log.Printf("Rows read from Postgres: %d", rowCount)
+	if scanErrors > 0 {
+		log.Printf("WARNING: %d rows failed to scan and were skipped", scanErrors)
+	}
 	log.Printf("Successfully written to TiKV: %d", finalSuccess)
```

### 3.5 [MEDIUM] audit.go entryChan send blocks without context check

**File:** `audit.go:974`

The blocking send `entryChan <- e` does not check for context cancellation. If the user presses Ctrl+C while the channel is full, the goroutine hangs until a worker drains the channel.

```diff
--- a/audit.go
+++ b/audit.go
@@ -971,7 +971,12 @@ func main() {
 		}
 		rowCount++
-		entryChan <- e
+		select {
+		case entryChan <- e:
+		case <-ctx.Done():
+			break
+		}
 	}
```

### 3.6 [LOW] Version constants drift across files

**File:** `main.go:28`, `audit.go:29`, `count_keys.go:20`

Each file has its own `const Version` that is updated independently. Currently main.go and count_keys.go are at "1.2.0" while audit.go is at "1.2.2". This makes it unclear which version is actually deployed.

```diff
--- a/main.go
+++ b/main.go
@@ -26,7 +26,7 @@ import (
 )

-const Version = "1.2.0"
+// Version is set here; audit.go and count_keys.go have their own Version constants.
+// IMPORTANT: When bumping version, update all three files.
+const Version = "1.2.6"
```

Note: The ideal fix is extracting this to a shared file, but since these are compiled as separate binaries from the same package, that would require restructuring. See Refactor section.

### 3.7 [LOW] Errors from http.NewRequest silently discarded

**File:** `seaweed-cleanup-go/main.go:686,804,817`

Multiple places use `req, _ := http.NewRequest(...)` discarding the error. While `http.NewRequest` almost never fails with valid method names, this masks potential issues.

```diff
--- a/seaweed-cleanup-go/main.go
+++ b/seaweed-cleanup-go/main.go
@@ -683,7 +683,10 @@ func runDeleteSafetyCheck(filerURL, startAfter string) bool {
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

---

## Section 4: REFACTOR - Opportunities to Improve Code Quality

### 4.1 [HIGH] Massive code duplication between main.go and audit.go

The following types and functions are copy-pasted identically between `main.go` and `audit.go`:
- `FilerConfig`, `Postgres2Config`, `TiKVConfig` structs
- `ConnectionString()`, `PDAddrList()` methods
- `hashToBytes()`, `generateTiKVKey()`, `applyPathPrefix()` functions
- `Entry` struct
- `validTableName` regex
- `MinInt64` constant
- TLS validation code blocks

**Recommendation:** Extract shared code into a `shared.go` file (which can be compiled with either binary via build tags) or restructure into a proper multi-binary layout using `cmd/` directories:

```
cmd/pg2tikv/main.go
cmd/audit/main.go
cmd/count-keys/main.go
internal/config/config.go    (FilerConfig, Postgres2Config, TiKVConfig)
internal/tikv/key.go          (hashToBytes, generateTiKVKey, applyPathPrefix)
```

### 4.2 [MEDIUM] Retry logic duplicated throughout seaweed-cleanup-go

The pattern of retry-with-sleep appears in at least 6 places in `seaweed-cleanup-go/main.go` (lines 218-226, 310-317, 357-367, 376-387, 488-496, 514-524). Each instance has the same structure: loop 3 times, sleep `(attempt+1) * time.Second`, check error.

**Recommendation:** Extract a generic retry helper:

```go
func withRetry(ctx context.Context, maxAttempts int, fn func() error) error {
    var err error
    for attempt := 0; attempt < maxAttempts; attempt++ {
        if err = fn(); err == nil {
            return nil
        }
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(time.Duration(attempt+1) * time.Second):
        }
    }
    return err
}
```

### 4.3 [MEDIUM] seaweed-cleanup-go uses deprecated `math/rand` global functions

**File:** `seaweed-cleanup-go/main.go:1110`

`rand.Shuffle` uses the global `math/rand` source. Since Go 1.20, the global source is auto-seeded, but for Go 1.21+, the `math/rand/v2` package is recommended. Not a bug with Go 1.21 (per go.mod), but worth modernizing.

### 4.4 [LOW] Unused variable `lastRateCount`

**File:** `seaweed-cleanup-go/main.go:1004,1051,1058`

`lastRateCount` is declared, assigned on line 1051, then immediately discarded with `_ = lastRateCount` on line 1058. The variable serves no purpose.

### 4.5 [LOW] Compiled binaries checked into git

The repository contains compiled Linux binaries:
- `seaweed-pg2tikv-linux-amd64`
- `seaweed-pg2tikv-audit-linux-amd64`
- `seaweed-count-keys-linux-amd64`
- `seaweed-cleanup-go/seaweed-cleanup`

**Recommendation:** Add these to `.gitignore` and use a CI pipeline or Makefile for building releases. Binary files bloat the repository and create merge conflicts.

### 4.6 [LOW] Python __pycache__ directory checked in

The directory `__pycache__/` with compiled Python protobuf stubs should be in `.gitignore`.

---

## Score Breakdown

| Category | Score | Max | Notes |
|----------|-------|-----|-------|
| Security | 14 | 20 | Good SQL injection protection, TLS validation. gRPC insecure-only in cleanup tool, unbounded reads. |
| Correctness | 17 | 25 | Core logic solid. dry-run flag broken, linear backoff mislabeled, scan errors silent, entryChan blocking. |
| Code Quality | 13 | 20 | Good patterns and error handling. Massive duplication, version drift, unused variables. |
| Test Coverage | 2 | 15 | Zero test files exist. Pure functions are highly testable. |
| Maintainability | 10 | 10 | Good changelog, comments, documentation. Clear structure within each file. |
| Architecture | 12 | 10 | Clean worker patterns, good state management, graceful shutdown. Loses points for binary layout. |
| **TOTAL** | **68** | **100** | |
