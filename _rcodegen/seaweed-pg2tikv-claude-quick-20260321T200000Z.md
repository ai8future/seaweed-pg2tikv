Date Created: 2026-03-21T20:00:00Z
TOTAL_SCORE: 62/100

Agent: Claude Code (Claude:Opus 4.6)

---

# seaweed-pg2tikv Combined Analysis Report

**Files analyzed:** `main.go` (812 lines), `audit.go` (1050 lines), `count_keys.go` (189 lines)
**Test coverage:** 0% — no test files exist

**Score Breakdown:**
- Functionality & correctness: 20/25 (works well, minor version inconsistencies)
- Security: 10/10 (SQL injection prevention, TLS, input validation)
- Code quality: 12/20 (clean within files, massive cross-file duplication)
- Architecture: 5/15 (three binaries in one dir, no shared packages, broken `go build`)
- Testing: 0/15 (zero tests)
- Maintainability: 10/15 (good comments, but duplication makes changes error-prone)
- Error handling: 5/5 (retries, graceful shutdown, proper cleanup)

---

## 1. AUDIT — Security and Code Quality Issues

### A1: State file written with world-readable permissions (Low)

`saveState` in `main.go:152` writes state files with `0644`. While state files don't contain credentials directly, the table name and progress data could be considered operational metadata that shouldn't be world-readable.

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

### A2: No validation on `workers` flag — negative/zero values accepted (Low)

`main.go:101` and `audit.go:103` accept any integer for `--workers`, including zero or negative, which would cause silent failures or panics.

```diff
--- a/main.go
+++ b/main.go
@@ -443,6 +443,9 @@ func main() {
 	if *partitionID < 0 || *partitionID >= *partitionMod {
 		log.Fatalf("Invalid partition-id %d: must be >= 0 and < partition-mod (%d)", *partitionID, *partitionMod)
 	}
+	if *workers <= 0 {
+		log.Fatalf("Invalid workers %d: must be > 0", *workers)
+	}

 	// Parse Postgres config
 	var pgConfig FilerConfig
```

```diff
--- a/audit.go
+++ b/audit.go
@@ -767,6 +767,9 @@ func main() {
 		*pathPrefix = strings.TrimRight(*pathPrefix, "/")
 	}

+	if *workers <= 0 {
+		log.Fatalf("Invalid workers %d: must be > 0", *workers)
+	}
+
 	// Validate table name to prevent SQL injection
 	if !validTableName.MatchString(*table) {
```

### A3: No validation on `batchSize` flag (Low)

`main.go:102` — zero or negative batch size would cause empty batches in an infinite loop.

```diff
--- a/main.go
+++ b/main.go
@@ -446,6 +446,9 @@ func main() {
 	if *workers <= 0 {
 		log.Fatalf("Invalid workers %d: must be > 0", *workers)
 	}
+	if *batchSize <= 0 {
+		log.Fatalf("Invalid batch size %d: must be > 0", *batchSize)
+	}

 	// Parse Postgres config
```

### A4: `count_keys.go` delete operation has no confirmation prompt (Medium)

The `--delete` flag on `count_keys.go:28` triggers an irreversible server-side `DeleteRange` with no confirmation. A typo in the prefix could wipe unrelated data.

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -91,6 +91,14 @@ func main() {

 	if *deleteKeys {
+		fmt.Printf("WARNING: About to delete ALL keys with prefix %q from TiKV at %s\n", *prefix, *pdAddrs)
+		fmt.Print("Type 'yes' to confirm: ")
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

### A5: `count_keys.go` iterator uses nil end key (Low)

`count_keys.go:116` — The txn iterator starts at prefix but passes `nil` as end key. This forces client-side prefix checking for every key beyond the range. Using `prefixEndKey` (already defined in the file) as the iterator bound would be more efficient.

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -113,7 +113,8 @@ func countAllKeys(addrs []string) {
 	}

 	prefixBytes := []byte(*prefix)
-	iter, err := txn.Iter(prefixBytes, nil)
+	endKey := prefixEndKey(prefixBytes)
+	iter, err := txn.Iter(prefixBytes, endKey)
 	if err != nil {
 		log.Fatalf("Failed to create iterator: %v", err)
 	}
```

---

## 2. TESTS — Proposed Unit Tests

No test files exist. The following pure functions can be unit tested without external dependencies.

### T1: Test `applyPathPrefix` (both main.go and audit.go)

```diff
--- /dev/null
+++ b/pathprefix_test.go
@@ -0,0 +1,47 @@
+//go:build ignore
+
+package main
+
+import "testing"
+
+func TestApplyPathPrefix(t *testing.T) {
+	tests := []struct {
+		prefix    string
+		directory string
+		want      string
+	}{
+		{"", "/some/dir", "/some/dir"},
+		{"", "/", "/"},
+		{"/buckets/my-bucket", "/", "/buckets/my-bucket"},
+		{"/buckets/my-bucket", "/subdir", "/buckets/my-bucket/subdir"},
+		{"/buckets/my-bucket", "/a/b/c", "/buckets/my-bucket/a/b/c"},
+	}
+	for _, tt := range tests {
+		got := applyPathPrefix(tt.prefix, tt.directory)
+		if got != tt.want {
+			t.Errorf("applyPathPrefix(%q, %q) = %q, want %q", tt.prefix, tt.directory, got, tt.want)
+		}
+	}
+}
```

### T2: Test `generateTiKVKey`

```diff
--- /dev/null
+++ b/keygen_test.go
@@ -0,0 +1,41 @@
+//go:build ignore
+
+package main
+
+import (
+	"bytes"
+	"testing"
+)
+
+func TestGenerateTiKVKey_NoPrefix(t *testing.T) {
+	key := generateTiKVKey(nil, "/some/dir", "file.txt")
+	if len(key) == 0 {
+		t.Fatal("expected non-empty key")
+	}
+	// Key should end with the filename
+	if !bytes.HasSuffix(key, []byte("file.txt")) {
+		t.Errorf("key should end with filename, got %x", key)
+	}
+}
+
+func TestGenerateTiKVKey_WithPrefix(t *testing.T) {
+	prefix := []byte("myprefix")
+	key := generateTiKVKey(prefix, "/dir", "name")
+	if !bytes.HasPrefix(key, prefix) {
+		t.Errorf("key should start with prefix, got %x", key)
+	}
+}
+
+func TestGenerateTiKVKey_Deterministic(t *testing.T) {
+	k1 := generateTiKVKey(nil, "/dir", "file")
+	k2 := generateTiKVKey(nil, "/dir", "file")
+	if !bytes.Equal(k1, k2) {
+		t.Error("same inputs should produce same key")
+	}
+}
```

### T3: Test `prefixEndKey` (count_keys.go)

```diff
--- /dev/null
+++ b/prefixend_test.go
@@ -0,0 +1,37 @@
+//go:build ignore
+
+package main
+
+import (
+	"bytes"
+	"testing"
+)
+
+func TestPrefixEndKey(t *testing.T) {
+	tests := []struct {
+		name   string
+		prefix []byte
+		want   []byte
+	}{
+		{"simple", []byte("abc"), []byte("abd")},
+		{"trailing_ff", []byte{0x01, 0xff}, []byte{0x02}},
+		{"all_ff", []byte{0xff, 0xff}, nil},
+		{"single_byte", []byte{0x00}, []byte{0x01}},
+	}
+	for _, tt := range tests {
+		got := prefixEndKey(tt.prefix)
+		if !bytes.Equal(got, tt.want) {
+			t.Errorf("%s: prefixEndKey(%x) = %x, want %x", tt.name, tt.prefix, got, tt.want)
+		}
+	}
+}
```

### T4: Test `loadState` / `saveState` round-trip

```diff
--- /dev/null
+++ b/state_test.go
@@ -0,0 +1,53 @@
+//go:build ignore
+
+package main
+
+import (
+	"os"
+	"testing"
+)
+
+func TestLoadState_NonExistent(t *testing.T) {
+	state, err := loadState("/tmp/nonexistent_state_test.json")
+	if err != nil {
+		t.Fatalf("unexpected error: %v", err)
+	}
+	if state.LastDirhash != MinInt64 {
+		t.Errorf("expected LastDirhash=%d, got %d", MinInt64, state.LastDirhash)
+	}
+}
+
+func TestSaveLoadState_RoundTrip(t *testing.T) {
+	f, err := os.CreateTemp("", "state_test_*.json")
+	if err != nil {
+		t.Fatal(err)
+	}
+	defer os.Remove(f.Name())
+	f.Close()
+
+	original := &State{
+		Table:         "test_table",
+		LastDirhash:   12345,
+		LastName:      "file.txt",
+		TotalMigrated: 99,
+		StartedAt:     "2026-01-01T00:00:00Z",
+	}
+
+	if err := saveState(f.Name(), original); err != nil {
+		t.Fatalf("saveState: %v", err)
+	}
+
+	loaded, err := loadState(f.Name())
+	if err != nil {
+		t.Fatalf("loadState: %v", err)
+	}
+
+	if loaded.Table != original.Table || loaded.LastDirhash != original.LastDirhash ||
+		loaded.LastName != original.LastName || loaded.TotalMigrated != original.TotalMigrated {
+		t.Errorf("round-trip mismatch: got %+v, want %+v", loaded, original)
+	}
+}
```

### T5: Test `ProgressTracker` contiguous tracking

```diff
--- /dev/null
+++ b/tracker_test.go
@@ -0,0 +1,55 @@
+//go:build ignore
+
+package main
+
+import "testing"
+
+func TestProgressTracker_ContiguousAdvance(t *testing.T) {
+	pt := NewProgressTracker()
+
+	// Record batch 0 success
+	pt.RecordResult(BatchResult{SeqNum: 0, LastEntry: Entry{Dirhash: 10, Name: "a"}, Success: true, Count: 5})
+	entry, ok := pt.GetSafeResumePoint()
+	if !ok || entry.Dirhash != 10 {
+		t.Errorf("expected dirhash=10 after seq 0, got ok=%v entry=%+v", ok, entry)
+	}
+
+	// Record batch 2 (out of order, gap at 1)
+	pt.RecordResult(BatchResult{SeqNum: 2, LastEntry: Entry{Dirhash: 30, Name: "c"}, Success: true, Count: 5})
+	entry, ok = pt.GetSafeResumePoint()
+	if !ok || entry.Dirhash != 10 {
+		t.Errorf("should still be at dirhash=10 (gap at seq 1), got ok=%v entry=%+v", ok, entry)
+	}
+
+	// Fill gap at batch 1
+	pt.RecordResult(BatchResult{SeqNum: 1, LastEntry: Entry{Dirhash: 20, Name: "b"}, Success: true, Count: 5})
+	entry, ok = pt.GetSafeResumePoint()
+	if !ok || entry.Dirhash != 30 {
+		t.Errorf("should advance to dirhash=30 after filling gap, got ok=%v entry=%+v", ok, entry)
+	}
+
+	success, _, _ := pt.GetCounts()
+	if success != 15 {
+		t.Errorf("expected 15 success count, got %d", success)
+	}
+}
+
+func TestProgressTracker_StopsAtFailure(t *testing.T) {
+	pt := NewProgressTracker()
+
+	pt.RecordResult(BatchResult{SeqNum: 0, Success: true, Count: 5, LastEntry: Entry{Dirhash: 10}})
+	pt.RecordResult(BatchResult{SeqNum: 1, Success: false, Count: 5})
+	pt.RecordResult(BatchResult{SeqNum: 2, Success: true, Count: 5, LastEntry: Entry{Dirhash: 30}})
+
+	entry, ok := pt.GetSafeResumePoint()
+	if !ok || entry.Dirhash != 10 {
+		t.Errorf("should stop at seq 0 before failure, got ok=%v entry=%+v", ok, entry)
+	}
+
+	_, _, hasFailure := pt.GetCounts()
+	if !hasFailure {
+		t.Error("expected hasFailure=true")
+	}
+}
```

### T6: Test `isPrintable` and `compareMeta` (audit.go)

```diff
--- /dev/null
+++ b/audit_helpers_test.go
@@ -0,0 +1,40 @@
+//go:build ignore
+
+package main
+
+import (
+	"strings"
+	"testing"
+)
+
+func TestIsPrintable(t *testing.T) {
+	if !isPrintable([]byte("hello world")) {
+		t.Error("expected printable")
+	}
+	if isPrintable([]byte{0x00}) {
+		t.Error("null byte should not be printable")
+	}
+	if isPrintable([]byte{}) {
+		t.Error("empty should not be printable")
+	}
+}
+
+func TestCompareMeta_Identical(t *testing.T) {
+	data := []byte{1, 2, 3, 4}
+	result := compareMeta(data, data)
+	if !strings.Contains(result, "identical") {
+		t.Errorf("expected 'identical', got %q", result)
+	}
+}
+
+func TestCompareMeta_DifferentLength(t *testing.T) {
+	result := compareMeta([]byte{1, 2}, []byte{1, 2, 3})
+	if !strings.Contains(result, "length differs") {
+		t.Errorf("expected 'length differs', got %q", result)
+	}
+}
```

---

## 3. FIXES — Bugs, Issues, and Code Smells

### F1: Build is broken with standard `go build` (Critical)

All three files (`main.go`, `audit.go`, `count_keys.go`) are `package main` with `func main()` and duplicate type/constant declarations. Running `go build` or `go vet` fails. The project requires per-file builds (e.g., `go build -o pg2tikv main.go`).

**Fix:** Move to standard Go multi-binary layout:

```diff
--- Current layout:
   main.go          (package main, func main, FilerConfig, etc.)
   audit.go         (package main, func main, FilerConfig, etc.)
   count_keys.go    (package main, func main)

+++ Proposed layout:
   cmd/pg2tikv/main.go       (migration binary)
   cmd/audit/main.go         (audit binary)
   cmd/count-keys/main.go    (count-keys binary)
   internal/config/config.go (shared FilerConfig, Postgres2Config, TiKVConfig)
   internal/tikv/keygen.go   (shared hashToBytes, generateTiKVKey, applyPathPrefix)
```

### F2: Version constants are inconsistent across files (Medium)

| File | `const Version` | VERSION file |
|------|----------------|--------------|
| main.go | `1.2.0` | `1.2.6` |
| audit.go | `1.2.2` | `1.2.6` |
| count_keys.go | `1.2.0` | `1.2.6` |

All three are behind VERSION. Either use ldflags to inject version at build time, or read from a shared constant.

```diff
--- a/main.go
+++ b/main.go
@@ -28,1 +28,1 @@
-const Version = "1.2.0"
+const Version = "1.2.6"
```

```diff
--- a/audit.go
+++ b/audit.go
@@ -29,1 +29,1 @@
-const Version = "1.2.2"
+const Version = "1.2.6"
```

```diff
--- a/count_keys.go
+++ b/count_keys.go
@@ -20,1 +20,1 @@
-const Version = "1.2.0"
+const Version = "1.2.6"
```

Better long-term: inject via ldflags:
```diff
-const Version = "1.2.6"
+var Version = "dev" // Set via -ldflags "-X main.Version=1.2.6"
```

### F3: `loadState` silently resets old state files (Low)

`main.go:141-143` — If `LastDirhash == 0 && LastName == "" && TotalMigrated == 0`, the state is silently reset to `MinInt64`. This is a heuristic that could misfire on a legitimate state where the first dirhash happens to be 0.

```diff
--- a/main.go
+++ b/main.go
@@ -138,7 +138,9 @@ func loadState(filename string) (*State, error) {
 	if err := json.Unmarshal(data, &state); err != nil {
 		return nil, fmt.Errorf("failed to parse state file: %w", err)
 	}
-	// Handle old state files that started from 0
+	// Handle old state files that started from 0.
+	// Note: This heuristic could misfire if the first real dirhash is 0.
+	// Consider adding a "version" field to state files to distinguish old vs new.
 	if state.LastDirhash == 0 && state.LastName == "" && state.TotalMigrated == 0 {
 		state.LastDirhash = MinInt64
 	}
```

### F4: `audit.go` sample query is slow for large tables (Medium)

`audit.go:944-949` — `ORDER BY RANDOM() LIMIT N` on a large Postgres table does a full table scan + sort. For tables with millions of rows, this could take minutes or OOM.

```diff
--- a/audit.go
+++ b/audit.go
@@ -941,10 +941,10 @@ func main() {
 	var query string
 	if *mode == "sample" {
 		query = fmt.Sprintf(`
-			SELECT dirhash, directory, name, meta
-			FROM %q
-			ORDER BY RANDOM()
-			LIMIT %d`,
+			SELECT dirhash, directory, name, meta
+			FROM %q
+			TABLESAMPLE SYSTEM (LEAST(100.0 * %d / GREATEST(1, (SELECT reltuples FROM pg_class WHERE relname = %q)), 100))
+			LIMIT %d`,
-			*table, *sampleSize)
+			*table, *sampleSize, *table, *sampleSize)
 		log.Printf("Running random sample of %d rows...", *sampleSize)
```

*(Note: TABLESAMPLE SYSTEM is approximate but avoids full-table sort. Alternatively, use `WHERE random() < N/total` for block-level sampling.)*

### F5: Missing `rows.Err()` check after `rows.Next()` loop in audit.go (Low)

`audit.go:967-975` — After the `for rows.Next()` loop, `rows.Err()` is never checked. If the iterator encountered an error, it would be silently dropped.

```diff
--- a/audit.go
+++ b/audit.go
@@ -975,6 +975,10 @@ func main() {
 		entryChan <- e
 	}

+	if err := rows.Err(); err != nil {
+		log.Printf("Warning: row iteration error: %v", err)
+	}
+
 	close(entryChan)
 	wg.Wait()
```

---

## 4. REFACTOR — Opportunities to Improve Code Quality

### R1: Extract shared code into internal packages (High Impact)

The following are duplicated identically between `main.go` and `audit.go`:
- `FilerConfig`, `Postgres2Config`, `TiKVConfig` structs and methods (~70 lines each)
- `hashToBytes`, `generateTiKVKey`, `applyPathPrefix` functions
- `Entry` struct
- `validTableName` regex
- `MinInt64` constant
- TLS validation logic (~15 lines)
- TLS configuration logic (~10 lines)

Total duplicated code: ~250 lines. Moving to `internal/config` and `internal/tikv` packages would halve the codebase and ensure consistency.

### R2: Adopt standard `cmd/` layout (High Impact)

The current flat layout with three `func main()` in one package is non-standard and prevents `go build ./...`, `go test ./...`, `go vet ./...`. Standard Go multi-binary projects use:
```
cmd/pg2tikv/main.go
cmd/audit/main.go
cmd/count-keys/main.go
internal/shared/...
```

### R3: Use `context.Context` consistently in `count_keys.go` (Low)

`countAllKeys` doesn't accept or use a context, so it can't be interrupted with Ctrl-C. `deleteAllKeys` uses `context.Background()` directly. Both should follow the signal-handling pattern from `main.go`.

### R4: Replace `atomic` operations in audit collector with plain operations (Low)

In `audit.go:886-938`, the result collector goroutine uses `atomic.AddInt64`/`atomic.LoadInt64` for counters, but since there's only one collector goroutine writing and reading these values, plain `int64` operations are sufficient. The atomics are only needed if there were multiple goroutines writing, which there aren't.

### R5: Connection string builder should quote values (Low)

`Postgres2Config.ConnectionString()` builds a libpq connection string but doesn't quote values. A password containing spaces or special characters (e.g., `p@ss word`) would break the connection string. Use `pq.QuoteLiteral()` or key-value escaping.

### R6: Consider a Makefile or build script (Medium)

Given the non-standard layout, a Makefile documenting how to build each binary would help:
```makefile
build:
    go build -o bin/seaweed-pg2tikv main.go
    go build -o bin/seaweed-pg2tikv-audit audit.go
    go build -o bin/seaweed-count-keys count_keys.go
```

This mitigates the confusing build situation until the codebase is restructured.
