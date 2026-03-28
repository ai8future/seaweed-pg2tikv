// Tests for main.go (migration tool)
// Run with: go test -v main.go main_test.go
package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ========== hashToBytes Tests ==========

func TestHashToBytes(t *testing.T) {
	tests := []struct {
		name string
		dir  string
	}{
		{"empty string", ""},
		{"root", "/"},
		{"simple path", "/home/user"},
		{"deep path", "/buckets/rootseek-heic/2024/01/15"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := hashToBytes(tt.dir)

			// SHA1 always produces 20 bytes
			if len(result) != 20 {
				t.Errorf("hashToBytes(%q) returned %d bytes, want 20", tt.dir, len(result))
			}

			// Verify it matches standard SHA1
			h := sha1.New()
			io.WriteString(h, tt.dir)
			expected := h.Sum(nil)
			if !bytes.Equal(result, expected) {
				t.Errorf("hashToBytes(%q) = %x, want %x", tt.dir, result, expected)
			}
		})
	}

	// Determinism: same input -> same output
	t.Run("deterministic", func(t *testing.T) {
		a := hashToBytes("/test/path")
		b := hashToBytes("/test/path")
		if !bytes.Equal(a, b) {
			t.Fatal("hashToBytes is not deterministic")
		}
	})

	// Different inputs -> different outputs
	t.Run("different inputs differ", func(t *testing.T) {
		a := hashToBytes("/path/a")
		b := hashToBytes("/path/b")
		if bytes.Equal(a, b) {
			t.Fatal("hashToBytes returned same result for different inputs")
		}
	})
}

// ========== generateTiKVKey Tests ==========

func TestGenerateTiKVKey(t *testing.T) {
	tests := []struct {
		name      string
		prefix    []byte
		directory string
		filename  string
	}{
		{"no prefix", nil, "/home", "file.txt"},
		{"empty prefix", []byte{}, "/home", "file.txt"},
		{"with prefix", []byte("sw."), "/home", "file.txt"},
		{"root dir", nil, "/", "root.txt"},
		{"empty filename", nil, "/dir", ""},
		{"long prefix", []byte("seaweedfs.filer."), "/data", "test.dat"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := generateTiKVKey(tt.prefix, tt.directory, tt.filename)

			expectedHash := hashToBytes(tt.directory)
			expectedLen := len(tt.prefix) + len(expectedHash) + len(tt.filename)
			if len(key) != expectedLen {
				t.Errorf("key length = %d, want %d", len(key), expectedLen)
			}

			// If prefix is set, key must start with it
			if len(tt.prefix) > 0 {
				if !bytes.HasPrefix(key, tt.prefix) {
					t.Errorf("key should start with prefix %q", tt.prefix)
				}
				// Hash should follow prefix
				hashPart := key[len(tt.prefix) : len(tt.prefix)+20]
				if !bytes.Equal(hashPart, expectedHash) {
					t.Errorf("hash portion mismatch")
				}
				// Filename should follow hash
				filenamePart := key[len(tt.prefix)+20:]
				if !bytes.Equal(filenamePart, []byte(tt.filename)) {
					t.Errorf("filename portion = %q, want %q", filenamePart, tt.filename)
				}
			} else {
				// Key should be: SHA1(dir) + filename
				if !bytes.Equal(key[:20], expectedHash) {
					t.Errorf("hash portion mismatch")
				}
				if tt.filename != "" {
					if !bytes.Equal(key[20:], []byte(tt.filename)) {
						t.Errorf("filename portion = %q, want %q", key[20:], tt.filename)
					}
				}
			}
		})
	}

	// Determinism: same inputs -> same key
	t.Run("deterministic", func(t *testing.T) {
		a := generateTiKVKey([]byte("pfx"), "/dir", "file")
		b := generateTiKVKey([]byte("pfx"), "/dir", "file")
		if !bytes.Equal(a, b) {
			t.Fatal("generateTiKVKey is not deterministic")
		}
	})
}

// ========== applyPathPrefix Tests ==========
// This function was the site of a known production bug.

func TestApplyPathPrefix(t *testing.T) {
	tests := []struct {
		name      string
		prefix    string
		directory string
		want      string
	}{
		{"no prefix passthrough", "", "/some/dir", "/some/dir"},
		{"no prefix root", "", "/", "/"},
		{"no prefix empty", "", "", ""},
		{"prefix with root dir", "/buckets/my-bucket", "/", "/buckets/my-bucket"},
		{"prefix with subdir", "/buckets/my-bucket", "/subdir", "/buckets/my-bucket/subdir"},
		{"prefix with deep path", "/buckets/my-bucket", "/a/b/c", "/buckets/my-bucket/a/b/c"},
		{"prefix with nested bucket", "/buckets/b", "/deep/nested/path", "/buckets/b/deep/nested/path"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyPathPrefix(tt.prefix, tt.directory)
			if got != tt.want {
				t.Errorf("applyPathPrefix(%q, %q) = %q, want %q", tt.prefix, tt.directory, got, tt.want)
			}
		})
	}
}

// ========== ConnectionString Tests ==========

func TestPostgres2ConfigConnectionString(t *testing.T) {
	tests := []struct {
		name string
		cfg  Postgres2Config
		want string
	}{
		{
			"basic connection",
			Postgres2Config{
				Hostname: "localhost", Port: 5432,
				Username: "user", Password: "pass",
				Database: "mydb", SSLMode: "disable",
			},
			"host=localhost port=5432 user=user password=pass dbname=mydb sslmode=disable",
		},
		{
			"with SSL certs",
			Postgres2Config{
				Hostname: "db.example.com", Port: 5433,
				Username: "admin", Password: "secret",
				Database: "prod", SSLMode: "verify-full",
				SSLCert: "/etc/ssl/client.crt", SSLKey: "/etc/ssl/client.key",
				SSLRootCert: "/etc/ssl/ca.crt",
			},
			"host=db.example.com port=5433 user=admin password=secret dbname=prod sslmode=verify-full sslcert=/etc/ssl/client.crt sslkey=/etc/ssl/client.key sslrootcert=/etc/ssl/ca.crt",
		},
		{
			"with schema",
			Postgres2Config{
				Hostname: "h", Port: 5432,
				Username: "u", Password: "p",
				Database: "d", SSLMode: "disable",
				Schema: "myschema",
			},
			"host=h port=5432 user=u password=p dbname=d sslmode=disable search_path=myschema",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.ConnectionString()
			if got != tt.want {
				t.Errorf("ConnectionString() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ========== PDAddrList Tests ==========

func TestPDAddrList(t *testing.T) {
	tests := []struct {
		name  string
		addrs string
		want  []string
	}{
		{"single", "localhost:2379", []string{"localhost:2379"}},
		{"multiple", "host1:2379,host2:2379,host3:2379", []string{"host1:2379", "host2:2379", "host3:2379"}},
		{"with spaces", "host1:2379 , host2:2379 , host3:2379", []string{"host1:2379", "host2:2379", "host3:2379"}},
		{"empty string", "", []string{""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := TiKVConfig{PDAddrs: tt.addrs}
			got := c.PDAddrList()
			if len(got) != len(tt.want) {
				t.Fatalf("PDAddrList() returned %d addrs, want %d", len(got), len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("PDAddrList()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// ========== validTableName Tests ==========

func TestValidTableName(t *testing.T) {
	valid := []string{
		"filemeta", "rootseek_heic", "my-bucket", "_private",
		"Table123", "a", "A_b_C", "with-dashes-here",
	}
	invalid := []string{
		"", "123start", "has spaces", "drop;--", "table'name",
		"path/traversal", "semi;colon", "back`tick",
		"has.dot", "paren(", "star*",
	}

	for _, name := range valid {
		if !validTableName.MatchString(name) {
			t.Errorf("validTableName rejected valid name %q", name)
		}
	}
	for _, name := range invalid {
		if validTableName.MatchString(name) {
			t.Errorf("validTableName accepted invalid name %q", name)
		}
	}
}

// ========== State Management Tests ==========

func TestLoadState_NewFile(t *testing.T) {
	state, err := loadState(filepath.Join(t.TempDir(), "nonexistent.json"))
	if err != nil {
		t.Fatalf("loadState() error: %v", err)
	}
	if state.LastDirhash != MinInt64 {
		t.Errorf("new state LastDirhash = %d, want %d", state.LastDirhash, MinInt64)
	}
	if state.LastName != "" {
		t.Errorf("new state LastName = %q, want empty", state.LastName)
	}
	if state.StartedAt == "" {
		t.Error("new state StartedAt should be set")
	}
}

func TestSaveAndLoadState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test_state.json")
	original := &State{
		Table:         "filemeta",
		LastDirhash:   42,
		LastName:      "test.txt",
		TotalMigrated: 1000,
		StartedAt:     "2026-01-01T00:00:00Z",
	}

	if err := saveState(path, original); err != nil {
		t.Fatalf("saveState() error: %v", err)
	}

	loaded, err := loadState(path)
	if err != nil {
		t.Fatalf("loadState() error: %v", err)
	}

	if loaded.Table != original.Table {
		t.Errorf("Table = %q, want %q", loaded.Table, original.Table)
	}
	if loaded.LastDirhash != original.LastDirhash {
		t.Errorf("LastDirhash = %d, want %d", loaded.LastDirhash, original.LastDirhash)
	}
	if loaded.LastName != original.LastName {
		t.Errorf("LastName = %q, want %q", loaded.LastName, original.LastName)
	}
	if loaded.TotalMigrated != original.TotalMigrated {
		t.Errorf("TotalMigrated = %d, want %d", loaded.TotalMigrated, original.TotalMigrated)
	}
}

func TestLoadState_OldFormat(t *testing.T) {
	// Old state files started from dirhash=0 -- should be corrected to MinInt64
	path := filepath.Join(t.TempDir(), "old_state.json")
	data, _ := json.Marshal(State{LastDirhash: 0, LastName: "", TotalMigrated: 0})
	os.WriteFile(path, data, 0644)

	state, err := loadState(path)
	if err != nil {
		t.Fatalf("loadState() error: %v", err)
	}
	if state.LastDirhash != MinInt64 {
		t.Errorf("old format LastDirhash = %d, want %d (MinInt64)", state.LastDirhash, MinInt64)
	}
}

func TestLoadState_InvalidJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.json")
	os.WriteFile(path, []byte("not json"), 0644)

	_, err := loadState(path)
	if err == nil {
		t.Error("loadState() with invalid JSON should return error")
	}
}

// ========== ProgressTracker Tests ==========

func TestProgressTracker_BasicFlow(t *testing.T) {
	pt := NewProgressTracker()

	// Initially empty
	_, ok := pt.GetSafeResumePoint()
	if ok {
		t.Error("new tracker should have no safe resume point")
	}
	s, f, hf := pt.GetCounts()
	if s != 0 || f != 0 || hf {
		t.Errorf("initial counts: got s=%d f=%d hf=%v, want 0,0,false", s, f, hf)
	}

	// Record sequential successes
	pt.RecordResult(BatchResult{SeqNum: 0, LastEntry: Entry{Dirhash: 10, Name: "a"}, Success: true, Count: 5})
	pt.RecordResult(BatchResult{SeqNum: 1, LastEntry: Entry{Dirhash: 20, Name: "b"}, Success: true, Count: 3})

	entry, ok := pt.GetSafeResumePoint()
	if !ok {
		t.Fatal("should have safe resume point after seq 0,1")
	}
	if entry.Dirhash != 20 || entry.Name != "b" {
		t.Errorf("resume point = {%d, %q}, want {20, \"b\"}", entry.Dirhash, entry.Name)
	}

	s, f, hf = pt.GetCounts()
	if s != 8 || f != 0 || hf {
		t.Errorf("counts: got s=%d f=%d hf=%v, want 8,0,false", s, f, hf)
	}
}

func TestProgressTracker_OutOfOrderBatches(t *testing.T) {
	pt := NewProgressTracker()

	// Batch 2 arrives before batch 0 and 1
	pt.RecordResult(BatchResult{SeqNum: 2, LastEntry: Entry{Dirhash: 30}, Success: true, Count: 1})

	_, ok := pt.GetSafeResumePoint()
	if ok {
		t.Error("should NOT have resume point when seq 0,1 are missing")
	}

	// Fill in batch 0
	pt.RecordResult(BatchResult{SeqNum: 0, LastEntry: Entry{Dirhash: 10}, Success: true, Count: 1})
	entry, ok := pt.GetSafeResumePoint()
	if !ok || entry.Dirhash != 10 {
		t.Errorf("after batch 0, should resume at dirhash=10, got %+v ok=%v", entry, ok)
	}

	// Fill in batch 1 -- now contiguous through 2
	pt.RecordResult(BatchResult{SeqNum: 1, LastEntry: Entry{Dirhash: 20}, Success: true, Count: 1})
	entry, ok = pt.GetSafeResumePoint()
	if !ok || entry.Dirhash != 30 {
		t.Errorf("after filling gap, resume = {%d, %v}, want {30, true}", entry.Dirhash, ok)
	}
}

func TestProgressTracker_FailedBatch(t *testing.T) {
	pt := NewProgressTracker()

	pt.RecordResult(BatchResult{SeqNum: 0, LastEntry: Entry{Dirhash: 10}, Success: true, Count: 5})
	pt.RecordResult(BatchResult{SeqNum: 1, Success: false, Count: 3})
	pt.RecordResult(BatchResult{SeqNum: 2, LastEntry: Entry{Dirhash: 30}, Success: true, Count: 2})

	// Should NOT advance past failed batch at seq 1
	entry, ok := pt.GetSafeResumePoint()
	if !ok || entry.Dirhash != 10 {
		t.Errorf("resume should stop at seq 0: got {%d, %v}", entry.Dirhash, ok)
	}

	s, f, hf := pt.GetCounts()
	if s != 7 || f != 3 || !hf {
		t.Errorf("counts = s=%d f=%d hf=%v, want 7,3,true", s, f, hf)
	}
}

func TestProgressTracker_ConcurrentAccess(t *testing.T) {
	pt := NewProgressTracker()
	var wg sync.WaitGroup

	for i := int64(0); i < 100; i++ {
		wg.Add(1)
		go func(seq int64) {
			defer wg.Done()
			pt.RecordResult(BatchResult{
				SeqNum:    seq,
				LastEntry: Entry{Dirhash: seq * 10},
				Success:   true,
				Count:     1,
			})
		}(i)
	}
	wg.Wait()

	s, _, _ := pt.GetCounts()
	if s != 100 {
		t.Errorf("concurrent success count = %d, want 100", s)
	}

	entry, ok := pt.GetSafeResumePoint()
	if !ok {
		t.Fatal("should have resume point after 100 contiguous batches")
	}
	if entry.Dirhash != 990 {
		t.Errorf("resume dirhash = %d, want 990", entry.Dirhash)
	}
}

// ========== sleepCtx Tests ==========

func TestSleepCtx_NormalSleep(t *testing.T) {
	ctx := context.Background()
	start := time.Now()
	ok := sleepCtx(ctx, 50*time.Millisecond)
	elapsed := time.Since(start)

	if !ok {
		t.Error("sleepCtx should return true for normal sleep")
	}
	if elapsed < 40*time.Millisecond {
		t.Errorf("slept only %v, expected >= 50ms", elapsed)
	}
}

func TestSleepCtx_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	start := time.Now()
	ok := sleepCtx(ctx, 5*time.Second)
	elapsed := time.Since(start)

	if ok {
		t.Error("sleepCtx should return false for cancelled context")
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("cancelled sleep took %v, should be near-instant", elapsed)
	}
}
