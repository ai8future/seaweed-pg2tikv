// seaweed-pg2tikv - Migrate SeaweedFS filer metadata from Postgres to TiKV
// Version 1.1.0
package main

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/lib/pq"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
)

const Version = "1.2.0"

const MinInt64 = -9223372036854775808

// validTableName validates that a table name contains only safe characters
var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_\-]*$`)

// ========== Config Structures ==========

type FilerConfig struct {
	Postgres2 Postgres2Config `toml:"postgres2"`
	TiKV      TiKVConfig      `toml:"tikv"`
}

type Postgres2Config struct {
	Enabled     bool   `toml:"enabled"`
	Hostname    string `toml:"hostname"`
	Port        int    `toml:"port"`
	Username    string `toml:"username"`
	Password    string `toml:"password"`
	Database    string `toml:"database"`
	Schema      string `toml:"schema"`
	SSLMode     string `toml:"sslmode"`
	SSLCert     string `toml:"sslcert"`
	SSLKey      string `toml:"sslkey"`
	SSLRootCert string `toml:"sslrootcert"`
}

func (c *Postgres2Config) ConnectionString() string {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		c.Hostname, c.Port, c.Username, c.Password, c.Database, c.SSLMode,
	)
	if c.SSLCert != "" {
		connStr += fmt.Sprintf(" sslcert=%s", c.SSLCert)
	}
	if c.SSLKey != "" {
		connStr += fmt.Sprintf(" sslkey=%s", c.SSLKey)
	}
	if c.SSLRootCert != "" {
		connStr += fmt.Sprintf(" sslrootcert=%s", c.SSLRootCert)
	}
	if c.Schema != "" {
		connStr += fmt.Sprintf(" search_path=%s", c.Schema)
	}
	return connStr
}

type TiKVConfig struct {
	Enabled   bool   `toml:"enabled"`
	PDAddrs   string `toml:"pdaddrs"`
	KeyPrefix string `toml:"keyPrefix"`
	Enable1PC bool   `toml:"enable_1pc"`
	CAPath    string `toml:"ca_path"`
	CertPath  string `toml:"cert_path"`
	KeyPath   string `toml:"key_path"`
	VerifyCN  string `toml:"verify_cn"`
}

func (c *TiKVConfig) PDAddrList() []string {
	addrs := strings.Split(c.PDAddrs, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	return addrs
}

// ========== CLI Flags ==========

var (
	pgConfigFile   = flag.String("pg-config", "", "Path to postgres filer.toml")
	tikvConfigFile = flag.String("tikv-config", "", "Path to tikv filer.toml (can be same file)")
	table          = flag.String("table", "filemeta", "Source Postgres table name")
	workers        = flag.Int("workers", 10, "Parallel TiKV write workers")
	batchSize      = flag.Int("batch", 1000, "Entries per TiKV transaction")
	stateFile      = flag.String("state", "", "Progress state file (default: migrate_state_{table}.json)")
	partitionMod   = flag.Int("partition-mod", 1, "Total partitions for parallel instances")
	partitionID    = flag.Int("partition-id", 0, "This instance's partition (0 to mod-1)")
	dryRun         = flag.Bool("dry-run", false, "Parse configs and exit without migrating")
	showVersion    = flag.Bool("version", false, "Show version and exit")
	maxRetries     = flag.Int("max-retries", 15, "Maximum retries per batch")
	retryBaseMs    = flag.Int("retry-base-ms", 500, "Base retry delay in milliseconds")
	pathPrefix     = flag.String("path-prefix", "", "Path prefix for per-bucket tables (e.g., /buckets/my-bucket)")
)

// ========== State Management ==========

type State struct {
	Table         string `json:"table"`
	LastDirhash   int64  `json:"last_dirhash"`
	LastName      string `json:"last_name"`
	TotalMigrated int64  `json:"total_migrated"`
	StartedAt     string `json:"started_at"`
}

func loadState(filename string) (*State, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			// Start from minimum int64 to include negative dirhash values
			return &State{
				StartedAt:   time.Now().UTC().Format(time.RFC3339),
				LastDirhash: MinInt64,
				LastName:    "",
			}, nil
		}
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state file: %w", err)
	}
	// Handle old state files that started from 0
	if state.LastDirhash == 0 && state.LastName == "" && state.TotalMigrated == 0 {
		state.LastDirhash = MinInt64
	}
	return &state, nil
}

func saveState(filename string, state *State) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}
	return nil
}

// ========== TiKV Key Generation ==========
// Note: SHA1 is required here to match SeaweedFS filer's key generation.
// This is not for cryptographic security, just consistent key derivation.

func hashToBytes(dir string) []byte {
	h := sha1.New()
	io.WriteString(h, dir)
	return h.Sum(nil)
}

func generateTiKVKey(prefix []byte, directory, filename string) []byte {
	key := hashToBytes(directory)
	key = append(key, []byte(filename)...)
	if len(prefix) > 0 {
		result := make([]byte, len(prefix)+len(key))
		copy(result, prefix)
		copy(result[len(prefix):], key)
		return result
	}
	return key
}

// applyPathPrefix prepends the bucket path to the directory for per-bucket tables.
// SeaweedFS postgres2 stores paths relative to the bucket root, but TiKV needs
// absolute paths. For the root filemeta table, pathPrefix is empty.
// For bucket tables, pathPrefix is e.g. "/buckets/my-bucket".
func applyPathPrefix(prefix, directory string) string {
	if prefix == "" {
		return directory
	}
	if directory == "/" {
		return prefix
	}
	return prefix + directory
}

// ========== Entry & Worker ==========

type Entry struct {
	Dirhash   int64
	Directory string
	Name      string
	Meta      []byte
}

// Batch wraps entries with a sequence number for ordered progress tracking
type Batch struct {
	SeqNum  int64
	Entries []Entry
}

// BatchResult reports the outcome of processing a batch
type BatchResult struct {
	SeqNum    int64
	LastEntry Entry
	Success   bool
	Count     int
}

// sleepCtx sleeps for the given duration but returns early if the context is cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func commitBatch(ctx context.Context, tikvClient *txnkv.Client, prefix []byte, enable1PC bool, batch []Entry, workerID int, maxRetryCount int, retryBaseDelayMs int) error {
	for retry := 0; retry < maxRetryCount; retry++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		txn, err := tikvClient.Begin()
		if err != nil {
			log.Printf("[worker-%02d] begin txn error (attempt %d/%d): %v", workerID, retry+1, maxRetryCount, err)
			if !sleepCtx(ctx, time.Duration(retry+1)*time.Duration(retryBaseDelayMs)*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}

		if enable1PC {
			txn.SetEnable1PC(true)
		}

		// Set all entries, fail batch on any error
		var setErr error
		for _, e := range batch {
			key := generateTiKVKey(prefix, e.Directory, e.Name)
			if err := txn.Set(key, e.Meta); err != nil {
				log.Printf("[worker-%02d] set error %s/%s: %v", workerID, e.Directory, e.Name, err)
				setErr = err
				break
			}
		}

		if setErr != nil {
			txn.Rollback()
			backoff := time.Duration(retry+1) * time.Duration(retryBaseDelayMs) * time.Millisecond
			if retry < maxRetryCount-1 {
				log.Printf("[worker-%02d] RETRY %d/%d after set error (backoff %v): %v", workerID, retry+1, maxRetryCount, backoff, setErr)
				if !sleepCtx(ctx, backoff) {
					return ctx.Err()
				}
				continue
			}
			return setErr
		}

		// Use the provided context for commit to respect cancellation
		err = txn.Commit(ctx)
		if err == nil {
			return nil // Success!
		}

		txn.Rollback()

		// Exponential backoff
		backoff := time.Duration(retry+1) * time.Duration(retryBaseDelayMs) * time.Millisecond
		if retry < maxRetryCount-1 {
			log.Printf("[worker-%02d] RETRY %d/%d (backoff %v): %v", workerID, retry+1, maxRetryCount, backoff, err)
			if !sleepCtx(ctx, backoff) {
				return ctx.Err()
			}
		} else {
			log.Printf("[worker-%02d] FAILED after %d attempts: %v", workerID, maxRetryCount, err)
			return err
		}
	}

	return fmt.Errorf("max retries exceeded")
}

func worker(ctx context.Context, id int, tikvClient *txnkv.Client, prefix []byte, enable1PC bool,
	batches <-chan Batch, results chan<- BatchResult, maxRetryCount int, retryBaseDelayMs int, wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range batches {
		if ctx.Err() != nil {
			return
		}

		if len(batch.Entries) == 0 {
			// Safety check: skip empty batches
			results <- BatchResult{SeqNum: batch.SeqNum, Success: true, Count: 0}
			continue
		}

		err := commitBatch(ctx, tikvClient, prefix, enable1PC, batch.Entries, id, maxRetryCount, retryBaseDelayMs)
		if err != nil {
			results <- BatchResult{
				SeqNum:    batch.SeqNum,
				LastEntry: batch.Entries[len(batch.Entries)-1],
				Success:   false,
				Count:     len(batch.Entries),
			}
			continue
		}

		results <- BatchResult{
			SeqNum:    batch.SeqNum,
			LastEntry: batch.Entries[len(batch.Entries)-1],
			Success:   true,
			Count:     len(batch.Entries),
		}
	}
}

// ========== Progress Tracker ==========
// Tracks batch completion to safely determine resume point.
// Only advances progress when contiguous batches have succeeded.

type ProgressTracker struct {
	mu              sync.Mutex
	completedSeqs   map[int64]Entry // seqNum -> lastEntry for completed batches
	highestContig   int64           // highest contiguous completed sequence
	successCount    int64
	failCount       int64
	hasFailure      bool // set to true if any batch fails
	failedSeqs      map[int64]bool
}

func NewProgressTracker() *ProgressTracker {
	return &ProgressTracker{
		completedSeqs: make(map[int64]Entry),
		failedSeqs:    make(map[int64]bool),
		highestContig: -1,
	}
}

func (pt *ProgressTracker) RecordResult(result BatchResult) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if result.Success {
		pt.completedSeqs[result.SeqNum] = result.LastEntry
		pt.successCount += int64(result.Count)

		// Update highest contiguous sequence
		for {
			nextSeq := pt.highestContig + 1
			if _, ok := pt.completedSeqs[nextSeq]; ok {
				pt.highestContig = nextSeq
			} else if pt.failedSeqs[nextSeq] {
				// Stop at failures - don't advance past them
				break
			} else {
				break
			}
		}
	} else {
		pt.failCount += int64(result.Count)
		pt.hasFailure = true
		pt.failedSeqs[result.SeqNum] = true
	}
}

func (pt *ProgressTracker) GetSafeResumePoint() (Entry, bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.highestContig < 0 {
		return Entry{}, false
	}
	entry, ok := pt.completedSeqs[pt.highestContig]
	return entry, ok
}

func (pt *ProgressTracker) GetCounts() (success, fail int64, hasFailure bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return pt.successCount, pt.failCount, pt.hasFailure
}

// ========== Main ==========

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("seaweed-pg2tikv version %s\n", Version)
		return
	}

	if *pgConfigFile == "" || *tikvConfigFile == "" {
		fmt.Printf("seaweed-pg2tikv version %s - Migrate SeaweedFS filer metadata from Postgres to TiKV\n", Version)
		fmt.Println()
		fmt.Println("Usage: seaweed-pg2tikv --pg-config=<postgres_filer.toml> --tikv-config=<tikv_filer.toml>")
		fmt.Println()
		fmt.Println("Options:")
		flag.PrintDefaults()
		fmt.Println()
		fmt.Println("Examples:")
		fmt.Println("  # Single instance migration")
		fmt.Println("  seaweed-pg2tikv --pg-config=pg.toml --tikv-config=tikv.toml --table=rootseek-heic")
		fmt.Println()
		fmt.Println("  # Per-bucket table migration (REQUIRED for bucket tables)")
		fmt.Println("  seaweed-pg2tikv --pg-config=pg.toml --tikv-config=tikv.toml --table=my-bucket \\")
		fmt.Println("          --path-prefix=/buckets/my-bucket")
		fmt.Println()
		fmt.Println("  # Parallel migration with 5 instances")
		fmt.Println("  seaweed-pg2tikv --pg-config=pg.toml --tikv-config=tikv.toml --table=rootseek-heic \\")
		fmt.Println("          --partition-mod=5 --partition-id=0 --state=state_0.json")
		os.Exit(1)
	}

	// Validate table name to prevent SQL injection
	if !validTableName.MatchString(*table) {
		log.Fatalf("Invalid table name %q: must start with letter/underscore and contain only alphanumeric, underscore, or hyphen", *table)
	}

	// Validate path prefix
	if *pathPrefix != "" {
		if !strings.HasPrefix(*pathPrefix, "/") {
			log.Fatalf("Invalid path-prefix %q: must start with /", *pathPrefix)
		}
		// Remove trailing slash if present
		*pathPrefix = strings.TrimRight(*pathPrefix, "/")
	}

	// Validate partition parameters
	if *partitionMod <= 0 {
		log.Fatalf("Invalid partition-mod %d: must be > 0", *partitionMod)
	}
	if *partitionID < 0 || *partitionID >= *partitionMod {
		log.Fatalf("Invalid partition-id %d: must be >= 0 and < partition-mod (%d)", *partitionID, *partitionMod)
	}

	// Parse Postgres config
	var pgConfig FilerConfig
	if _, err := toml.DecodeFile(*pgConfigFile, &pgConfig); err != nil {
		log.Fatalf("Failed to parse postgres config %s: %v", *pgConfigFile, err)
	}
	if pgConfig.Postgres2.Hostname == "" {
		log.Fatalf("No [postgres2] section found in %s", *pgConfigFile)
	}

	// Parse TiKV config
	var tikvConfig FilerConfig
	if _, err := toml.DecodeFile(*tikvConfigFile, &tikvConfig); err != nil {
		log.Fatalf("Failed to parse tikv config %s: %v", *tikvConfigFile, err)
	}
	if tikvConfig.TiKV.PDAddrs == "" {
		log.Fatalf("No [tikv] section found in %s", *tikvConfigFile)
	}

	// Validate TLS paths if specified
	if tikvConfig.TiKV.CAPath != "" {
		if _, err := os.Stat(tikvConfig.TiKV.CAPath); err != nil {
			log.Fatalf("TLS CA path not accessible: %v", err)
		}
	}
	if tikvConfig.TiKV.CertPath != "" {
		if _, err := os.Stat(tikvConfig.TiKV.CertPath); err != nil {
			log.Fatalf("TLS cert path not accessible: %v", err)
		}
	}
	if tikvConfig.TiKV.KeyPath != "" {
		if _, err := os.Stat(tikvConfig.TiKV.KeyPath); err != nil {
			log.Fatalf("TLS key path not accessible: %v", err)
		}
	}

	// Print config summary
	log.Printf("seaweed-pg2tikv version %s", Version)
	log.Println("=== Configuration ===")
	log.Printf("Postgres: %s@%s:%d/%s (table: %s)",
		pgConfig.Postgres2.Username,
		pgConfig.Postgres2.Hostname,
		pgConfig.Postgres2.Port,
		pgConfig.Postgres2.Database,
		*table)
	log.Printf("TiKV PD:  %s (prefix: %q, 1PC: %v)",
		tikvConfig.TiKV.PDAddrs,
		tikvConfig.TiKV.KeyPrefix,
		tikvConfig.TiKV.Enable1PC)
	log.Printf("Workers:  %d, Batch: %d, Partition: %d/%d",
		*workers, *batchSize, *partitionID, *partitionMod)
	if *pathPrefix != "" {
		log.Printf("Path prefix: %s (per-bucket table mode)", *pathPrefix)
	}
	log.Printf("Retries:  max=%d, base_delay=%dms", *maxRetries, *retryBaseMs)

	if *dryRun {
		log.Println("Dry run - config parsed successfully, exiting without migration")
		return
	}

	// Set default state file based on table name if not specified
	actualStateFile := *stateFile
	if actualStateFile == "" {
		actualStateFile = fmt.Sprintf("migrate_state_%s.json", *table)
	}
	log.Printf("State file: %s", actualStateFile)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Load resume state
	state, err := loadState(actualStateFile)
	if err != nil {
		log.Fatalf("Failed to load state: %v", err)
	}
	if state.Table != "" && state.Table != *table {
		log.Printf("Warning: state file is for table %q, resetting for %q", state.Table, *table)
		state = &State{StartedAt: time.Now().UTC().Format(time.RFC3339), LastDirhash: MinInt64}
	}
	state.Table = *table

	// Connect to Postgres
	pgConnStr := pgConfig.Postgres2.ConnectionString()
	db, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatalf("Postgres connection failed: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Postgres ping failed: %v", err)
	}
	log.Println("Connected to Postgres")

	// Connect to TiKV
	if tikvConfig.TiKV.CAPath != "" {
		verifyCN := strings.Split(tikvConfig.TiKV.VerifyCN, ",")
		config.UpdateGlobal(func(conf *config.Config) {
			conf.Security = config.NewSecurity(
				tikvConfig.TiKV.CAPath,
				tikvConfig.TiKV.CertPath,
				tikvConfig.TiKV.KeyPath,
				verifyCN,
			)
		})
	}

	tikvClient, err := txnkv.NewClient(tikvConfig.TiKV.PDAddrList())
	if err != nil {
		log.Fatalf("TiKV connection failed: %v", err)
	}
	defer tikvClient.Close()
	log.Println("Connected to TiKV")

	prefix := []byte(tikvConfig.TiKV.KeyPrefix)
	enable1PC := tikvConfig.TiKV.Enable1PC

	// Initialize progress tracker
	tracker := NewProgressTracker()

	batchChan := make(chan Batch, *workers*2)
	resultChan := make(chan BatchResult, *workers*2)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(ctx, i, tikvClient, prefix, enable1PC, batchChan, resultChan, *maxRetries, *retryBaseMs, &wg)
	}

	// Result collector goroutine
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		for result := range resultChan {
			tracker.RecordResult(result)
		}
	}()

	// Progress reporter
	startTime := time.Now()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		var lastCount int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				success, failed, _ := tracker.GetCounts()
				rate := float64(success-lastCount) / 10.0
				lastCount = success
				elapsed := time.Since(startTime)

				safeEntry, hasSafe := tracker.GetSafeResumePoint()
				dirhash := state.LastDirhash
				if hasSafe {
					dirhash = safeEntry.Dirhash
				}

				log.Printf("Progress: %d OK, %d FAILED (%.0f/sec) | elapsed: %v | dirhash: %d",
					success, failed, rate, elapsed.Round(time.Second), dirhash)

				// Save state only from safe resume point
				if hasSafe {
					state.TotalMigrated = success
					state.LastDirhash = safeEntry.Dirhash
					state.LastName = safeEntry.Name
					if err := saveState(actualStateFile, state); err != nil {
						log.Printf("Warning: failed to save state: %v", err)
					}
				}
			}
		}
	}()

	// Paginated query with resume and partitioning
	// Table name has been validated against SQL injection
	queryLimit := *batchSize * *workers * 2

	// When partition-mod=1 (single instance), skip the mod() clause
	// so Postgres can use an index on (dirhash, name)
	var query string
	usePartitioning := *partitionMod > 1
	if usePartitioning {
		query = fmt.Sprintf(`
			SELECT dirhash, directory, name, meta
			FROM %q
			WHERE (dirhash, name) > ($1, $2)
			  AND mod(abs(dirhash), $3) = $4
			ORDER BY dirhash, name
			LIMIT $5`,
			*table)
	} else {
		query = fmt.Sprintf(`
			SELECT dirhash, directory, name, meta
			FROM %q
			WHERE (dirhash, name) > ($1, $2)
			ORDER BY dirhash, name
			LIMIT $3`,
			*table)
	}

	curDirhash := state.LastDirhash
	curName := state.LastName
	log.Printf("Starting from dirhash=%d name=%q (page size: %d)", curDirhash, curName, queryLimit)

	entries := make([]Entry, 0, *batchSize)
	var rowCount int64
	var batchSeqNum int64
	done := make(chan struct{})

	go func() {
		defer close(done)
		queryRetries := 0
		const maxQueryRetries = 10
		for {
			if ctx.Err() != nil {
				return
			}

			var rows *sql.Rows
			var err error
			if usePartitioning {
				rows, err = db.QueryContext(ctx, query,
					curDirhash, curName,
					*partitionMod, *partitionID,
					queryLimit)
			} else {
				rows, err = db.QueryContext(ctx, query,
					curDirhash, curName,
					queryLimit)
			}
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				queryRetries++
				if queryRetries > maxQueryRetries {
					log.Printf("Query failed after %d retries, giving up: %v", maxQueryRetries, err)
					return
				}
				backoff := time.Duration(queryRetries) * 5 * time.Second
				log.Printf("Query error (retry %d/%d, backoff %v): %v", queryRetries, maxQueryRetries, backoff, err)
				if !sleepCtx(ctx, backoff) {
					return
				}
				continue
			}
			queryRetries = 0 // reset on success

			pageCount := 0
			for rows.Next() {
				var e Entry
				if err := rows.Scan(&e.Dirhash, &e.Directory, &e.Name, &e.Meta); err != nil {
					log.Printf("Scan error: %v", err)
					continue
				}

				// Apply path prefix for per-bucket tables
				e.Directory = applyPathPrefix(*pathPrefix, e.Directory)

				rowCount++
				pageCount++
				curDirhash = e.Dirhash
				curName = e.Name
				entries = append(entries, e)
				if len(entries) >= *batchSize {
					select {
					case batchChan <- Batch{SeqNum: batchSeqNum, Entries: entries}:
						batchSeqNum++
						entries = make([]Entry, 0, *batchSize)
					case <-ctx.Done():
						rows.Close()
						return
					}
				}
			}
			rows.Close()

			if err := rows.Err(); err != nil {
				if ctx.Err() != nil {
					return
				}
				queryRetries++
				if queryRetries > maxQueryRetries {
					log.Printf("Query iteration failed after %d retries, giving up: %v", maxQueryRetries, err)
					return
				}
				backoff := time.Duration(queryRetries) * 5 * time.Second
				log.Printf("Query iteration error (retry %d/%d from last position, backoff %v): %v",
					queryRetries, maxQueryRetries, backoff, err)
				if !sleepCtx(ctx, backoff) {
					return
				}
				continue
			}

			// If we got fewer rows than the limit, we've read everything
			if pageCount < queryLimit {
				break
			}
			log.Printf("Page complete: %d rows fetched this page, continuing from dirhash=%d", pageCount, curDirhash)
		}

		// Flush remaining entries
		if len(entries) > 0 {
			select {
			case batchChan <- Batch{SeqNum: batchSeqNum, Entries: entries}:
			case <-ctx.Done():
			}
		}
	}()

	// Wait for completion or interrupt
	select {
	case <-sigChan:
		log.Println("Interrupt received, saving state and draining workers...")
		cancel()
		// Second Ctrl-C force-exits
		go func() {
			<-sigChan
			log.Println("Second interrupt received, force exiting")
			os.Exit(1)
		}()
	case <-done:
		log.Printf("All %d rows read from Postgres, waiting for workers...", rowCount)
	}

	close(batchChan)
	wg.Wait()
	close(resultChan)
	collectorWg.Wait()

	// Final state save - only from safe resume point
	finalSuccess, finalFailed, hadFailure := tracker.GetCounts()
	if safeEntry, hasSafe := tracker.GetSafeResumePoint(); hasSafe {
		state.TotalMigrated = finalSuccess
		state.LastDirhash = safeEntry.Dirhash
		state.LastName = safeEntry.Name
		if err := saveState(actualStateFile, state); err != nil {
			log.Printf("Error: failed to save final state: %v", err)
		}
	}

	log.Println("=== Migration Complete ===")
	log.Printf("Rows read from Postgres: %d", rowCount)
	log.Printf("Successfully written to TiKV: %d", finalSuccess)
	log.Printf("Failed to write: %d", finalFailed)
	if hadFailure {
		log.Printf("WARNING: Some batches failed. Re-run to retry from saved checkpoint.")
	}
	log.Printf("State saved to: %s", actualStateFile)
	log.Printf("seaweed-pg2tikv version %s", Version)
}
