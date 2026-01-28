// seaweed-pg2tikv - Migrate SeaweedFS filer metadata from Postgres to TiKV
// Version 1.0.3
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
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/lib/pq"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
)

const Version = "1.0.3"

const MinInt64 = -9223372036854775808

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
)

// ========== State Management ==========

type State struct {
	Table         string `json:"table"`
	LastDirhash   int64  `json:"last_dirhash"`
	LastName      string `json:"last_name"`
	TotalMigrated int64  `json:"total_migrated"`
	StartedAt     string `json:"started_at"`
}

func loadState(filename string) *State {
	data, err := os.ReadFile(filename)
	if err != nil {
		// Start from minimum int64 to include negative dirhash values
		return &State{
			StartedAt:   time.Now().UTC().Format(time.RFC3339),
			LastDirhash: MinInt64,
			LastName:    "",
		}
	}
	var state State
	json.Unmarshal(data, &state)
	// Handle old state files that started from 0
	if state.LastDirhash == 0 && state.LastName == "" && state.TotalMigrated == 0 {
		state.LastDirhash = MinInt64
	}
	return &state
}

func saveState(filename string, state *State) {
	data, _ := json.MarshalIndent(state, "", "  ")
	os.WriteFile(filename, data, 0644)
}

// ========== TiKV Key Generation ==========

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

// ========== Entry & Worker ==========

type Entry struct {
	Dirhash   int64
	Directory string
	Name      string
	Meta      []byte
}

func commitBatch(ctx context.Context, tikvClient *txnkv.Client, prefix []byte, enable1PC bool, batch []Entry, workerID int) error {
	const maxRetries = 15

	for retry := 0; retry < maxRetries; retry++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		txn, err := tikvClient.Begin()
		if err != nil {
			log.Printf("[worker-%02d] begin txn error (attempt %d/%d): %v", workerID, retry+1, maxRetries, err)
			time.Sleep(time.Duration(retry+1) * 200 * time.Millisecond)
			continue
		}

		if enable1PC {
			txn.SetEnable1PC(true)
		}

		for _, e := range batch {
			key := generateTiKVKey(prefix, e.Directory, e.Name)
			if err := txn.Set(key, e.Meta); err != nil {
				log.Printf("[worker-%02d] set error %s/%s: %v", workerID, e.Directory, e.Name, err)
			}
		}

		err = txn.Commit(context.Background())
		if err == nil {
			return nil // Success!
		}

		txn.Rollback()

		// Exponential backoff with jitter
		backoff := time.Duration(retry+1) * 500 * time.Millisecond
		if retry < maxRetries-1 {
			log.Printf("[worker-%02d] RETRY %d/%d (backoff %v): %v", workerID, retry+1, maxRetries, backoff, err)
			time.Sleep(backoff)
		} else {
			log.Printf("[worker-%02d] FAILED after %d attempts: %v", workerID, maxRetries, err)
			return err
		}
	}

	return fmt.Errorf("max retries exceeded")
}

func worker(ctx context.Context, id int, tikvClient *txnkv.Client, prefix []byte, enable1PC bool,
	entries <-chan []Entry, successCounter *int64, failCounter *int64, lastEntry *Entry, mu *sync.Mutex, wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range entries {
		if ctx.Err() != nil {
			return
		}

		err := commitBatch(ctx, tikvClient, prefix, enable1PC, batch, id)
		if err != nil {
			atomic.AddInt64(failCounter, int64(len(batch)))
			continue
		}

		// Track progress (last entry in batch)
		last := batch[len(batch)-1]
		mu.Lock()
		if last.Dirhash > lastEntry.Dirhash ||
			(last.Dirhash == lastEntry.Dirhash && last.Name > lastEntry.Name) {
			*lastEntry = last
		}
		mu.Unlock()

		atomic.AddInt64(successCounter, int64(len(batch)))
	}
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
		fmt.Println("  # Parallel migration with 5 instances")
		fmt.Println("  seaweed-pg2tikv --pg-config=pg.toml --tikv-config=tikv.toml --table=rootseek-heic \\")
		fmt.Println("          --partition-mod=5 --partition-id=0 --state=state_0.json")
		os.Exit(1)
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
	state := loadState(actualStateFile)
	if state.Table != "" && state.Table != *table {
		log.Printf("Warning: state file is for table %q, resetting for %q", state.Table, *table)
		state = &State{StartedAt: time.Now().UTC().Format(time.RFC3339)}
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
	var successCounter int64 = state.TotalMigrated
	var failCounter int64 = 0
	var lastEntry Entry
	lastEntry.Dirhash = state.LastDirhash
	lastEntry.Name = state.LastName
	var mu sync.Mutex

	entryChan := make(chan []Entry, *workers*2)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go worker(ctx, i, tikvClient, prefix, enable1PC, entryChan, &successCounter, &failCounter, &lastEntry, &mu, &wg)
	}

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
				success := atomic.LoadInt64(&successCounter)
				failed := atomic.LoadInt64(&failCounter)
				rate := float64(success-lastCount) / 10.0
				lastCount = success
				elapsed := time.Since(startTime)

				mu.Lock()
				le := lastEntry
				mu.Unlock()

				log.Printf("Progress: %d OK, %d FAILED (%.0f/sec) | elapsed: %v | dirhash: %d",
					success, failed, rate, elapsed.Round(time.Second), le.Dirhash)

				// Save state
				state.TotalMigrated = success
				state.LastDirhash = le.Dirhash
				state.LastName = le.Name
				saveState(actualStateFile, state)
			}
		}
	}()

	// Query with resume and partitioning
	query := fmt.Sprintf(`
		SELECT dirhash, directory, name, meta
		FROM %q
		WHERE (dirhash, name) > ($1, $2)
		  AND mod(abs(dirhash), $3) = $4
		ORDER BY dirhash, name`,
		*table)

	log.Printf("Starting from dirhash=%d name=%q", state.LastDirhash, state.LastName)

	rows, err := db.QueryContext(ctx, query,
		state.LastDirhash, state.LastName,
		*partitionMod, *partitionID)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	batch := make([]Entry, 0, *batchSize)
	rowCount := int64(0)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for rows.Next() {
			var e Entry
			if err := rows.Scan(&e.Dirhash, &e.Directory, &e.Name, &e.Meta); err != nil {
				log.Printf("Scan error: %v", err)
				continue
			}

			rowCount++
			batch = append(batch, e)
			if len(batch) >= *batchSize {
				select {
				case entryChan <- batch:
					batch = make([]Entry, 0, *batchSize)
				case <-ctx.Done():
					return
				}
			}
		}
		if len(batch) > 0 {
			entryChan <- batch
		}
	}()

	// Wait for completion or interrupt
	select {
	case <-sigChan:
		log.Println("Interrupt received, saving state...")
		cancel()
	case <-done:
		log.Printf("All %d rows read from Postgres, waiting for workers...", rowCount)
	}

	close(entryChan)
	wg.Wait()

	// Final state save
	finalSuccess := atomic.LoadInt64(&successCounter)
	finalFailed := atomic.LoadInt64(&failCounter)
	mu.Lock()
	state.TotalMigrated = finalSuccess
	state.LastDirhash = lastEntry.Dirhash
	state.LastName = lastEntry.Name
	mu.Unlock()
	saveState(actualStateFile, state)

	log.Println("=== Migration Complete ===")
	log.Printf("Rows read from Postgres: %d", rowCount)
	log.Printf("Successfully written to TiKV: %d", finalSuccess)
	log.Printf("Failed to write: %d", finalFailed)
	log.Printf("State saved to: %s", actualStateFile)
	log.Printf("seaweed-pg2tikv version %s", Version)
}
