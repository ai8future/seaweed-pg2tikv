// seaweed-pg2tikv-audit - Verify migration by comparing Postgres to TiKV
// Version 1.0.4
package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/lib/pq"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
)

const Version = "1.0.4"
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
	tikvConfigFile = flag.String("tikv-config", "", "Path to tikv filer.toml")
	table          = flag.String("table", "filemeta", "Postgres table name to audit")
	mode           = flag.String("mode", "sample", "Audit mode: 'sample' (random sample) or 'complete' (full scan)")
	sampleSize     = flag.Int("sample-size", 10000, "Number of random rows to check in sample mode")
	workers        = flag.Int("workers", 10, "Parallel verification workers")
	showMissing    = flag.Bool("show-missing", false, "Print details of missing/mismatched entries")
	showVersion    = flag.Bool("version", false, "Show version and exit")
	maxMismatches  = flag.Int("max-mismatches", 100, "Stop after this many mismatches (0=unlimited)")
	verbose        = flag.Bool("verbose", false, "Show detailed byte comparison for mismatches")
)

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

// ========== Entry ==========

type Entry struct {
	Dirhash   int64
	Directory string
	Name      string
	Meta      []byte
}

type AuditResult struct {
	Found    bool
	Match    bool
	Entry    Entry
	TiKVMeta []byte
}

func compareMeta(pg, tikv []byte) string {
	if len(pg) != len(tikv) {
		return fmt.Sprintf("length differs: pg=%d tikv=%d", len(pg), len(tikv))
	}

	// Find first differing byte
	firstDiff := -1
	diffCount := 0
	for i := 0; i < len(pg); i++ {
		if pg[i] != tikv[i] {
			if firstDiff == -1 {
				firstDiff = i
			}
			diffCount++
		}
	}

	if firstDiff == -1 {
		return "identical (false positive?)"
	}

	// Show context around first difference
	start := firstDiff - 8
	if start < 0 {
		start = 0
	}
	end := firstDiff + 16
	if end > len(pg) {
		end = len(pg)
	}

	return fmt.Sprintf("%d bytes differ, first at offset %d: pg[%d:%d]=%x tikv[%d:%d]=%x",
		diffCount, firstDiff, start, end, pg[start:end], start, end, tikv[start:end])
}

// ========== Audit Worker ==========

func auditWorker(ctx context.Context, id int, tikvClient *txnkv.Client, prefix []byte,
	entries <-chan Entry, results chan<- AuditResult, wg *sync.WaitGroup) {
	defer wg.Done()

	for entry := range entries {
		if ctx.Err() != nil {
			return
		}

		key := generateTiKVKey(prefix, entry.Directory, entry.Name)

		txn, err := tikvClient.Begin()
		if err != nil {
			results <- AuditResult{Found: false, Match: false, Entry: entry}
			continue
		}

		val, err := txn.Get(ctx, key)
		txn.Rollback()

		if err != nil || val == nil {
			results <- AuditResult{Found: false, Match: false, Entry: entry}
			continue
		}

		match := bytes.Equal(val, entry.Meta)
		results <- AuditResult{Found: true, Match: match, Entry: entry, TiKVMeta: val}
	}
}

// ========== Main ==========

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("seaweed-pg2tikv-audit version %s\n", Version)
		return
	}

	if *pgConfigFile == "" || *tikvConfigFile == "" {
		fmt.Printf("seaweed-pg2tikv-audit version %s - Verify Postgres to TiKV migration\n", Version)
		fmt.Println()
		fmt.Println("Usage: seaweed-pg2tikv-audit --pg-config=<filer.toml> --tikv-config=<filer.toml> --table=<table>")
		fmt.Println()
		fmt.Println("Modes:")
		fmt.Println("  --mode=sample    Random sample verification (default, fast)")
		fmt.Println("  --mode=complete  Full table verification (slow but thorough)")
		fmt.Println()
		fmt.Println("Options:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	if *mode != "sample" && *mode != "complete" {
		log.Fatalf("Invalid mode %q. Use 'sample' or 'complete'", *mode)
	}

	// Validate table name to prevent SQL injection
	if !validTableName.MatchString(*table) {
		log.Fatalf("Invalid table name %q: must start with letter/underscore and contain only alphanumeric, underscore, or hyphen", *table)
	}

	// Parse Postgres config
	var pgConfig FilerConfig
	if _, err := toml.DecodeFile(*pgConfigFile, &pgConfig); err != nil {
		log.Fatalf("Failed to parse postgres config: %v", err)
	}

	// Parse TiKV config
	var tikvConfig FilerConfig
	if _, err := toml.DecodeFile(*tikvConfigFile, &tikvConfig); err != nil {
		log.Fatalf("Failed to parse tikv config: %v", err)
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

	log.Printf("seaweed-pg2tikv-audit version %s", Version)
	log.Println("=== Configuration ===")
	log.Printf("Postgres: %s@%s:%d/%s (table: %s)",
		pgConfig.Postgres2.Username,
		pgConfig.Postgres2.Hostname,
		pgConfig.Postgres2.Port,
		pgConfig.Postgres2.Database,
		*table)
	log.Printf("TiKV PD:  %s (prefix: %q)", tikvConfig.TiKV.PDAddrs, tikvConfig.TiKV.KeyPrefix)
	log.Printf("Mode: %s, Workers: %d", *mode, *workers)
	if *mode == "sample" {
		log.Printf("Sample size: %d", *sampleSize)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to Postgres
	db, err := sql.Open("postgres", pgConfig.Postgres2.ConnectionString())
	if err != nil {
		log.Fatalf("Postgres connection failed: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Postgres ping failed: %v", err)
	}
	log.Println("Connected to Postgres")

	// Get row count (table name validated above)
	var totalRows int64
	err = db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %q", *table)).Scan(&totalRows)
	if err != nil {
		log.Fatalf("Failed to count rows: %v", err)
	}
	log.Printf("Total rows in Postgres: %d", totalRows)

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

	// Channels
	entryChan := make(chan Entry, *workers*2)
	resultChan := make(chan AuditResult, *workers*2)

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go auditWorker(ctx, i, tikvClient, prefix, entryChan, resultChan, &wg)
	}

	// Counters
	var checked, found, matched, missing, mismatched int64
	startTime := time.Now()

	// Result collector
	var collectorWg sync.WaitGroup
	collectorWg.Add(1)
	go func() {
		defer collectorWg.Done()
		for result := range resultChan {
			atomic.AddInt64(&checked, 1)
			if result.Found {
				atomic.AddInt64(&found, 1)
				if result.Match {
					atomic.AddInt64(&matched, 1)
				} else {
					m := atomic.AddInt64(&mismatched, 1)
					if *showMissing {
						if *verbose {
							log.Printf("MISMATCH #%d: %s%s", m, result.Entry.Directory, result.Entry.Name)
							log.Printf("  Details: %s", compareMeta(result.Entry.Meta, result.TiKVMeta))
						} else {
							log.Printf("MISMATCH: %s%s (pg_meta_len=%d, tikv_meta_len=%d)",
								result.Entry.Directory, result.Entry.Name,
								len(result.Entry.Meta), len(result.TiKVMeta))
						}
					}
					if *maxMismatches > 0 && m >= int64(*maxMismatches) {
						log.Printf("Reached max mismatches (%d), stopping early", *maxMismatches)
					}
				}
			} else {
				atomic.AddInt64(&missing, 1)
				if *showMissing {
					log.Printf("MISSING: %s/%s", result.Entry.Directory, result.Entry.Name)
				}
			}

			c := atomic.LoadInt64(&checked)
			if c%10000 == 0 {
				elapsed := time.Since(startTime)
				rate := float64(c) / elapsed.Seconds()
				log.Printf("Progress: %d checked, %d found, %d matched, %d missing, %d mismatched (%.0f/sec)",
					c, atomic.LoadInt64(&found), atomic.LoadInt64(&matched),
					atomic.LoadInt64(&missing), atomic.LoadInt64(&mismatched), rate)
			}
		}
	}()

	// Build query based on mode
	// Table name has been validated against SQL injection
	var query string
	if *mode == "sample" {
		// Random sample using ORDER BY RANDOM()
		query = fmt.Sprintf(`
			SELECT dirhash, directory, name, meta
			FROM %q
			ORDER BY RANDOM()
			LIMIT %d`,
			*table, *sampleSize)
		log.Printf("Running random sample of %d rows...", *sampleSize)
	} else {
		// Complete scan
		query = fmt.Sprintf(`
			SELECT dirhash, directory, name, meta
			FROM %q
			ORDER BY dirhash, name`,
			*table)
		log.Printf("Running complete table scan...")
	}

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.Dirhash, &e.Directory, &e.Name, &e.Meta); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}
		rowCount++
		entryChan <- e
	}

	close(entryChan)
	wg.Wait()
	close(resultChan)
	collectorWg.Wait()

	// Final report
	elapsed := time.Since(startTime)
	finalChecked := atomic.LoadInt64(&checked)
	finalFound := atomic.LoadInt64(&found)
	finalMatched := atomic.LoadInt64(&matched)
	finalMissing := atomic.LoadInt64(&missing)
	finalMismatched := atomic.LoadInt64(&mismatched)

	log.Println()
	log.Println("=== Audit Complete ===")
	log.Printf("Mode: %s", *mode)
	log.Printf("Postgres rows: %d", totalRows)
	log.Printf("Rows checked: %d", finalChecked)
	log.Printf("Found in TiKV: %d (%.2f%%)", finalFound, float64(finalFound)/float64(finalChecked)*100)
	log.Printf("Matched exactly: %d (%.2f%%)", finalMatched, float64(finalMatched)/float64(finalChecked)*100)
	log.Printf("Missing from TiKV: %d (%.2f%%)", finalMissing, float64(finalMissing)/float64(finalChecked)*100)
	log.Printf("Mismatched data: %d (%.2f%%)", finalMismatched, float64(finalMismatched)/float64(finalChecked)*100)
	log.Printf("Time: %v", elapsed.Round(time.Second))
	log.Printf("seaweed-pg2tikv-audit version %s", Version)

	// Exit code based on results
	if finalMissing > 0 || finalMismatched > 0 {
		log.Println()
		log.Println("AUDIT FAILED - Migration incomplete or data mismatch detected")
		os.Exit(1)
	} else {
		log.Println()
		log.Println("AUDIT PASSED - All checked rows found and matched")
		os.Exit(0)
	}
}
