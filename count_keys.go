// seaweed-count-keys - Count TiKV keys with a given prefix
// Version 1.0.4
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
)

const Version = "1.1.5"

var (
	pdAddrs     = flag.String("pd", "localhost:2379", "PD addresses (comma-separated)")
	prefix      = flag.String("prefix", "", "Key prefix to count")
	caPath      = flag.String("ca", "", "CA certificate path")
	certPath    = flag.String("cert", "", "Client certificate path")
	keyPath     = flag.String("key", "", "Client key path")
	showVersion = flag.Bool("version", false, "Show version and exit")
)

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("seaweed-count-keys version %s\n", Version)
		return
	}

	if *prefix == "" {
		fmt.Printf("seaweed-count-keys version %s - Count TiKV keys with a given prefix\n", Version)
		fmt.Println()
		fmt.Println("Usage: seaweed-count-keys --pd=<pd-addrs> --prefix=<key-prefix>")
		fmt.Println()
		flag.PrintDefaults()
		return
	}

	// Validate TLS paths if specified
	if *caPath != "" {
		if _, err := os.Stat(*caPath); err != nil {
			log.Fatalf("TLS CA path not accessible: %v", err)
		}
	}
	if *certPath != "" {
		if _, err := os.Stat(*certPath); err != nil {
			log.Fatalf("TLS cert path not accessible: %v", err)
		}
	}
	if *keyPath != "" {
		if _, err := os.Stat(*keyPath); err != nil {
			log.Fatalf("TLS key path not accessible: %v", err)
		}
	}

	// Configure TLS if provided
	if *caPath != "" {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.Security = config.NewSecurity(*caPath, *certPath, *keyPath, nil)
		})
	}

	addrs := strings.Split(*pdAddrs, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}

	client, err := txnkv.NewClient(addrs)
	if err != nil {
		log.Fatalf("Failed to connect to TiKV: %v", err)
	}
	defer client.Close()

	log.Printf("Connected to TiKV at %s", *pdAddrs)
	log.Printf("Counting keys with prefix: %q", *prefix)

	txn, err := client.Begin()
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	prefixBytes := []byte(*prefix)
	iter, err := txn.Iter(prefixBytes, nil)
	if err != nil {
		log.Fatalf("Failed to create iterator: %v", err)
	}
	defer iter.Close()

	count := int64(0)
	startTime := time.Now()
	lastReport := startTime

	for iter.Valid() {
		key := iter.Key()
		// Use bytes.HasPrefix for efficient comparison (no string allocation)
		if !bytes.HasPrefix(key, prefixBytes) {
			break
		}

		count++

		if time.Since(lastReport) > 5*time.Second {
			elapsed := time.Since(startTime)
			rate := float64(count) / elapsed.Seconds()
			log.Printf("Counted %d keys (%.0f/sec)...", count, rate)
			lastReport = time.Now()
		}

		if err := iter.Next(); err != nil {
			log.Printf("Iterator error: %v", err)
			break
		}
	}

	txn.Rollback()

	elapsed := time.Since(startTime)
	log.Printf("=== Count Complete ===")
	log.Printf("Prefix: %q", *prefix)
	log.Printf("Total keys: %d", count)
	log.Printf("Time: %v", elapsed.Round(time.Second))
	log.Printf("seaweed-count-keys version %s", Version)
}
