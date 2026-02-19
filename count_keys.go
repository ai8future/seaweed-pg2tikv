// seaweed-count-keys - Count (and optionally delete) TiKV keys with a given prefix
// Version 1.2.0
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
)

const Version = "1.2.0"

var (
	pdAddrs     = flag.String("pd", "localhost:2379", "PD addresses (comma-separated)")
	prefix      = flag.String("prefix", "", "Key prefix to count or delete")
	caPath      = flag.String("ca", "", "CA certificate path")
	certPath    = flag.String("cert", "", "Client certificate path")
	keyPath     = flag.String("key", "", "Client key path")
	deleteKeys  = flag.Bool("delete", false, "Delete all keys with the given prefix (use with caution!)")
	batchSize   = flag.Int("batch", 5000, "Batch size for delete operations")
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

	if *deleteKeys {
		deleteAllKeys(client)
	} else {
		countAllKeys(client)
	}
}

func countAllKeys(client *txnkv.Client) {
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

func deleteAllKeys(client *txnkv.Client) {
	log.Printf("DELETING all keys with prefix: %q (batch size: %d)", *prefix, *batchSize)

	ctx := context.Background()
	prefixBytes := []byte(*prefix)

	totalDeleted := int64(0)
	startTime := time.Now()

	for {
		// Scan a batch of keys
		scanTxn, err := client.Begin()
		if err != nil {
			log.Fatalf("Failed to begin scan transaction: %v", err)
		}

		iter, err := scanTxn.Iter(prefixBytes, nil)
		if err != nil {
			scanTxn.Rollback()
			log.Fatalf("Failed to create iterator: %v", err)
		}

		keys := make([][]byte, 0, *batchSize)
		for iter.Valid() && len(keys) < *batchSize {
			key := iter.Key()
			if !bytes.HasPrefix(key, prefixBytes) {
				break
			}
			keys = append(keys, append([]byte(nil), key...))
			if err := iter.Next(); err != nil {
				break
			}
		}
		iter.Close()
		scanTxn.Rollback()

		if len(keys) == 0 {
			break
		}

		// Delete the batch
		delTxn, err := client.Begin()
		if err != nil {
			log.Fatalf("Failed to begin delete transaction: %v", err)
		}

		for _, key := range keys {
			if err := delTxn.Delete(key); err != nil {
				delTxn.Rollback()
				log.Fatalf("Failed to delete key: %v", err)
			}
		}

		if err := delTxn.Commit(ctx); err != nil {
			delTxn.Rollback()
			log.Fatalf("Failed to commit delete batch: %v", err)
		}

		totalDeleted += int64(len(keys))
		elapsed := time.Since(startTime)
		rate := float64(totalDeleted) / elapsed.Seconds()
		log.Printf("Deleted %d keys so far (%.0f/sec)...", totalDeleted, rate)
	}

	elapsed := time.Since(startTime)
	log.Printf("=== Delete Complete ===")
	log.Printf("Prefix: %q", *prefix)
	log.Printf("Total keys deleted: %d", totalDeleted)
	log.Printf("Time: %v", elapsed.Round(time.Second))
	log.Printf("seaweed-count-keys version %s", Version)
}
