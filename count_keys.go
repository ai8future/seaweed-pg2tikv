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
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv"
)

const Version = "1.2.0"

var (
	pdAddrs     = flag.String("pd", "localhost:2379", "PD addresses (comma-separated)")
	prefix      = flag.String("prefix", "", "Key prefix to count or delete")
	caPath      = flag.String("ca", "", "CA certificate path")
	certPath    = flag.String("cert", "", "Client certificate path")
	keyPath     = flag.String("key", "", "Client key path")
	deleteKeys  = flag.Bool("delete", false, "Delete all keys with the given prefix (server-side DeleteRange)")
	showVersion = flag.Bool("version", false, "Show version and exit")
)

// prefixEndKey returns the end key for a prefix scan/delete range.
// It increments the last byte of the prefix to create an exclusive upper bound.
func prefixEndKey(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	// All 0xff bytes — no upper bound possible (extremely unlikely for a text prefix)
	return nil
}

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

	if *deleteKeys {
		deleteAllKeys(addrs)
	} else {
		countAllKeys(addrs)
	}
}

func countAllKeys(addrs []string) {
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

func deleteAllKeys(addrs []string) {
	ctx := context.Background()
	prefixBytes := []byte(*prefix)
	endKey := prefixEndKey(prefixBytes)

	log.Printf("Connecting to TiKV (raw client) at %s", *pdAddrs)

	security := config.Security{
		ClusterSSLCA:   *caPath,
		ClusterSSLCert: *certPath,
		ClusterSSLKey:  *keyPath,
	}

	client, err := rawkv.NewClient(ctx, addrs, security)
	if err != nil {
		log.Fatalf("Failed to connect raw client to TiKV: %v", err)
	}
	defer client.Close()

	log.Printf("DELETING all keys in range [%q, %q) via server-side DeleteRange", *prefix, string(endKey))

	startTime := time.Now()
	err = client.DeleteRange(ctx, prefixBytes, endKey)
	if err != nil {
		log.Fatalf("DeleteRange failed: %v", err)
	}

	elapsed := time.Since(startTime)
	log.Printf("=== Delete Complete ===")
	log.Printf("Prefix: %q", *prefix)
	log.Printf("DeleteRange completed in %v", elapsed.Round(time.Millisecond))
	log.Printf("seaweed-count-keys version %s", Version)
}
