// seaweed-count-keys - Count TiKV keys with a given prefix
package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
)

var (
	pdAddrs   = flag.String("pd", "localhost:2379", "PD addresses (comma-separated)")
	prefix    = flag.String("prefix", "", "Key prefix to count")
	caPath    = flag.String("ca", "", "CA certificate path")
	certPath  = flag.String("cert", "", "Client certificate path")
	keyPath   = flag.String("key", "", "Client key path")
)

func main() {
	flag.Parse()

	if *prefix == "" {
		fmt.Println("seaweed-count-keys - Count TiKV keys with a given prefix")
		fmt.Println()
		fmt.Println("Usage: seaweed-count-keys --pd=<pd-addrs> --prefix=<key-prefix>")
		fmt.Println()
		flag.PrintDefaults()
		return
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
		if len(key) < len(prefixBytes) {
			break
		}
		if string(key[:len(prefixBytes)]) != *prefix {
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
}
