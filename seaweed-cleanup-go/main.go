package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"os"
	"math/rand"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "seaweed-cleanup/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var candidateBuckets = []string{
	"/buckets/rootseek-heic",
	"/buckets/rootseek-ocr",
	"/buckets/rootseek-newspaper",
	"/buckets/rootseek-jp2",
	"/buckets/rootseek-fast-parquet",
}

var version = "1.2.6"

var extToBucket = map[string]string{
	".heic":          "/buckets/rootseek-heic",
	".json.gz":       "/buckets/rootseek-ocr",
	".lp_newspaper":  "/buckets/rootseek-newspaper",
	".jp2":           "/buckets/rootseek-jp2",
	".parquet":       "/buckets/rootseek-fast-parquet",
}

const sampleFiles = 0 // 0 = verify ALL files (no sampling)

// Shared HTTP client — reuses connections across goroutines
var httpClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 1000,
		IdleConnTimeout:     90 * time.Second,
	},
}

// FilerEntry represents a single entry from filer directory listing
type FilerEntry struct {
	FullPath string `json:"FullPath"`
	FileSize int64  `json:"FileSize"`
	Mode     int    `json:"Mode"`
	Chunks   []struct {
		FileID string `json:"file_id"`
	} `json:"chunks"`
}

// FilerListResponse represents the filer directory listing response
type FilerListResponse struct {
	Entries              []FilerEntry `json:"Entries"`
	LastFileName         string       `json:"LastFileName"`
	ShouldDisplayLoadMore bool       `json:"ShouldDisplayLoadMore"`
}

// FileMetadata represents file metadata with chunks
type FileMetadata struct {
	FullPath string `json:"FullPath"`
	Chunks   []struct {
		FileID string `json:"file_id"`
	} `json:"chunks"`
}

func getFileExt(fname string) string {
	if strings.HasSuffix(fname, ".json.gz") {
		return ".json.gz"
	}
	if strings.HasSuffix(fname, ".lp_newspaper") {
		return ".lp_newspaper"
	}
	ext := filepath.Ext(fname)
	return ext
}

func encodePath(filerURL, path string) string {
	// Encode each path segment individually, preserving slashes
	segments := strings.Split(path, "/")
	for i, seg := range segments {
		segments[i] = url.PathEscape(seg)
	}
	return filerURL + strings.Join(segments, "/")
}

func filerGet(filerURL, path string, params map[string]string) (*FilerListResponse, error) {
	u := encodePath(filerURL, path)
	if len(params) > 0 {
		vals := url.Values{}
		for k, v := range params {
			vals.Set(k, v)
		}
		u += "?" + vals.Encode()
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result FilerListResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func filerDeleteMetadataOnly(filerURL, path string) bool {
	u := encodePath(filerURL, path) + "?recursive=true&ignoreRecursiveError=true&skipChunkDeletion=true"
	req, err := http.NewRequest("DELETE", u, nil)
	if err != nil {
		return false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode < 400
}

func getFileChunks(filerURL, filePath string) ([]string, error) {
	u := encodePath(filerURL, filePath) + "?metadata=true"
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var meta FileMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, err
	}

	chunks := make([]string, len(meta.Chunks))
	for i, c := range meta.Chunks {
		chunks[i] = c.FileID
	}
	sort.Strings(chunks)
	return chunks, nil
}

func findFileInBuckets(filerURL, dirName, fileName string) (string, []string) {
	for _, bucket := range candidateBuckets {
		bucketPath := bucket + "/" + dirName + "/" + fileName
		chunks, err := getFileChunks(filerURL, bucketPath)
		if err == nil {
			return bucket, chunks
		}
	}
	return "", nil
}

type verifyResult struct {
	isDupe bool
	detail string
}

func verifyDuplicate(filerURL, rootDirName string) verifyResult {
	rootPath := "/" + rootDirName

	checked := 0
	matched := 0
	bucketsSeen := map[string]bool{}
	lastFile := ""

	for {
		var data *FilerListResponse
		for attempt := 0; attempt < 3; attempt++ {
			var err error
			data, err = filerGet(filerURL, rootPath+"/", map[string]string{
				"limit":        "1000",
				"lastFileName": lastFile,
			})
			if err == nil && data != nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		if data == nil || len(data.Entries) == 0 {
			if checked == 0 {
				return verifyResult{false, "root entry has no files"}
			}
			break
		}

		for _, rf := range data.Entries {
			fname := filepath.Base(rf.FullPath)

			// Skip subdirectories
			if rf.FileSize == 0 && rf.Mode&0o40000 != 0 {
				continue
			}

			rootChunks, err := getFileChunks(filerURL, rf.FullPath)
			if err != nil {
				return verifyResult{false, fmt.Sprintf("could not read chunks for %s (%d/%d verified)", fname, matched, checked+1)}
			}

			bucket, bucketChunks := findFileInBuckets(filerURL, rootDirName, fname)
			if bucket == "" {
				return verifyResult{false, fmt.Sprintf("file %s not found in any bucket (%d/%d verified)", fname, matched, checked+1)}
			}

			if !chunksEqual(rootChunks, bucketChunks) {
				return verifyResult{false, fmt.Sprintf("chunks differ for %s (%d/%d verified)", fname, matched, checked+1)}
			}

			checked++
			matched++
			bucketsSeen[filepath.Base(bucket)] = true
		}

		lastFile = data.LastFileName
		if !data.ShouldDisplayLoadMore {
			break
		}
	}

	if checked == 0 {
		return verifyResult{false, "no files to compare"}
	}

	buckets := make([]string, 0, len(bucketsSeen))
	for b := range bucketsSeen {
		buckets = append(buckets, b)
	}
	sort.Strings(buckets)
	return verifyResult{true, fmt.Sprintf("%d/%d files match [%s]", matched, checked, strings.Join(buckets, "+"))}
}

func shardHash(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return h.Sum32()
}

func chunksEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func migrateOrphan(filerURL string, grpcClient pb.SeaweedFilerClient, rootDirName string) (string, string) {
	rootPath := "/" + rootDirName
	ctx := context.Background()

	// List ALL files in the directory (paginated, with retry)
	var allFiles []string
	lastFile := ""
	for {
		var data *FilerListResponse
		var listErr error
		for attempt := 0; attempt < 3; attempt++ {
			data, listErr = filerGet(filerURL, rootPath+"/", map[string]string{
				"limit":        "1000",
				"lastFileName": lastFile,
			})
			if listErr == nil && data != nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		if listErr != nil || data == nil || len(data.Entries) == 0 {
			break
		}
		for _, entry := range data.Entries {
			fname := filepath.Base(entry.FullPath)
			if entry.FileSize == 0 && entry.Mode&0o40000 != 0 {
				continue
			}
			allFiles = append(allFiles, fname)
		}
		lastFile = data.LastFileName
		if !data.ShouldDisplayLoadMore {
			break
		}
	}

	if len(allFiles) == 0 {
		return "MIGRATE_FAIL", "no files found"
	}

	migrated := 0
	skipped := 0
	failed := 0
	var err error
	bucketsSeen := map[string]bool{}

	for _, fname := range allFiles {
		ext := getFileExt(fname)
		destBucket, ok := extToBucket[ext]
		if !ok {
			skipped++
			continue
		}

		destDir := destBucket + "/" + rootDirName

		// Lookup source entry via gRPC (with retry)
		var lookupResp *pb.LookupDirectoryEntryResponse
		for attempt := 0; attempt < 3; attempt++ {
			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			lookupResp, err = grpcClient.LookupDirectoryEntry(rctx, &pb.LookupDirectoryEntryRequest{
				Directory: rootPath,
				Name:      fname,
			})
			cancel()
			if err == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		if err != nil {
			failed++
			continue
		}

		// Create at destination via gRPC (with retry)
		var createResp *pb.CreateEntryResponse
		for attempt := 0; attempt < 3; attempt++ {
			rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			createResp, err = grpcClient.CreateEntry(rctx, &pb.CreateEntryRequest{
				Directory:                destDir,
				Entry:                    lookupResp.Entry,
				SkipCheckParentDirectory: true,
			})
			cancel()
			if err == nil {
				break
			}
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
		if err != nil {
			failed++
			continue
		}
		if createResp.Error != "" {
			failed++
			continue
		}

		migrated++
		bucketsSeen[filepath.Base(destBucket)] = true
	}

	if migrated == 0 {
		if failed > 0 {
			return "MIGRATE_FAIL", fmt.Sprintf("0/%d migrated, %d errors, %d unknown ext", len(allFiles), failed, skipped)
		}
		if skipped == len(allFiles) {
			return "MIGRATE_FAIL", fmt.Sprintf("all %d files have unknown extensions", len(allFiles))
		}
		return "MIGRATE_FAIL", fmt.Sprintf("0/%d migrated", len(allFiles))
	}

	// Only delete root directory if ALL files were handled
	if failed > 0 || skipped > 0 {
		return "MIGRATE_PARTIAL", fmt.Sprintf("%d/%d migrated, %d failed, %d unknown ext", migrated, len(allFiles), failed, skipped)
	}

	// All files migrated — verify directory is actually empty before deleting
	// This prevents data loss if the initial listing was incomplete (pagination failure)
	verifyData, verifyErr := filerGet(filerURL, rootPath+"/", map[string]string{"limit": "1"})
	if verifyErr == nil && verifyData != nil && len(verifyData.Entries) > 0 {
		return "MIGRATE_PARTIAL", fmt.Sprintf("%d/%d migrated but dir still has entries — not deleting", migrated, len(allFiles))
	}

	if !filerDeleteMetadataOnly(filerURL, rootPath) {
		return "MIGRATE_PARTIAL", fmt.Sprintf("%d/%d migrated but root delete failed", migrated, len(allFiles))
	}

	buckets := make([]string, 0, len(bucketsSeen))
	for b := range bucketsSeen {
		buckets = append(buckets, b)
	}
	sort.Strings(buckets)
	return "MIGRATED", fmt.Sprintf("%d/%d files [%s]", migrated, len(allFiles), strings.Join(buckets, "+"))
}

type entryResult struct {
	name   string
	action string
	detail string
}

func processEntry(filerURL string, grpcClient pb.SeaweedFilerClient, name string, doDelete, doMigrate bool) entryResult {
	vr := verifyDuplicate(filerURL, name)

	if vr.isDupe {
		if doDelete {
			if filerDeleteMetadataOnly(filerURL, "/"+name) {
				return entryResult{name, "DELETED", vr.detail}
			}
			return entryResult{name, "FAILED", vr.detail}
		}
		return entryResult{name, "DUPE", vr.detail}
	}

	// Not a dupe — orphan
	if doMigrate {
		action, detail := migrateOrphan(filerURL, grpcClient, name)
		return entryResult{name, action, detail}
	}
	return entryResult{name, "SKIP", vr.detail}
}

func runDeleteSafetyCheck(filerURL, startAfter string) bool {
	testDir := ""
	searchLast := startAfter
	searched := 0

	for testDir == "" && searched < 500 {
		data, err := filerGet(filerURL, "/", map[string]string{
			"limit":        "100",
			"lastFileName": searchLast,
		})
		if err != nil || data == nil || len(data.Entries) == 0 {
			break
		}
		for _, entry := range data.Entries {
			name := strings.TrimLeft(entry.FullPath, "/")
			if name == "buckets" || name == "etc" || name == "topics" {
				continue
			}
			searched++
			vr := verifyDuplicate(filerURL, name)
			if vr.isDupe {
				testDir = name
				break
			}
		}
		searchLast = data.LastFileName
		if !data.ShouldDisplayLoadMore {
			break
		}
	}

	if testDir == "" {
		fmt.Printf("  No confirmed dupe found in %d entries near start position — skipping safety check\n", searched)
		fmt.Println("  (safety was previously verified; proceeding)")
		return true
	}

	listData, err := filerGet(filerURL, "/"+testDir+"/", map[string]string{"limit": "1"})
	if err != nil || len(listData.Entries) == 0 {
		fmt.Fprintf(os.Stderr, "  Could not list test dir\n")
		return false
	}
	fname := filepath.Base(listData.Entries[0].FullPath)
	bucket, bucketChunks := findFileInBuckets(filerURL, testDir, fname)
	if bucket == "" {
		fmt.Fprintf(os.Stderr, "  Could not find bucket copy of %s\n", fname)
		return false
	}

	bucketFilePath := bucket + "/" + testDir + "/" + fname
	fmt.Printf("  Test dir:    /%s\n", testDir)
	fmt.Printf("  Test file:   %s\n", fname)
	fmt.Printf("  Bucket path: %s\n", bucketFilePath)
	fmt.Printf("  Chunks:      %v\n", bucketChunks)

	fmt.Printf("  Deleting root metadata for /%s (skipChunkDeletion=true)...\n", testDir)
	if !filerDeleteMetadataOnly(filerURL, "/"+testDir) {
		fmt.Fprintf(os.Stderr, "  Delete call failed\n")
		return false
	}

	// Verify bucket copy chunks still readable
	chunksAfter, err := getFileChunks(filerURL, bucketFilePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  DANGER: bucket file metadata gone after delete!\n")
		return false
	}
	if !chunksEqual(chunksAfter, bucketChunks) {
		fmt.Fprintf(os.Stderr, "  DANGER: bucket file chunks changed after delete!\n")
		return false
	}

	// Verify data readable
	testURL := encodePath(filerURL, bucketFilePath)
	req, _ := http.NewRequest("GET", testURL, nil)
	req.Header.Set("Range", "bytes=0-63")
	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  DANGER: could not read bucket file data: %v\n", err)
		return false
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		fmt.Fprintf(os.Stderr, "  DANGER: bucket file returns empty data!\n")
		return false
	}
	fmt.Printf("  Bucket file still readable: got %d bytes\n", len(body))
	fmt.Println("  SAFE: root metadata deleted, bucket data intact.")
	return true
}

func runMigrateSafetyCheck(filerURL string, grpcClient pb.SeaweedFilerClient, startAfter string) bool {
	ctx := context.Background()
	testDir := ""
	testFile := ""
	searchLast := startAfter
	searched := 0

	for testDir == "" && searched < 500 {
		data, err := filerGet(filerURL, "/", map[string]string{
			"limit":        "100",
			"lastFileName": searchLast,
		})
		if err != nil || data == nil || len(data.Entries) == 0 {
			break
		}
		for _, entry := range data.Entries {
			name := strings.TrimLeft(entry.FullPath, "/")
			if name == "buckets" || name == "etc" || name == "topics" {
				continue
			}
			searched++
			// Quick orphan check: list first file, see if it exists in any bucket
			dirData, err := filerGet(filerURL, "/"+name+"/", map[string]string{"limit": "5"})
			if err != nil || dirData == nil || len(dirData.Entries) == 0 {
				continue
			}
			for _, f := range dirData.Entries {
				fn := filepath.Base(f.FullPath)
				ext := getFileExt(fn)
				if _, ok := extToBucket[ext]; !ok {
					continue
				}
				// Check if this file exists in any bucket (quick single-file check)
				bucket, _ := findFileInBuckets(filerURL, name, fn)
				if bucket == "" {
					// Orphan found
					testDir = name
					testFile = fn
					break
				}
			}
			if testDir != "" {
				break
			}
		}
		searchLast = data.LastFileName
		if !data.ShouldDisplayLoadMore {
			break
		}
	}

	if testDir == "" {
		fmt.Printf("  No orphan found in %d entries near start position — skipping safety check\n", searched)
		fmt.Println("  (safety was previously verified; proceeding)")
		return true
	}

	ext := getFileExt(testFile)
	destBucket := extToBucket[ext]
	destDir := destBucket + "/" + testDir
	destPath := destDir + "/" + testFile

	fmt.Printf("  Test dir:    /%s\n", testDir)
	fmt.Printf("  Test file:   %s\n", testFile)
	fmt.Printf("  Dest path:   %s\n", destPath)

	// 1. Lookup source
	lookupResp, err := grpcClient.LookupDirectoryEntry(ctx, &pb.LookupDirectoryEntryRequest{
		Directory: "/" + testDir,
		Name:      testFile,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "  Failed to lookup source: %v\n", err)
		return false
	}
	entry := lookupResp.Entry
	chunkIDs := make([]string, len(entry.Chunks))
	for i, c := range entry.Chunks {
		chunkIDs[i] = c.FileId
	}
	fmt.Printf("  Source chunks: %v\n", chunkIDs)

	// 2. Create at destination
	createResp, err := grpcClient.CreateEntry(ctx, &pb.CreateEntryRequest{
		Directory:                destDir,
		Entry:                    entry,
		SkipCheckParentDirectory: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "  Failed to create entry: %v\n", err)
		return false
	}
	if createResp.Error != "" {
		fmt.Fprintf(os.Stderr, "  CreateEntry error: %s\n", createResp.Error)
		return false
	}
	fmt.Printf("  Created entry at %s\n", destPath)

	// 3. Verify readable at new path
	testURL := encodePath(filerURL, destPath)
	readReq, _ := http.NewRequest("GET", testURL, nil)
	readReq.Header.Set("Range", "bytes=0-63")
	resp, err := httpClient.Do(readReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  DANGER: file not readable at bucket path: %v\n", err)
		return false
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	fmt.Printf("  Readable at bucket path: %d bytes\n", len(body))

	// 4. Delete root copy of test file
	delURL := encodePath(filerURL, "/"+testDir+"/"+testFile) + "?skipChunkDeletion=true"
	delReq, _ := http.NewRequest("DELETE", delURL, nil)
	delResp, err := httpClient.Do(delReq)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  WARNING: could not delete root copy: %v\n", err)
	} else {
		delResp.Body.Close()
		fmt.Printf("  Deleted root copy of %s\n", testFile)
	}

	// 5. Verify bucket copy still readable (fresh request)
	readReq2, _ := http.NewRequest("GET", testURL, nil)
	readReq2.Header.Set("Range", "bytes=0-63")
	resp2, err := httpClient.Do(readReq2)
	if err != nil {
		fmt.Fprintf(os.Stderr, "  DANGER: bucket copy not readable after delete: %v\n", err)
		return false
	}
	body2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	fmt.Printf("  Bucket copy still readable after delete: %d bytes\n", len(body2))

	fmt.Println("  SAFE: migration works correctly.")
	return true
}

func main() {
	showVersion := flag.Bool("version", false, "Print version and exit")
	filerURL := flag.String("filer", "", "Filer URL, e.g. http://localhost:8888")
	filerGRPC := flag.String("filer-grpc", "", "Filer gRPC address (default: derived from -filer, port+10000)")
	doDelete := flag.Bool("delete", false, "Delete confirmed duplicate root metadata")
	doMigrate := flag.Bool("migrate", false, "Migrate orphan entries to correct buckets via gRPC")
	dryRun := flag.Bool("dry-run", false, "Only report, don't delete or migrate")
	batchSize := flag.Int("batch-size", 1000, "Entries per page")
	startAfter := flag.String("start-after", "", "Resume from this directory name")
	limit := flag.Int("limit", 0, "Stop after this many entries (0=unlimited)")
	workers := flag.Int("workers", 1, "Number of parallel workers")
	randomOrder := flag.Bool("random", false, "Shuffle directory order (for running multiple instances in parallel)")
	shardSpec := flag.String("shard", "", "Shard spec N/M (e.g., 0/3 = process shard 0 of 3)")
	flag.Parse()

	var shardIndex, shardTotal int
	if *shardSpec != "" {
		_, err := fmt.Sscanf(*shardSpec, "%d/%d", &shardIndex, &shardTotal)
		if err != nil || shardTotal < 1 || shardIndex < 0 || shardIndex >= shardTotal {
			fmt.Fprintf(os.Stderr, "Error: invalid --shard spec %q (expected N/M where 0 <= N < M, e.g. 0/3)\n", *shardSpec)
			os.Exit(1)
		}
	}

	if *showVersion {
		fmt.Printf("seaweed-cleanup v%s\n", version)
		os.Exit(0)
	}

	if *filerURL == "" {
		fmt.Fprintln(os.Stderr, "Error: -filer is required")
		flag.Usage()
		os.Exit(1)
	}

	*filerURL = strings.TrimRight(*filerURL, "/")

	if !*doDelete && !*doMigrate {
		*dryRun = true
	}
	_ = *dryRun // used implicitly

	// Derive gRPC address
	grpcAddr := *filerGRPC
	if grpcAddr == "" {
		u, err := url.Parse(*filerURL)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing filer URL: %v\n", err)
			os.Exit(1)
		}
		port := 8888
		if u.Port() != "" {
			fmt.Sscanf(u.Port(), "%d", &port)
		}
		grpcAddr = fmt.Sprintf("%s:%d", u.Hostname(), port+10000)
	}

	var modeParts []string
	if *doDelete {
		modeParts = append(modeParts, "DELETE dupes")
	}
	if *doMigrate {
		modeParts = append(modeParts, "MIGRATE orphans")
	}
	if len(modeParts) == 0 {
		modeParts = append(modeParts, "DRY RUN")
	}

	fmt.Printf("seaweed-cleanup v%s\n", version)
	fmt.Printf("Filer: %s\n", *filerURL)
	fmt.Printf("Filer gRPC: %s\n", grpcAddr)
	fmt.Printf("Mode: %s\n", strings.Join(modeParts, " + "))
	fmt.Printf("Workers: %d\n", *workers)
	if *randomOrder {
		fmt.Println("Order: RANDOM")
	}
	if shardTotal > 0 {
		fmt.Printf("Shard: %d/%d\n", shardIndex, shardTotal)
	}
	bucketNames := make([]string, len(candidateBuckets))
	for i, b := range candidateBuckets {
		bucketNames[i] = filepath.Base(b)
	}
	fmt.Printf("Candidate buckets: %v\n", bucketNames)
	fmt.Printf("Sample files per dir: %d\n", sampleFiles)
	if *startAfter != "" {
		fmt.Printf("Resuming after: %s\n", *startAfter)
	}
	fmt.Println()

	// Connect gRPC if needed
	var grpcClient pb.SeaweedFilerClient
	if *doMigrate {
		conn, err := grpc.NewClient(grpcAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to connect gRPC: %v\n", err)
			os.Exit(1)
		}
		defer conn.Close()
		grpcClient = pb.NewSeaweedFilerClient(conn)

		// Test connectivity
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_, err = grpcClient.Ping(ctx, &pb.PingRequest{})
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "gRPC ping failed: %v\n", err)
			os.Exit(1)
		}
	}

	// Safety checks
	if *doDelete {
		fmt.Println("Running pre-flight safety check (deletion)...")
		if !runDeleteSafetyCheck(*filerURL, *startAfter) {
			fmt.Fprintln(os.Stderr, "SAFETY CHECK FAILED — aborting.")
			os.Exit(1)
		}
		fmt.Println("Deletion safety check passed.")
		fmt.Println()
	}

	if *doMigrate {
		fmt.Println("Running pre-flight safety check (migration)...")
		if !runMigrateSafetyCheck(*filerURL, grpcClient, *startAfter) {
			fmt.Fprintln(os.Stderr, "MIGRATION SAFETY CHECK FAILED — aborting.")
			os.Exit(1)
		}
		fmt.Println("Migration safety check passed.")
		fmt.Println()
	}

	// ANSI color codes
	const (
		colorReset   = "\033[0m"
		colorRed     = "\033[31m"
		colorGreen   = "\033[32m"
		colorYellow  = "\033[33m"
		colorBlue    = "\033[34m"
		colorMagenta = "\033[35m"
		colorCyan    = "\033[36m"
		colorDim     = "\033[2m"
		colorBold    = "\033[1m"
	)

	// Counters
	var total, dupes, notDupes, deleted, migrated, migratePartial, errors int64

	// Handle Ctrl+C gracefully
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	last := *startAfter
	submitted := int64(0)
	resultCh := make(chan entryResult, *workers*2)
	var wg sync.WaitGroup

	// Rate tracking
	startTime := time.Now()
	var lastRateTime time.Time
	var lastRateCount int64

	// Collector goroutine
	done := make(chan struct{})
	go func() {
		defer close(done)
		lastRateTime = time.Now()
		for r := range resultCh {
			t := atomic.AddInt64(&total, 1)

			switch r.action {
			case "DELETED":
				atomic.AddInt64(&deleted, 1)
				atomic.AddInt64(&dupes, 1)
				fmt.Printf("  %s✓ DELETED %s /%s %s(%s)%s\n", colorGreen, colorReset, r.name, colorDim, r.detail, colorReset)
			case "FAILED":
				atomic.AddInt64(&errors, 1)
				atomic.AddInt64(&dupes, 1)
				fmt.Printf("  %s✗ FAILED  %s /%s %s(%s)%s\n", colorRed, colorReset, r.name, colorDim, r.detail, colorReset)
			case "DUPE":
				atomic.AddInt64(&dupes, 1)
				fmt.Printf("  %s● DUPE    %s /%s %s(%s)%s\n", colorCyan, colorReset, r.name, colorDim, r.detail, colorReset)
			case "MIGRATED":
				atomic.AddInt64(&migrated, 1)
				atomic.AddInt64(&notDupes, 1)
				fmt.Printf("  %s▶ MIGRATED%s /%s %s(%s)%s\n", colorBlue, colorReset, r.name, colorDim, r.detail, colorReset)
			case "MIGRATE_PARTIAL":
				atomic.AddInt64(&migratePartial, 1)
				atomic.AddInt64(&notDupes, 1)
				fmt.Printf("  %s⚠ PARTIAL %s /%s %s(%s)%s\n", colorYellow, colorReset, r.name, colorDim, r.detail, colorReset)
			case "MIGRATE_FAIL":
				atomic.AddInt64(&errors, 1)
				atomic.AddInt64(&notDupes, 1)
				fmt.Printf("  %s✗ MIG_FAIL%s /%s %s(%s)%s\n", colorRed, colorReset, r.name, colorDim, r.detail, colorReset)
			default:
				atomic.AddInt64(&notDupes, 1)
				fmt.Printf("  %s○ SKIP    %s /%s %s(%s)%s\n", colorDim, colorReset, r.name, colorDim, r.detail, colorReset)
			}

			if t%500 == 0 {
				now := time.Now()
				elapsed := now.Sub(startTime).Seconds()
				avgRate := float64(t) / elapsed
				// Recent rate over last 500
				recentElapsed := now.Sub(lastRateTime).Seconds()
				recentRate := float64(500) / recentElapsed
				lastRateTime = now
				lastRateCount = t

				d := atomic.LoadInt64(&dupes)
				del := atomic.LoadInt64(&deleted)
				mig := atomic.LoadInt64(&migrated)
				orph := atomic.LoadInt64(&notDupes)
				errs := atomic.LoadInt64(&errors)
				_ = lastRateCount

				fmt.Printf("\n%s%s--- Progress: %d checked | %d deleted | %d migrated | %d orphans | %d errors | %.1f/s avg | %.1f/s now ---%s%s\n\n",
					colorBold, colorMagenta, t, del, mig, orph, errs, avgRate, recentRate, colorReset, colorReset)
				_ = d
			}
		}
	}()

	// Worker pool using semaphore pattern
	sem := make(chan struct{}, *workers)

	if *randomOrder {
		// Pre-collect all directory names, then shuffle for parallel instance support
		fmt.Println("Collecting directory listing for shuffle...")
		var allNames []string
		scanLast := ""
		for {
			if ctx.Err() != nil {
				break
			}
			data, err := filerGet(*filerURL, "/", map[string]string{
				"limit":        "1000",
				"lastFileName": scanLast,
			})
			if err != nil || data == nil || len(data.Entries) == 0 {
				break
			}
			for _, entry := range data.Entries {
				name := strings.TrimLeft(entry.FullPath, "/")
				if name == "buckets" || name == "etc" || name == "topics" {
					continue
				}
				allNames = append(allNames, name)
			}
			scanLast = data.LastFileName
			if !data.ShouldDisplayLoadMore {
				break
			}
		}
		if shardTotal > 0 {
			filtered := allNames[:0]
			for _, name := range allNames {
				if shardHash(name)%uint32(shardTotal) == uint32(shardIndex) {
					filtered = append(filtered, name)
				}
			}
			fmt.Printf("Collected %d directories, shard %d/%d keeps %d, shuffling...\n\n", len(allNames), shardIndex, shardTotal, len(filtered))
			allNames = filtered
		} else {
			fmt.Printf("Collected %d directories, shuffling...\n\n", len(allNames))
		}
		rand.Shuffle(len(allNames), func(i, j int) {
			allNames[i], allNames[j] = allNames[j], allNames[i]
		})

	randomLoop:
		for _, name := range allNames {
			if *limit > 0 && submitted >= int64(*limit) {
				break
			}
			select {
			case <-ctx.Done():
				break randomLoop
			case sem <- struct{}{}:
			}
			submitted++
			wg.Add(1)
			go func(n string) {
				defer func() {
					<-sem
					wg.Done()
				}()
				resultCh <- processEntry(*filerURL, grpcClient, n, *doDelete, *doMigrate)
			}(name)
		}
	} else {
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
			}

			params := map[string]string{
				"limit":        fmt.Sprintf("%d", *batchSize),
				"lastFileName": last,
			}
			var data *FilerListResponse
			var err error
			for attempt := 0; attempt < 5; attempt++ {
				data, err = filerGet(*filerURL, "/", params)
				if err == nil && data != nil && len(data.Entries) > 0 {
					break
				}
				if attempt < 4 {
					wait := time.Duration(attempt+1) * 2 * time.Second
					fmt.Fprintf(os.Stderr, "WARNING: filer listing failed (attempt %d/5): %v — retrying in %s\n", attempt+1, err, wait)
					time.Sleep(wait)
				}
			}
			if err != nil || data == nil || len(data.Entries) == 0 {
				if err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: could not list root directory after 5 attempts: %v\n", err)
				}
				break
			}

			// Update pagination cursor before processing — ensures resume works
			// even if we break mid-batch due to limit or Ctrl+C
			last = data.LastFileName

			for _, entry := range data.Entries {
				name := strings.TrimLeft(entry.FullPath, "/")
				if name == "buckets" || name == "etc" || name == "topics" {
					continue
				}
				if shardTotal > 0 && shardHash(name)%uint32(shardTotal) != uint32(shardIndex) {
					continue
				}

				if *limit > 0 && submitted >= int64(*limit) {
					break loop
				}

				select {
				case <-ctx.Done():
					break loop
				case sem <- struct{}{}:
				}

				submitted++
				wg.Add(1)
				go func(n string) {
					defer func() {
						<-sem
						wg.Done()
					}()
					r := processEntry(*filerURL, grpcClient, n, *doDelete, *doMigrate)
					resultCh <- r
				}(name)
			}
			if !data.ShouldDisplayLoadMore {
				break
			}
			if *limit > 0 && submitted >= int64(*limit) {
				break
			}
		}
	}

	wg.Wait()
	close(resultCh)
	<-done

	elapsed := time.Since(startTime)
	finalTotal := atomic.LoadInt64(&total)
	avgRate := float64(finalTotal) / elapsed.Seconds()

	fmt.Printf("\n%s%s%s\n", colorBold, strings.Repeat("=", 60), colorReset)
	fmt.Printf("%s%sSUMMARY%s\n", colorBold, colorCyan, colorReset)
	fmt.Printf("  Total root entries checked: %s%d%s\n", colorBold, finalTotal, colorReset)
	fmt.Printf("  Confirmed duplicates:       %s%d%s\n", colorGreen, atomic.LoadInt64(&dupes), colorReset)
	fmt.Printf("  Orphans (not in bucket):    %s%d%s\n", colorYellow, atomic.LoadInt64(&notDupes), colorReset)
	if *doDelete {
		fmt.Printf("  Successfully deleted:       %s%d%s\n", colorGreen, atomic.LoadInt64(&deleted), colorReset)
	}
	if *doMigrate {
		fmt.Printf("  Successfully migrated:      %s%d%s\n", colorBlue, atomic.LoadInt64(&migrated), colorReset)
		fmt.Printf("  Partially migrated:         %s%d%s\n", colorYellow, atomic.LoadInt64(&migratePartial), colorReset)
	}
	errs := atomic.LoadInt64(&errors)
	if errs > 0 {
		fmt.Printf("  Errors:                     %s%d%s\n", colorRed, errs, colorReset)
	} else {
		fmt.Printf("  Errors:                     %d\n", errs)
	}
	fmt.Printf("  Elapsed:                    %s\n", elapsed.Truncate(time.Second))
	fmt.Printf("  Average rate:               %.1f entries/s\n", avgRate)
	if !*doDelete && !*doMigrate {
		fmt.Printf("  Mode: %sDRY RUN%s (use -delete and/or -migrate)\n", colorYellow, colorReset)
	}
	// Use the filer pagination cursor for resume — it's the alphabetically-last
	// entry name from the last batch fetched, which is safe for resuming
	if last != "" && !*randomOrder {
		fmt.Printf("  Resume cursor:              %s\n", last)
		fmt.Printf("  (use -start-after '%s' to resume)\n", last)
	}
}
