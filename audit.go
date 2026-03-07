// seaweed-pg2tikv-audit - Verify migration by comparing Postgres to TiKV
// Version 1.2.2
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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BurntSushi/toml"
	_ "github.com/lib/pq"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
	"google.golang.org/protobuf/encoding/protowire"
)

const Version = "1.2.2"
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
	verbose        = flag.Bool("verbose", false, "Show detailed field-level comparison for mismatches")
	pathPrefix     = flag.String("path-prefix", "", "Path prefix for per-bucket tables (e.g., /buckets/my-bucket)")
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

// applyPathPrefix prepends the bucket path to the directory for per-bucket tables.
func applyPathPrefix(prefix, directory string) string {
	if prefix == "" {
		return directory
	}
	if directory == "/" {
		return prefix
	}
	return prefix + directory
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

// ========== Protobuf Field Analysis ==========

// SeaweedFS Entry protobuf field names
var entryFieldNames = map[int]string{
	1: "name", 2: "is_directory", 3: "attributes", 4: "chunks",
	5: "extended", 7: "hard_link_id", 8: "hard_link_counter",
	9: "content", 10: "remote_entry", 11: "quota",
}

// SeaweedFS FuseAttributes protobuf field names
var attrFieldNames = map[int]string{
	1: "file_size", 2: "mtime", 3: "file_mode", 4: "uid", 5: "gid",
	6: "crtime", 7: "mime", 8: "replication", 9: "collection",
	10: "ttl_sec", 11: "user_name", 12: "group_names",
	13: "symlink_target", 14: "md5", 15: "disk_type",
}

// SeaweedFS FileChunk protobuf field names
var chunkFieldNames = map[int]string{
	1: "file_id", 2: "offset", 3: "size", 4: "modified_ts_ns",
	5: "e_tag", 6: "source_file_id", 7: "fid", 8: "source_fid",
	9: "cipher_key", 10: "is_compressed", 11: "is_chunk_manifest",
}

// SeaweedFS FileId protobuf field names
var fileIdFieldNames = map[int]string{
	1: "volume_id", 2: "file_key", 3: "cookie",
}

// Fields that contain unix timestamps in seconds
var secondTimestampFields = map[string]bool{
	"attributes.mtime": true, "attributes.crtime": true,
}

// Fields that contain unix timestamps in nanoseconds
var nanoTimestampFields = map[string]bool{
	"modified_ts_ns": true,
}

type fieldValue struct {
	wireType protowire.Type
	varint   uint64
	fixed32  uint32
	fixed64  uint64
	bval     []byte
}

func parseProtoFields(data []byte) map[int][]fieldValue {
	fields := make(map[int][]fieldValue)
	for len(data) > 0 {
		num, wtype, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			break
		}
		data = data[tagLen:]

		var fv fieldValue
		fv.wireType = wtype

		switch wtype {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return fields
			}
			fv.varint = v
			data = data[n:]
		case protowire.Fixed32Type:
			v, n := protowire.ConsumeFixed32(data)
			if n < 0 {
				return fields
			}
			fv.fixed32 = v
			data = data[n:]
		case protowire.Fixed64Type:
			v, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return fields
			}
			fv.fixed64 = v
			data = data[n:]
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return fields
			}
			fv.bval = append([]byte{}, v...)
			data = data[n:]
		default:
			return fields
		}

		fields[int(num)] = append(fields[int(num)], fv)
	}
	return fields
}

func fieldValuesEqual(a, b []fieldValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].wireType != b[i].wireType {
			return false
		}
		switch a[i].wireType {
		case protowire.VarintType:
			if a[i].varint != b[i].varint {
				return false
			}
		case protowire.Fixed32Type:
			if a[i].fixed32 != b[i].fixed32 {
				return false
			}
		case protowire.Fixed64Type:
			if a[i].fixed64 != b[i].fixed64 {
				return false
			}
		case protowire.BytesType:
			if !bytes.Equal(a[i].bval, b[i].bval) {
				return false
			}
		}
	}
	return true
}

func sortedFieldNums(maps ...map[int][]fieldValue) []int {
	seen := make(map[int]bool)
	for _, m := range maps {
		for k := range m {
			seen[k] = true
		}
	}
	keys := make([]int, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// decodeMapEntries decodes protobuf map<string,bytes> repeated field values into a Go map.
func decodeMapEntries(entries []fieldValue) map[string][]byte {
	result := make(map[string][]byte)
	for _, e := range entries {
		if e.wireType != protowire.BytesType {
			continue
		}
		fields := parseProtoFields(e.bval)
		if keyVals, ok := fields[1]; ok && len(keyVals) > 0 && keyVals[0].wireType == protowire.BytesType {
			key := string(keyVals[0].bval)
			var val []byte
			if valVals, ok := fields[2]; ok && len(valVals) > 0 && valVals[0].wireType == protowire.BytesType {
				val = valVals[0].bval
			}
			result[key] = val
		}
	}
	return result
}

// diffChunks compares repeated FileChunk values and returns sub-field diff names.
func diffChunks(pgChunks, tikvChunks []fieldValue) []string {
	var diffs []string

	if len(pgChunks) != len(tikvChunks) {
		diffs = append(diffs, fmt.Sprintf("chunks (count: pg=%d tikv=%d)", len(pgChunks), len(tikvChunks)))
	}

	minLen := len(pgChunks)
	if len(tikvChunks) < minLen {
		minLen = len(tikvChunks)
	}

	for i := 0; i < minLen; i++ {
		if pgChunks[i].wireType != protowire.BytesType || tikvChunks[i].wireType != protowire.BytesType {
			diffs = append(diffs, fmt.Sprintf("chunks[%d]", i))
			continue
		}
		prefix := fmt.Sprintf("chunks[%d].", i)
		subDiffs := diffFieldsGeneric(pgChunks[i].bval, tikvChunks[i].bval, chunkFieldNames, prefix)
		if len(subDiffs) > 0 {
			diffs = append(diffs, subDiffs...)
		} else if !bytes.Equal(pgChunks[i].bval, tikvChunks[i].bval) {
			diffs = append(diffs, fmt.Sprintf("chunks[%d] (encoding)", i))
		}
	}

	return diffs
}

// diffExtended compares extended map<string,bytes> fields.
func diffExtended(pgExts, tikvExts []fieldValue) []string {
	pgMap := decodeMapEntries(pgExts)
	tikvMap := decodeMapEntries(tikvExts)

	var diffs []string
	allKeys := make(map[string]bool)
	for k := range pgMap {
		allKeys[k] = true
	}
	for k := range tikvMap {
		allKeys[k] = true
	}

	for k := range allKeys {
		pgVal, pgOk := pgMap[k]
		tikvVal, tikvOk := tikvMap[k]
		if pgOk && tikvOk {
			if !bytes.Equal(pgVal, tikvVal) {
				diffs = append(diffs, fmt.Sprintf("extended[%q] (changed)", k))
			}
		} else if !pgOk {
			diffs = append(diffs, fmt.Sprintf("extended[%q] (added in tikv)", k))
		} else if !tikvOk {
			diffs = append(diffs, fmt.Sprintf("extended[%q] (missing in tikv)", k))
		}
	}

	sort.Strings(diffs)
	return diffs
}

// diffFieldsGeneric compares two protobuf blobs field-by-field without recursing into sub-messages.
func diffFieldsGeneric(pgData, tikvData []byte, nameMap map[int]string, prefix string) []string {
	pgFields := parseProtoFields(pgData)
	tikvFields := parseProtoFields(tikvData)

	var diffs []string
	for _, num := range sortedFieldNums(pgFields, tikvFields) {
		name := nameMap[num]
		if name == "" {
			name = fmt.Sprintf("field_%d", num)
		}
		fullName := prefix + name
		pgVals := pgFields[num]
		tikvVals := tikvFields[num]

		if !fieldValuesEqual(pgVals, tikvVals) {
			// Recurse into fid/source_fid (FileId sub-message)
			if (num == 7 || num == 8) &&
				len(pgVals) >= 1 && len(tikvVals) >= 1 &&
				pgVals[0].wireType == protowire.BytesType &&
				tikvVals[0].wireType == protowire.BytesType {
				subDiffs := diffFieldsGeneric(pgVals[0].bval, tikvVals[0].bval, fileIdFieldNames, fullName+".")
				if len(subDiffs) > 0 {
					diffs = append(diffs, subDiffs...)
				} else {
					diffs = append(diffs, fullName+" (encoding)")
				}
			} else {
				diffs = append(diffs, fullName)
			}
		}
	}
	return diffs
}

// diffFields returns the names of differing protobuf fields between two Entry blobs.
func diffFields(pgData, tikvData []byte, nameMap map[int]string, prefix string) []string {
	pgFields := parseProtoFields(pgData)
	tikvFields := parseProtoFields(tikvData)

	var diffs []string
	for _, num := range sortedFieldNums(pgFields, tikvFields) {
		name := nameMap[num]
		if name == "" {
			name = fmt.Sprintf("field_%d", num)
		}
		fullName := prefix + name
		pgVals := pgFields[num]
		tikvVals := tikvFields[num]

		if !fieldValuesEqual(pgVals, tikvVals) {
			switch {
			// Recurse into attributes (Entry field 3)
			case prefix == "" && num == 3 &&
				len(pgVals) >= 1 && len(tikvVals) >= 1 &&
				pgVals[0].wireType == protowire.BytesType &&
				tikvVals[0].wireType == protowire.BytesType:
				subDiffs := diffFields(pgVals[0].bval, tikvVals[0].bval, attrFieldNames, "attributes.")
				if len(subDiffs) > 0 {
					diffs = append(diffs, subDiffs...)
				} else {
					diffs = append(diffs, fullName+" (encoding)")
				}

			// Recurse into chunks (Entry field 4, repeated FileChunk)
			case prefix == "" && num == 4:
				diffs = append(diffs, diffChunks(pgVals, tikvVals)...)

			// Decode extended (Entry field 5, map<string,bytes>)
			case prefix == "" && num == 5:
				diffs = append(diffs, diffExtended(pgVals, tikvVals)...)

			default:
				diffs = append(diffs, fullName)
			}
		}
	}
	sort.Strings(diffs)
	return diffs
}

func formatValues(vals []fieldValue) string {
	if len(vals) == 0 {
		return "<absent>"
	}
	parts := make([]string, len(vals))
	for i, v := range vals {
		switch v.wireType {
		case protowire.VarintType:
			parts[i] = fmt.Sprintf("%d", v.varint)
		case protowire.Fixed32Type:
			parts[i] = fmt.Sprintf("0x%08x", v.fixed32)
		case protowire.Fixed64Type:
			parts[i] = fmt.Sprintf("%d", v.fixed64)
		case protowire.BytesType:
			if len(v.bval) <= 64 && isPrintable(v.bval) {
				parts[i] = fmt.Sprintf("%q", string(v.bval))
			} else if len(v.bval) <= 32 {
				parts[i] = fmt.Sprintf("%x", v.bval)
			} else {
				parts[i] = fmt.Sprintf("(%d bytes)", len(v.bval))
			}
		}
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c < 0x20 || c > 0x7e {
			return false
		}
	}
	return len(b) > 0
}

func formatValuesNamed(fullName string, vals []fieldValue) string {
	if len(vals) == 0 {
		return "<absent>"
	}
	// Check if this is a seconds-based timestamp
	baseName := fullName
	if idx := strings.LastIndex(fullName, "."); idx >= 0 {
		baseName = fullName[idx+1:]
	}
	if secondTimestampFields[fullName] && len(vals) == 1 && vals[0].wireType == protowire.VarintType {
		ts := int64(vals[0].varint)
		t := time.Unix(ts, 0).UTC()
		return fmt.Sprintf("%d (%s)", ts, t.Format("2006-01-02T15:04:05Z"))
	}
	if nanoTimestampFields[baseName] && len(vals) == 1 && vals[0].wireType == protowire.VarintType {
		ns := int64(vals[0].varint)
		t := time.Unix(0, ns).UTC()
		return fmt.Sprintf("%d (%s)", ns, t.Format("2006-01-02T15:04:05.000Z"))
	}
	return formatValues(vals)
}

// detailedFieldDiff returns verbose diff lines with actual values.
func detailedFieldDiff(pgData, tikvData []byte) []string {
	pgEntry := parseProtoFields(pgData)
	tikvEntry := parseProtoFields(tikvData)

	var lines []string

	// --- Attributes sub-fields ---
	var pgAttr, tikvAttr map[int][]fieldValue
	if vals, ok := pgEntry[3]; ok && len(vals) >= 1 && vals[0].wireType == protowire.BytesType {
		pgAttr = parseProtoFields(vals[0].bval)
	}
	if vals, ok := tikvEntry[3]; ok && len(vals) >= 1 && vals[0].wireType == protowire.BytesType {
		tikvAttr = parseProtoFields(vals[0].bval)
	}
	if pgAttr != nil || tikvAttr != nil {
		if pgAttr == nil {
			pgAttr = make(map[int][]fieldValue)
		}
		if tikvAttr == nil {
			tikvAttr = make(map[int][]fieldValue)
		}
		for _, num := range sortedFieldNums(pgAttr, tikvAttr) {
			name := attrFieldNames[num]
			if name == "" {
				name = fmt.Sprintf("field_%d", num)
			}
			fullName := "attributes." + name
			pgVals := pgAttr[num]
			tikvVals := tikvAttr[num]
			if !fieldValuesEqual(pgVals, tikvVals) {
				pgStr := formatValuesNamed(fullName, pgVals)
				tikvStr := formatValuesNamed(fullName, tikvVals)
				lines = append(lines, fmt.Sprintf("  %s: pg=%s tikv=%s", fullName, pgStr, tikvStr))
			}
		}
	}

	// --- Chunks sub-fields ---
	pgChunks := pgEntry[4]
	tikvChunks := tikvEntry[4]
	if len(pgChunks) != len(tikvChunks) {
		lines = append(lines, fmt.Sprintf("  chunks count: pg=%d tikv=%d", len(pgChunks), len(tikvChunks)))
	}
	minChunks := len(pgChunks)
	if len(tikvChunks) < minChunks {
		minChunks = len(tikvChunks)
	}
	for i := 0; i < minChunks; i++ {
		if pgChunks[i].wireType != protowire.BytesType || tikvChunks[i].wireType != protowire.BytesType {
			continue
		}
		pgCF := parseProtoFields(pgChunks[i].bval)
		tikvCF := parseProtoFields(tikvChunks[i].bval)
		for _, num := range sortedFieldNums(pgCF, tikvCF) {
			name := chunkFieldNames[num]
			if name == "" {
				name = fmt.Sprintf("field_%d", num)
			}
			fullName := fmt.Sprintf("chunks[%d].%s", i, name)
			pgVals := pgCF[num]
			tikvVals := tikvCF[num]
			if !fieldValuesEqual(pgVals, tikvVals) {
				// For fid/source_fid, decode the FileId
				if (num == 7 || num == 8) && len(pgVals) >= 1 && len(tikvVals) >= 1 &&
					pgVals[0].wireType == protowire.BytesType && tikvVals[0].wireType == protowire.BytesType {
					lines = append(lines, formatFileIdDiff(fullName, pgVals[0].bval, tikvVals[0].bval)...)
				} else {
					pgStr := formatValuesNamed(fullName, pgVals)
					tikvStr := formatValuesNamed(fullName, tikvVals)
					lines = append(lines, fmt.Sprintf("  %s: pg=%s tikv=%s", fullName, pgStr, tikvStr))
				}
			}
		}
	}

	// --- Extended map entries ---
	pgExts := pgEntry[5]
	tikvExts := tikvEntry[5]
	pgMap := decodeMapEntries(pgExts)
	tikvMap := decodeMapEntries(tikvExts)
	allExtKeys := make(map[string]bool)
	for k := range pgMap {
		allExtKeys[k] = true
	}
	for k := range tikvMap {
		allExtKeys[k] = true
	}
	for k := range allExtKeys {
		pgVal, pgOk := pgMap[k]
		tikvVal, tikvOk := tikvMap[k]
		if pgOk && tikvOk {
			if !bytes.Equal(pgVal, tikvVal) {
				lines = append(lines, fmt.Sprintf("  extended[%q]: pg=%q tikv=%q", k, pgVal, tikvVal))
			}
		} else if !pgOk {
			lines = append(lines, fmt.Sprintf("  extended[%q]: pg=<absent> tikv=%q", k, tikvVal))
		} else {
			lines = append(lines, fmt.Sprintf("  extended[%q]: pg=%q tikv=<absent>", k, pgVal))
		}
	}

	// --- Other top-level fields (skip 3, 4, 5 handled above) ---
	for _, num := range sortedFieldNums(pgEntry, tikvEntry) {
		if num == 3 || num == 4 || num == 5 {
			continue
		}
		name := entryFieldNames[num]
		if name == "" {
			name = fmt.Sprintf("field_%d", num)
		}
		pgVals := pgEntry[num]
		tikvVals := tikvEntry[num]
		if !fieldValuesEqual(pgVals, tikvVals) {
			lines = append(lines, fmt.Sprintf("  %s: pg=%s tikv=%s",
				name, formatValues(pgVals), formatValues(tikvVals)))
		}
	}

	return lines
}

func formatFileIdDiff(prefix string, pgFid, tikvFid []byte) []string {
	pgFields := parseProtoFields(pgFid)
	tikvFields := parseProtoFields(tikvFid)
	var lines []string
	for _, num := range sortedFieldNums(pgFields, tikvFields) {
		name := fileIdFieldNames[num]
		if name == "" {
			name = fmt.Sprintf("field_%d", num)
		}
		fullName := prefix + "." + name
		if !fieldValuesEqual(pgFields[num], tikvFields[num]) {
			lines = append(lines, fmt.Sprintf("  %s: pg=%s tikv=%s",
				fullName, formatValues(pgFields[num]), formatValues(tikvFields[num])))
		}
	}
	return lines
}

func compareMeta(pg, tikv []byte) string {
	if len(pg) != len(tikv) {
		return fmt.Sprintf("length differs: pg=%d tikv=%d", len(pg), len(tikv))
	}

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

		effectiveDir := applyPathPrefix(*pathPrefix, entry.Directory)
		key := generateTiKVKey(prefix, effectiveDir, entry.Name)

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

	// Validate path prefix
	if *pathPrefix != "" {
		if !strings.HasPrefix(*pathPrefix, "/") {
			log.Fatalf("Invalid path-prefix %q: must start with /", *pathPrefix)
		}
		*pathPrefix = strings.TrimRight(*pathPrefix, "/")
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
	if *pathPrefix != "" {
		log.Printf("Path prefix: %s (per-bucket table mode)", *pathPrefix)
	}
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

	// Mismatch field tracking (only written by single collector goroutine)
	mismatchFieldCounts := make(map[string]int)
	mismatchPatternCounts := make(map[string]int)

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

					// Analyze which protobuf fields differ
					diffs := diffFields(result.Entry.Meta, result.TiKVMeta, entryFieldNames, "")
					pattern := strings.Join(diffs, ", ")
					if pattern == "" {
						pattern = "(encoding only)"
					}
					mismatchPatternCounts[pattern]++
					for _, d := range diffs {
						mismatchFieldCounts[d]++
					}

					if *showMissing {
						if *verbose {
							log.Printf("MISMATCH #%d: %s%s [%s]", m, result.Entry.Directory, result.Entry.Name, pattern)
							for _, line := range detailedFieldDiff(result.Entry.Meta, result.TiKVMeta) {
								log.Print(line)
							}
						} else {
							log.Printf("MISMATCH #%d: %s%s [%s]", m, result.Entry.Directory, result.Entry.Name, pattern)
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
		query = fmt.Sprintf(`
			SELECT dirhash, directory, name, meta
			FROM %q
			ORDER BY RANDOM()
			LIMIT %d`,
			*table, *sampleSize)
		log.Printf("Running random sample of %d rows...", *sampleSize)
	} else {
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
	if finalChecked > 0 {
		log.Printf("Found in TiKV: %d (%.2f%%)", finalFound, float64(finalFound)/float64(finalChecked)*100)
		log.Printf("Matched exactly: %d (%.2f%%)", finalMatched, float64(finalMatched)/float64(finalChecked)*100)
		log.Printf("Missing from TiKV: %d (%.2f%%)", finalMissing, float64(finalMissing)/float64(finalChecked)*100)
		log.Printf("Mismatched data: %d (%.2f%%)", finalMismatched, float64(finalMismatched)/float64(finalChecked)*100)
	} else {
		log.Printf("No rows checked - nothing to report")
	}

	// Mismatch field analysis summary
	if finalMismatched > 0 {
		log.Println()
		log.Println("=== Mismatch Analysis ===")

		type kv struct {
			name  string
			count int
		}

		var fields []kv
		for name, count := range mismatchFieldCounts {
			fields = append(fields, kv{name, count})
		}
		sort.Slice(fields, func(i, j int) bool { return fields[i].count > fields[j].count })

		log.Println("Differing fields:")
		for _, f := range fields {
			log.Printf("  %-50s %d entries", f.name, f.count)
		}

		var patterns []kv
		for pattern, count := range mismatchPatternCounts {
			patterns = append(patterns, kv{pattern, count})
		}
		sort.Slice(patterns, func(i, j int) bool { return patterns[i].count > patterns[j].count })

		log.Println("Mismatch patterns:")
		for _, p := range patterns {
			log.Printf("  [%s]  %d entries", p.name, p.count)
		}
	}

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
