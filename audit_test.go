// Tests for audit.go (migration audit tool)
// Run with: go test -v audit.go audit_test.go
package main

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
)

// ========== parseProtoFields Tests ==========

func TestParseProtoFields_Empty(t *testing.T) {
	fields := parseProtoFields(nil)
	if len(fields) != 0 {
		t.Errorf("parseProtoFields(nil) returned %d fields, want 0", len(fields))
	}
	fields = parseProtoFields([]byte{})
	if len(fields) != 0 {
		t.Errorf("parseProtoFields([]) returned %d fields, want 0", len(fields))
	}
}

func TestParseProtoFields_Varint(t *testing.T) {
	data := protowire.AppendTag(nil, 1, protowire.VarintType)
	data = protowire.AppendVarint(data, 150)

	fields := parseProtoFields(data)
	vals, ok := fields[1]
	if !ok || len(vals) != 1 {
		t.Fatalf("expected 1 value for field 1, got %v", fields)
	}
	if vals[0].wireType != protowire.VarintType {
		t.Errorf("wire type = %v, want VarintType", vals[0].wireType)
	}
	if vals[0].varint != 150 {
		t.Errorf("varint = %d, want 150", vals[0].varint)
	}
}

func TestParseProtoFields_Bytes(t *testing.T) {
	data := protowire.AppendTag(nil, 2, protowire.BytesType)
	data = protowire.AppendBytes(data, []byte("hello"))

	fields := parseProtoFields(data)
	vals, ok := fields[2]
	if !ok || len(vals) != 1 {
		t.Fatalf("expected 1 value for field 2, got %v", fields)
	}
	if vals[0].wireType != protowire.BytesType {
		t.Errorf("wire type = %v, want BytesType", vals[0].wireType)
	}
	if !bytes.Equal(vals[0].bval, []byte("hello")) {
		t.Errorf("bytes = %q, want %q", vals[0].bval, "hello")
	}
}

func TestParseProtoFields_MultipleFields(t *testing.T) {
	var data []byte
	data = protowire.AppendTag(data, 1, protowire.VarintType)
	data = protowire.AppendVarint(data, 42)
	data = protowire.AppendTag(data, 2, protowire.BytesType)
	data = protowire.AppendBytes(data, []byte("test"))
	data = protowire.AppendTag(data, 3, protowire.VarintType)
	data = protowire.AppendVarint(data, 100)

	fields := parseProtoFields(data)
	if len(fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(fields))
	}
	if fields[1][0].varint != 42 {
		t.Errorf("field 1 = %d, want 42", fields[1][0].varint)
	}
	if string(fields[2][0].bval) != "test" {
		t.Errorf("field 2 = %q, want %q", fields[2][0].bval, "test")
	}
	if fields[3][0].varint != 100 {
		t.Errorf("field 3 = %d, want 100", fields[3][0].varint)
	}
}

func TestParseProtoFields_RepeatedField(t *testing.T) {
	var data []byte
	data = protowire.AppendTag(data, 4, protowire.BytesType)
	data = protowire.AppendBytes(data, []byte("chunk1"))
	data = protowire.AppendTag(data, 4, protowire.BytesType)
	data = protowire.AppendBytes(data, []byte("chunk2"))

	fields := parseProtoFields(data)
	vals := fields[4]
	if len(vals) != 2 {
		t.Fatalf("expected 2 values for repeated field 4, got %d", len(vals))
	}
	if string(vals[0].bval) != "chunk1" || string(vals[1].bval) != "chunk2" {
		t.Errorf("repeated values = [%q, %q], want [chunk1, chunk2]",
			vals[0].bval, vals[1].bval)
	}
}

func TestParseProtoFields_Fixed32(t *testing.T) {
	data := protowire.AppendTag(nil, 5, protowire.Fixed32Type)
	data = protowire.AppendFixed32(data, 0xDEADBEEF)

	fields := parseProtoFields(data)
	if fields[5][0].wireType != protowire.Fixed32Type {
		t.Errorf("wire type = %v, want Fixed32Type", fields[5][0].wireType)
	}
	if fields[5][0].fixed32 != 0xDEADBEEF {
		t.Errorf("fixed32 = 0x%x, want 0xDEADBEEF", fields[5][0].fixed32)
	}
}

func TestParseProtoFields_Fixed64(t *testing.T) {
	data := protowire.AppendTag(nil, 6, protowire.Fixed64Type)
	data = protowire.AppendFixed64(data, 0x123456789ABCDEF0)

	fields := parseProtoFields(data)
	if fields[6][0].wireType != protowire.Fixed64Type {
		t.Errorf("wire type = %v, want Fixed64Type", fields[6][0].wireType)
	}
	if fields[6][0].fixed64 != 0x123456789ABCDEF0 {
		t.Errorf("fixed64 = 0x%x, want 0x123456789ABCDEF0", fields[6][0].fixed64)
	}
}

func TestParseProtoFields_CorruptInput(t *testing.T) {
	// Should not panic on corrupt input
	corrupt := parseProtoFields([]byte{0xff, 0xff, 0xff})
	_ = corrupt // just verify no panic
}

// ========== fieldValuesEqual Tests ==========

func TestFieldValuesEqual(t *testing.T) {
	tests := []struct {
		name string
		a, b []fieldValue
		want bool
	}{
		{"both nil", nil, nil, true},
		{"both empty slices", []fieldValue{}, []fieldValue{}, true},
		{"different lengths", []fieldValue{{wireType: protowire.VarintType}}, nil, false},
		{
			"same varint",
			[]fieldValue{{wireType: protowire.VarintType, varint: 42}},
			[]fieldValue{{wireType: protowire.VarintType, varint: 42}},
			true,
		},
		{
			"different varint",
			[]fieldValue{{wireType: protowire.VarintType, varint: 42}},
			[]fieldValue{{wireType: protowire.VarintType, varint: 43}},
			false,
		},
		{
			"same bytes",
			[]fieldValue{{wireType: protowire.BytesType, bval: []byte("hello")}},
			[]fieldValue{{wireType: protowire.BytesType, bval: []byte("hello")}},
			true,
		},
		{
			"different bytes",
			[]fieldValue{{wireType: protowire.BytesType, bval: []byte("hello")}},
			[]fieldValue{{wireType: protowire.BytesType, bval: []byte("world")}},
			false,
		},
		{
			"different wire types",
			[]fieldValue{{wireType: protowire.VarintType, varint: 42}},
			[]fieldValue{{wireType: protowire.Fixed32Type, fixed32: 42}},
			false,
		},
		{
			"same fixed32",
			[]fieldValue{{wireType: protowire.Fixed32Type, fixed32: 0xAB}},
			[]fieldValue{{wireType: protowire.Fixed32Type, fixed32: 0xAB}},
			true,
		},
		{
			"same fixed64",
			[]fieldValue{{wireType: protowire.Fixed64Type, fixed64: 0xABCD}},
			[]fieldValue{{wireType: protowire.Fixed64Type, fixed64: 0xABCD}},
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fieldValuesEqual(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("fieldValuesEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

// ========== sortedFieldNums Tests ==========

func TestSortedFieldNums(t *testing.T) {
	m1 := map[int][]fieldValue{3: {}, 1: {}, 5: {}}
	m2 := map[int][]fieldValue{2: {}, 5: {}, 7: {}}

	got := sortedFieldNums(m1, m2)
	want := []int{1, 2, 3, 5, 7}

	if len(got) != len(want) {
		t.Fatalf("sortedFieldNums() returned %d items, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("sortedFieldNums()[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestSortedFieldNums_Empty(t *testing.T) {
	got := sortedFieldNums()
	if len(got) != 0 {
		t.Errorf("sortedFieldNums() with no args = %v, want empty", got)
	}
}

// ========== decodeMapEntries Tests ==========

func TestDecodeMapEntries(t *testing.T) {
	// Build a protobuf map entry: {1: "mykey", 2: "myval"}
	var entryData []byte
	entryData = protowire.AppendTag(entryData, 1, protowire.BytesType)
	entryData = protowire.AppendBytes(entryData, []byte("mykey"))
	entryData = protowire.AppendTag(entryData, 2, protowire.BytesType)
	entryData = protowire.AppendBytes(entryData, []byte("myval"))

	entries := []fieldValue{
		{wireType: protowire.BytesType, bval: entryData},
	}

	result := decodeMapEntries(entries)
	if len(result) != 1 {
		t.Fatalf("decodeMapEntries() returned %d entries, want 1", len(result))
	}
	val, ok := result["mykey"]
	if !ok {
		t.Fatal("missing key 'mykey'")
	}
	if !bytes.Equal(val, []byte("myval")) {
		t.Errorf("value = %q, want %q", val, "myval")
	}
}

func TestDecodeMapEntries_Empty(t *testing.T) {
	result := decodeMapEntries(nil)
	if len(result) != 0 {
		t.Errorf("decodeMapEntries(nil) returned %d entries, want 0", len(result))
	}
}

// ========== diffExtended Tests ==========

func TestDiffExtended_Identical(t *testing.T) {
	var entryData []byte
	entryData = protowire.AppendTag(entryData, 1, protowire.BytesType)
	entryData = protowire.AppendBytes(entryData, []byte("key"))
	entryData = protowire.AppendTag(entryData, 2, protowire.BytesType)
	entryData = protowire.AppendBytes(entryData, []byte("val"))

	fv := []fieldValue{{wireType: protowire.BytesType, bval: entryData}}

	diffs := diffExtended(fv, fv)
	if len(diffs) != 0 {
		t.Errorf("identical extended should have no diffs, got %v", diffs)
	}
}

func TestDiffExtended_MissingKey(t *testing.T) {
	var entry1 []byte
	entry1 = protowire.AppendTag(entry1, 1, protowire.BytesType)
	entry1 = protowire.AppendBytes(entry1, []byte("key1"))
	entry1 = protowire.AppendTag(entry1, 2, protowire.BytesType)
	entry1 = protowire.AppendBytes(entry1, []byte("val1"))

	pg := []fieldValue{{wireType: protowire.BytesType, bval: entry1}}

	diffs := diffExtended(pg, nil)
	if len(diffs) != 1 {
		t.Errorf("expected 1 diff for missing key, got %d: %v", len(diffs), diffs)
	}
}

func TestDiffExtended_ChangedValue(t *testing.T) {
	makeMapEntry := func(key, val string) fieldValue {
		var buf []byte
		buf = protowire.AppendTag(buf, 1, protowire.BytesType)
		buf = protowire.AppendBytes(buf, []byte(key))
		buf = protowire.AppendTag(buf, 2, protowire.BytesType)
		buf = protowire.AppendBytes(buf, []byte(val))
		return fieldValue{wireType: protowire.BytesType, bval: buf}
	}

	pg := []fieldValue{makeMapEntry("key1", "val1"), makeMapEntry("key2", "val2")}
	tikv := []fieldValue{makeMapEntry("key1", "val1"), makeMapEntry("key2", "changed")}

	diffs := diffExtended(pg, tikv)
	if len(diffs) != 1 {
		t.Errorf("expected 1 diff, got %v", diffs)
	}
}

// ========== diffFields Tests ==========

func TestDiffFields_Identical(t *testing.T) {
	var buf []byte
	buf = protowire.AppendTag(buf, 1, protowire.BytesType)
	buf = protowire.AppendBytes(buf, []byte("test.txt"))
	buf = protowire.AppendTag(buf, 2, protowire.VarintType)
	buf = protowire.AppendVarint(buf, 0)

	diffs := diffFields(buf, buf, entryFieldNames, "")
	if len(diffs) != 0 {
		t.Errorf("identical data should have no diffs, got %v", diffs)
	}
}

func TestDiffFields_Different(t *testing.T) {
	var pg []byte
	pg = protowire.AppendTag(pg, 1, protowire.BytesType)
	pg = protowire.AppendBytes(pg, []byte("old.txt"))

	var tikv []byte
	tikv = protowire.AppendTag(tikv, 1, protowire.BytesType)
	tikv = protowire.AppendBytes(tikv, []byte("new.txt"))

	diffs := diffFields(pg, tikv, entryFieldNames, "")
	if len(diffs) != 1 || diffs[0] != "name" {
		t.Errorf("expected [name] diff, got %v", diffs)
	}
}
