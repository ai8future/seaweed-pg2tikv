// Tests for count_keys.go (key counting/deletion tool)
// Run with: go test -v count_keys.go count_keys_test.go
package main

import (
	"bytes"
	"testing"
)

func TestPrefixEndKey(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		want   []byte
	}{
		{"simple ASCII", []byte("abc"), []byte("abd")},
		{"single byte", []byte{0x42}, []byte{0x43}},
		{"trailing 0xff truncated", []byte{0x01, 0xff}, []byte{0x02}},
		{"all 0xff returns nil", []byte{0xff, 0xff, 0xff}, nil},
		{"realistic key prefix", []byte("seaweedfs"), []byte("seaweedft")},
		{"byte 0xfe increments to 0xff", []byte{0xfe}, []byte{0xff}},
		{"mixed with trailing 0xff", []byte{0x01, 0x02, 0xff, 0xff}, []byte{0x01, 0x03}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := prefixEndKey(tt.prefix)
			if !bytes.Equal(got, tt.want) {
				t.Errorf("prefixEndKey(%x) = %x, want %x", tt.prefix, got, tt.want)
			}
		})
	}
}

func TestPrefixEndKey_DoesNotMutateInput(t *testing.T) {
	input := []byte{0x01, 0x02, 0x03}
	original := make([]byte, len(input))
	copy(original, input)

	prefixEndKey(input)

	if !bytes.Equal(input, original) {
		t.Errorf("prefixEndKey mutated input: got %x, original was %x", input, original)
	}
}

func TestPrefixEndKey_RangeProperty(t *testing.T) {
	// Verify that prefix < prefixEndKey for non-nil results
	prefix := []byte("test_prefix")
	end := prefixEndKey(prefix)
	if end == nil {
		t.Skip("nil end key, skip range check")
	}
	if bytes.Compare(prefix, end) >= 0 {
		t.Errorf("prefix %x should be < end %x", prefix, end)
	}
	// Any key starting with prefix should be < end
	testKey := append([]byte{}, prefix...)
	testKey = append(testKey, []byte("anything_after")...)
	if bytes.Compare(testKey, end) >= 0 {
		t.Errorf("key with prefix %x should be < end %x", testKey, end)
	}
}
