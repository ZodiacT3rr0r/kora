package util

import "testing"

func TestCRC32AndVerifyCRC32(t *testing.T) {
	data := []byte("hello world")
	checksum := CRC32(data)
	if checksum == 0 {
		t.Fatal("expected non-zero checksum for non-empty data")
	}
	if !VerifyCRC32(data, checksum) {
		t.Fatal("expected checksum verification to pass")
	}
	if VerifyCRC32([]byte("hello world!"), checksum) {
		t.Fatal("expected checksum verification to fail for different data")
	}
}

func TestCRC32EmptyInput(t *testing.T) {
	if got := CRC32(nil); got != CRC32([]byte{}) {
		t.Fatalf("expected nil and empty slice to hash the same, got %d vs %d", CRC32(nil), CRC32([]byte{}))
	}
	if !VerifyCRC32(nil, CRC32(nil)) {
		t.Fatal("expected empty input checksum verification to pass")
	}
}
