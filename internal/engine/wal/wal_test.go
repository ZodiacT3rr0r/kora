package wal

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"kora/internal/engine"
)

func TestWALAppendRecover(t *testing.T) {
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	entries := []*engine.Entry{
		{Key: []byte("a"), Value: []byte("1"), Kind: engine.EntryKindSet, SeqNum: 1},
		{Key: []byte("b"), Value: []byte("2"), Kind: engine.EntryKindSet, SeqNum: 2},
		{Key: []byte("c"), Value: []byte("3"), Kind: engine.EntryKindSet, SeqNum: 3},
	}

	for _, e := range entries {
		if _, err := w.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	// Reopen and recover
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	recovered, err := w2.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if len(recovered) != len(entries) {
		t.Fatalf("recovered count mismatch: got %d want %d", len(recovered), len(entries))
	}
	for i := range entries {
		if string(recovered[i].Key) != string(entries[i].Key) || string(recovered[i].Value) != string(entries[i].Value) || recovered[i].SeqNum != entries[i].SeqNum {
			t.Fatalf("entry %d mismatch", i)
		}
	}
}

func TestWALTruncatePartialRecord(t *testing.T) {
	dir, err := ioutil.TempDir("", "waltest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	entries := []*engine.Entry{
		{Key: []byte("x"), Value: []byte("1"), Kind: engine.EntryKindSet, SeqNum: 1},
		{Key: []byte("y"), Value: []byte("2"), Kind: engine.EntryKindSet, SeqNum: 2},
		{Key: []byte("z"), Value: []byte("3"), Kind: engine.EntryKindSet, SeqNum: 3},
	}

	positions := make([]int64, 0, len(entries))
	for _, e := range entries {
		pos, err := w.Append(e)
		if err != nil {
			t.Fatalf("append: %v", err)
		}
		positions = append(positions, pos)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}

	// Now truncate the file to remove the last record's payload partially.
	// Seek to position of last record and then add 2 bytes to simulate partial write.
	lastPos := positions[len(positions)-1]
	// Compute new size: lastPos + 4 (len) + 4 (crc) + 2 (partial payload)
	newSize := lastPos + 4 + 4 + 2
	if err := w.Truncate(newSize); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// Reopen and recover
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	w2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	recovered, err := w2.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	// Should have only the first two records fully recovered
	if len(recovered) != 2 {
		t.Fatalf("expected 2 recovered entries, got %d", len(recovered))
	}

	// Verify torn tail was truncated cleanly so a subsequent append recovers properly.
	if _, err := w2.Append(&engine.Entry{Key: []byte("w"), Value: []byte("4"), Kind: engine.EntryKindSet, SeqNum: 4}); err != nil {
		t.Fatalf("append after recovery: %v", err)
	}
	if err := w2.Sync(); err != nil {
		t.Fatalf("sync after recovery: %v", err)
	}
	recovered2, err := w2.Recover()
	if err != nil {
		t.Fatalf("second recover: %v", err)
	}
	if len(recovered2) != 3 || string(recovered2[2].Key) != "w" {
		t.Fatalf("expected 3 entries ending in 'w', got %d", len(recovered2))
	}
}
