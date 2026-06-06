package core

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"kora/internal/engine/sstable"
)

func TestFlushOnceCreatesSSTablesAndUpdatesManifest(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		DataDir:         dir,
		WALPath:         filepath.Join(dir, "kora.wal"),
		ManifestPath:    filepath.Join(dir, "manifest.json"),
		MemTableMaxSize: 64, // small so rotation occurs
		SyncOnWrite:     false,
	}
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer func() { _ = e.Close() }()

	// write enough entries to trigger at least one rotation
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("key-%02d", i))
		v := []byte(fmt.Sprintf("val-%02d", i))
		if err := e.Put(k, v, 0); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	if e.ImmutableCount() == 0 {
		t.Fatalf("expected at least one immutable memtable before flush")
	}

	if err := e.FlushOnce(); err != nil {
		t.Fatalf("flush once failed: %v", err)
	}

	// manifest should have at least one sstable
	items := e.manifest.List()
	if len(items) == 0 {
		t.Fatalf("expected manifest to contain sstable entries after flush")
	}

	// check files exist
	for _, it := range items {
		if _, err := os.Stat(it.File); err != nil {
			t.Fatalf("sstable file missing: %v", err)
		}
		// basic meta sanity
		if it.MinSeq == 0 || it.MaxSeq == 0 || it.MaxSeq < it.MinSeq {
			t.Fatalf("invalid seq range in meta: %+v", it)
		}
	}

	// Optionally verify we can open an SST and read a key
	first := items[0]
	r, err := sstable.Open(first.File)
	if err != nil {
		t.Fatalf("open sstable: %v", err)
	}
	defer r.Close()
	// iterate quickly to ensure no panic
	it := r.Iterator()
	ok := it.Next()
	if !ok && it.Err() != nil {
		t.Fatalf("sstable iterator error: %v", it.Err())
	}
}
