package sstable

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"kora/internal/engine"
)

func TestSSTableWriteRead(t *testing.T) {
	dir, err := ioutil.TempDir("", "ssttest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "table.sst")

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	entries := []*engine.Entry{
		{Key: []byte("alpha"), Value: []byte("1"), Kind: engine.EntryKindSet, SeqNum: 1},
		{Key: []byte("bravo"), Value: []byte("2"), Kind: engine.EntryKindSet, SeqNum: 2},
		{Key: []byte("charlie"), Value: []byte("3"), Kind: engine.EntryKindSet, SeqNum: 3},
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	// iterate
	it := r.Iterator()
	var gotKeys []string
	for it.Next() {
		cur := it.Current()
		if cur != nil {
			gotKeys = append(gotKeys, string(cur.Key))
		}
	}
	if it.Err() != nil {
		t.Fatalf("iterator error: %v", it.Err())
	}
	want := []string{"alpha", "bravo", "charlie"}
	if len(gotKeys) != len(want) {
		t.Fatalf("count mismatch: got %d want %d", len(gotKeys), len(want))
	}
	for i := range want {
		if gotKeys[i] != want[i] {
			t.Fatalf("order mismatch at %d: got %q want %q", i, gotKeys[i], want[i])
		}
	}
}

func TestSSTableSeekAndGet(t *testing.T) {
	dir, err := ioutil.TempDir("", "ssttest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "table2.sst")

	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	entries := []*engine.Entry{
		{Key: []byte("a"), Value: []byte("1"), Kind: engine.EntryKindSet, SeqNum: 1},
		{Key: []byte("c"), Value: []byte("3"), Kind: engine.EntryKindSet, SeqNum: 2},
		{Key: []byte("f"), Value: []byte("6"), Kind: engine.EntryKindSet, SeqNum: 3},
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	r, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	// exact get
	ent, err := r.Get([]byte("c"))
	if err != nil {
		t.Fatalf("get c: %v", err)
	}
	if string(ent.Value) != "3" {
		t.Fatalf("value mismatch for c: %s", ent.Value)
	}

	// seek to key >= 'b' => should return 'c'
	ent2, err := r.Seek([]byte("b"))
	if err != nil {
		t.Fatalf("seek b: %v", err)
	}
	if string(ent2.Key) != "c" {
		t.Fatalf("seek result mismatch: got %s want c", ent2.Key)
	}

	// seek past end
	_, err = r.Seek([]byte("z"))
	if err == nil {
		t.Fatalf("expected not found for seek z")
	}
}
