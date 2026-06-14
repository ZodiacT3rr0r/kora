package compaction

import (
    "os"
    "path/filepath"
    "testing"

    "kora/internal/engine"
    "kora/internal/engine/manifest"
    "kora/internal/engine/sstable"
)

func TestMergeFilesBasic(t *testing.T) {
    dir := t.TempDir()
    // create first sstable
    p1 := filepath.Join(dir, "a.sst")
    w1, err := sstable.NewWriter(p1)
    if err != nil {
        t.Fatal(err)
    }
    e1 := &engine.Entry{Key: []byte("a"), Value: []byte("v1"), Kind: engine.EntryKindSet, SeqNum: 1}
    e2 := &engine.Entry{Key: []byte("b"), Value: []byte("v1b"), Kind: engine.EntryKindSet, SeqNum: 1}
    if err := w1.Add(e1); err != nil {
        t.Fatal(err)
    }
    if err := w1.Add(e2); err != nil {
        t.Fatal(err)
    }
    if err := w1.Close(); err != nil {
        t.Fatal(err)
    }
    fi1, _ := os.Stat(p1)
    m1 := manifest.SSTableMeta{File: p1, Level: 0, MinKey: []byte("a"), MaxKey: []byte("b"), Size: fi1.Size(), MinSeq: 1, MaxSeq: 1}

    // create second sstable with newer "a" and a new "c"
    p2 := filepath.Join(dir, "b.sst")
    w2, err := sstable.NewWriter(p2)
    if err != nil {
        t.Fatal(err)
    }
    e3 := &engine.Entry{Key: []byte("a"), Value: []byte("v2"), Kind: engine.EntryKindSet, SeqNum: 2}
    e4 := &engine.Entry{Key: []byte("c"), Value: []byte("v3"), Kind: engine.EntryKindSet, SeqNum: 2}
    if err := w2.Add(e3); err != nil {
        t.Fatal(err)
    }
    if err := w2.Add(e4); err != nil {
        t.Fatal(err)
    }
    if err := w2.Close(); err != nil {
        t.Fatal(err)
    }
    fi2, _ := os.Stat(p2)
    m2 := manifest.SSTableMeta{File: p2, Level: 0, MinKey: []byte("a"), MaxKey: []byte("c"), Size: fi2.Size(), MinSeq: 2, MaxSeq: 2}

    out := filepath.Join(dir, "merged.sst")
    meta, err := MergeFiles([]manifest.SSTableMeta{m1, m2}, out)
    if err != nil {
        t.Fatal(err)
    }
    if meta.File != out {
        t.Fatalf("expected output file %s got %s", out, meta.File)
    }

    r, err := sstable.Open(out)
    if err != nil {
        t.Fatal(err)
    }
    defer r.Close()

    ea, err := r.Get([]byte("a"))
    if err != nil {
        t.Fatal(err)
    }
    if string(ea.Value) != "v2" {
        t.Fatalf("expected a=v2 got %s", string(ea.Value))
    }

    eb, err := r.Get([]byte("b"))
    if err != nil {
        t.Fatal(err)
    }
    if string(eb.Value) != "v1b" {
        t.Fatalf("expected b=v1b got %s", string(eb.Value))
    }

    ec, err := r.Get([]byte("c"))
    if err != nil {
        t.Fatal(err)
    }
    if string(ec.Value) != "v3" {
        t.Fatalf("expected c=v3 got %s", string(ec.Value))
    }
}
