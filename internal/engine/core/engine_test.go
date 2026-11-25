package core

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnginePutGetDelete(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Options{DataDir: dir, SyncOnWrite: true})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer e.Close()

	if err := e.Put([]byte("alpha"), []byte("1"), 0); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, ok, err := e.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok || string(val) != "1" {
		t.Fatalf("unexpected get result: ok=%v val=%q", ok, val)
	}

	if err := e.Delete([]byte("alpha")); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, ok, err = e.Get([]byte("alpha"))
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if ok {
		t.Fatal("expected deleted key to be absent")
	}
}

func TestEngineTTL(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Options{DataDir: dir})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer e.Close()

	if err := e.Put([]byte("temp"), []byte("v"), 30*time.Millisecond); err != nil {
		t.Fatalf("put ttl: %v", err)
	}
	time.Sleep(45 * time.Millisecond)
	_, ok, err := e.Get([]byte("temp"))
	if err != nil {
		t.Fatalf("get ttl: %v", err)
	}
	if ok {
		t.Fatal("expected expired key to be absent")
	}
}

func TestEngineWALRecovery(t *testing.T) {
	dir := t.TempDir()
	opts := Options{DataDir: dir, SyncOnWrite: true}
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	if err := e.Put([]byte("k1"), []byte("v1"), 0); err != nil {
		t.Fatalf("put k1: %v", err)
	}
	if err := e.Put([]byte("k2"), []byte("v2"), 0); err != nil {
		t.Fatalf("put k2: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e2.Close()

	val, ok, err := e2.Get([]byte("k1"))
	if err != nil || !ok || string(val) != "v1" {
		t.Fatalf("recovered k1 mismatch: ok=%v val=%q err=%v", ok, val, err)
	}
	val, ok, err = e2.Get([]byte("k2"))
	if err != nil || !ok || string(val) != "v2" {
		t.Fatalf("recovered k2 mismatch: ok=%v val=%q err=%v", ok, val, err)
	}
}

func TestEngineMemTableRotation(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Options{DataDir: dir, MemTableMaxSize: 120})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer e.Close()

	for i := 0; i < 30; i++ {
		key := []byte{byte('a' + byte(i%26)), byte('0' + byte(i%10))}
		if err := e.Put(key, []byte("value-value-value"), 0); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if e.ImmutableCount() == 0 {
		t.Fatal("expected immutable memtable after threshold crossing")
	}
}

func TestEngineCustomPaths(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "custom.wal")
	manifestPath := filepath.Join(dir, "custom.manifest")
	e, err := Open(Options{DataDir: dir, WALPath: walPath, ManifestPath: manifestPath})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer e.Close()

	if err := e.Put([]byte("a"), []byte("b"), 0); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := os.Stat(walPath); err != nil {
		t.Fatalf("expected WAL file at custom path: %v", err)
	}
}

func TestEngineBackgroundFlushAndCompaction(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Options{
		DataDir:              dir,
		MemTableMaxSize:      1024,
		BackgroundCompaction: true,
		CompactionInterval:   25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	for i := 0; i < 120; i++ {
		k := []byte("bg-key-" + string(rune('A'+(i%26))) + string(rune('0'+(i%10))))
		if err := e.Put(k, []byte("payload-for-background-flush-and-compaction"), 0); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	// Delete one key and ensure tombstone semantics hold across background flush/compaction.
	targetDel := []byte("bg-key-A0")
	if err := e.Delete(targetDel); err != nil {
		t.Fatalf("delete: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if e.ImmutableCount() == 0 && len(e.manifest.List()) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, ok, err := e.Get(targetDel); err != nil || ok {
		t.Fatalf("expected deleted key to stay deleted after background compaction: ok=%v err=%v", ok, err)
	}
	val, ok, err := e.Get([]byte("bg-key-B1"))
	if err != nil || !ok || string(val) != "payload-for-background-flush-and-compaction" {
		t.Fatalf("unexpected value after background flush/compaction: ok=%v val=%q err=%v", ok, val, err)
	}

	if err := e.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestEngineSeqNumMonotonicAcrossFlushedRestart(t *testing.T) {
	dir := t.TempDir()
	opts := Options{
		DataDir:         dir,
		MemTableMaxSize: 1024,
	}
	e, err := Open(opts)
	if err != nil {
		t.Fatalf("open 1: %v", err)
	}
	for i := 0; i < 40; i++ {
		k := []byte("seq-key-" + string(rune('a'+(i%26))))
		if err := e.Put(k, []byte("old-value-payload-padding-to-trigger-flush"), 0); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}
	if err := e.FlushOnce(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatalf("close 1: %v", err)
	}

	// Reopen with flushed SSTables and overwrite key "seq-key-a" with a fresh value.
	e2, err := Open(opts)
	if err != nil {
		t.Fatalf("open 2: %v", err)
	}
	defer e2.Close()
	if e2.nextSeq == 0 {
		t.Fatalf("expected nextSeq initialized from manifest MaxSeq, got 0")
	}
	if err := e2.Put([]byte("seq-key-a"), []byte("new-value"), 0); err != nil {
		t.Fatalf("put overwrite: %v", err)
	}
	// Force flush and compact so both versions meet in SSTables.
	for i := 0; i < 35; i++ {
		_ = e2.Put([]byte("pad-"+string(rune('0'+(i%10)))), []byte("padding-value-for-second-flush-trigger"), 0)
	}
	if err := e2.FlushOnce(); err != nil {
		t.Fatalf("flush 2: %v", err)
	}
	if err := e2.Compact(); err != nil {
		t.Fatalf("compact: %v", err)
	}
	got, ok, err := e2.Get([]byte("seq-key-a"))
	if err != nil || !ok || string(got) != "new-value" {
		t.Fatalf("expected 'new-value' after flushed restart + compaction, got ok=%v val=%q err=%v", ok, got, err)
	}
}
