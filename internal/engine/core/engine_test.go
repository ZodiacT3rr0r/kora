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
