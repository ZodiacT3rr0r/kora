package manifest

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestManifestSaveLoad(t *testing.T) {
	dir, err := ioutil.TempDir("", "manifesttest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "manifest.json")

	m := New(path)
	m.Load() // ensure empty start

	meta := SSTableMeta{
		File:    "table-1.sst",
		Level:   0,
		MinKey:  []byte("a"),
		MaxKey:  []byte("z"),
		Size:    1024,
		MinSeq:  1,
		MaxSeq:  100,
		Created: time.Now().UnixNano(),
	}
	if err := m.Add(meta); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Reload manifest into new object
	m2 := New(path)
	if err := m2.Load(); err != nil {
		t.Fatalf("load: %v", err)
	}
	items := m2.List()
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].File != meta.File {
		t.Fatalf("file mismatch: got %s want %s", items[0].File, meta.File)
	}
}

func TestManifestRemove(t *testing.T) {
	dir, err := ioutil.TempDir("", "manifesttest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "manifest.json")

	m := New(path)
	m.Load()
	m.Add(SSTableMeta{File: "a.sst", Level: 0, Created: time.Now().UnixNano()})
	m.Add(SSTableMeta{File: "b.sst", Level: 0, Created: time.Now().UnixNano()})
	if len(m.List()) != 2 {
		t.Fatalf("expected 2 items")
	}
	if err := m.Remove("a.sst"); err != nil {
		t.Fatalf("remove: %v", err)
	}
	if len(m.List()) != 1 {
		t.Fatalf("expected 1 item after remove")
	}
}

func TestManifestFindByKey(t *testing.T) {
	dir, err := ioutil.TempDir("", "manifesttest")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "manifest.json")

	m := New(path)
	m.Load()
	m.Add(SSTableMeta{File: "a.sst", MinKey: []byte("a"), MaxKey: []byte("c"), Created: time.Now().UnixNano()})
	m.Add(SSTableMeta{File: "b.sst", MinKey: []byte("d"), MaxKey: []byte("f"), Created: time.Now().UnixNano()})

	res := m.FindByKey([]byte("b"))
	if len(res) != 1 || res[0].File != "a.sst" {
		t.Fatalf("expected b in a.sst, got %v", res)
	}
	res2 := m.FindByKey([]byte("e"))
	if len(res2) != 1 || res2[0].File != "b.sst" {
		t.Fatalf("expected e in b.sst, got %v", res2)
	}
}
