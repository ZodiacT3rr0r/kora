package memtable

import (
	"testing"

	"kora/internal/engine"
)

func TestMemTablePutGetBasic(t *testing.T) {
	mt := NewMemTable()

	entry := &engine.Entry{
		Key:    []byte("testkey"),
		Value:  []byte("testvalue"),
		Kind:   engine.EntryKindSet,
		SeqNum: 1,
	}

	err := mt.Put(entry)
	if err != nil {
		t.Fatalf("put failed: %v", err)
	}

	got, ok := mt.Get([]byte("testkey"))
	if !ok {
		t.Fatal("expected key to exist after put")
	}

	if string(got.Key) != "testkey" {
		t.Fatalf("key mismatch: got %q want testkey", got.Key)
	}
	if string(got.Value) != "testvalue" {
		t.Fatalf("value mismatch: got %q want testvalue", got.Value)
	}
	if got.SeqNum != 1 {
		t.Fatalf("seqnum mismatch: got %d want 1", got.SeqNum)
	}
}

func TestMemTableSize(t *testing.T) {
	mt := NewMemTable()

	for i := 0; i < 10; i++ {
		entry := &engine.Entry{
			Key:    []byte("key" + string(rune(i))),
			Value:  []byte("value"),
			Kind:   engine.EntryKindSet,
			SeqNum: uint64(i + 1),
		}
		if err := mt.Put(entry); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	if got, want := mt.Size(), 10; got != want {
		t.Fatalf("size mismatch: got %d want %d", got, want)
	}
}

func TestMemTableApproximateSizeBytes(t *testing.T) {
	mt := NewMemTable()

	entry := &engine.Entry{
		Key:    []byte("key"),
		Value:  []byte("value"),
		Kind:   engine.EntryKindSet,
		SeqNum: 1,
	}

	initialSize := mt.ApproximateSizeBytes()
	if initialSize != 0 {
		t.Fatalf("initial size should be 0, got %d", initialSize)
	}

	if err := mt.Put(entry); err != nil {
		t.Fatalf("put failed: %v", err)
	}

	afterSize := mt.ApproximateSizeBytes()
	if afterSize <= 0 {
		t.Fatalf("size after put should be > 0, got %d", afterSize)
	}

	// Size should increase monotonically with each put.
	if err := mt.Put(entry); err != nil {
		t.Fatalf("second put failed: %v", err)
	}

	afterSecondSize := mt.ApproximateSizeBytes()
	if afterSecondSize <= afterSize {
		t.Fatalf("size should increase, was %d now %d", afterSize, afterSecondSize)
	}
}

func TestMemTableIsFull(t *testing.T) {
	smallThreshold := int64(2000) // 2000 bytes (above the 1KB minimum enforced by NewMemTableWithSize)
	mt := NewMemTableWithSize(smallThreshold)

	if mt.IsFull() {
		t.Fatal("memtable should not be full initially")
	}

	// Add enough entries to exceed threshold.
	// Each entry costs about 40 (overhead) + len(key) + len(value).
	for i := 0; i < 50; i++ {
		entry := &engine.Entry{
			Key:    []byte("key" + string(rune(i%10))),
			Value:  []byte("This is a longer value that takes up more space to help reach the threshold faster."),
			Kind:   engine.EntryKindSet,
			SeqNum: uint64(i + 1),
		}
		if err := mt.Put(entry); err != nil {
			t.Fatalf("put failed: %v", err)
		}

		// Check if we've reached full after this entry.
		if mt.ApproximateSizeBytes() >= smallThreshold {
			if !mt.IsFull() {
				t.Fatalf("memtable should be full when size (%d) >= threshold (%d)", mt.ApproximateSizeBytes(), smallThreshold)
			}
			return
		}
	}

	// If we exhausted all puts and didn't get full, that's a test design issue.
	if !mt.IsFull() {
		t.Fatalf("memtable did not reach full threshold; final size: %d bytes (threshold: %d)", mt.ApproximateSizeBytes(), smallThreshold)
	}
}

func TestMemTableFreeze(t *testing.T) {
	mt := NewMemTable()

	entry := &engine.Entry{
		Key:    []byte("key"),
		Value:  []byte("value"),
		Kind:   engine.EntryKindSet,
		SeqNum: 1,
	}

	if err := mt.Put(entry); err != nil {
		t.Fatalf("put before freeze failed: %v", err)
	}

	if mt.IsFrozen() {
		t.Fatal("memtable should not be frozen initially")
	}

	mt.Freeze()

	if !mt.IsFrozen() {
		t.Fatal("memtable should be frozen after Freeze()")
	}

	// Reads should still work.
	_, ok := mt.Get([]byte("key"))
	if !ok {
		t.Fatal("get should work on frozen memtable")
	}

	// Puts should fail.
	newEntry := &engine.Entry{
		Key:    []byte("newkey"),
		Value:  []byte("newvalue"),
		Kind:   engine.EntryKindSet,
		SeqNum: 2,
	}
	err := mt.Put(newEntry)
	if err == nil {
		t.Fatal("put on frozen memtable should fail")
	}
	if err != ErrMemTableFrozen {
		t.Fatalf("expected ErrMemTableFrozen, got %v", err)
	}
}

func TestMemTableIteratorOrdered(t *testing.T) {
	mt := NewMemTable()

	keys := []string{"charlie", "alpha", "bravo"}
	for i, key := range keys {
		entry := &engine.Entry{
			Key:    []byte(key),
			Value:  []byte("value-" + key),
			Kind:   engine.EntryKindSet,
			SeqNum: uint64(i + 1),
		}
		if err := mt.Put(entry); err != nil {
			t.Fatalf("put failed: %v", err)
		}
	}

	iter := mt.Iterator()
	defer iter.Close()

	var got []string
	for iter.Next() {
		current := iter.Current()
		if current != nil {
			got = append(got, string(current.Key))
		}
	}

	// Should be sorted.
	want := []string{"alpha", "bravo", "charlie"}
	if len(got) != len(want) {
		t.Fatalf("iterator count mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("iterator order mismatch at %d: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestMemTableNilPutIsIgnored(t *testing.T) {
	mt := NewMemTable()

	initialSize := mt.Size()
	initialBytes := mt.ApproximateSizeBytes()

	// Putting nil should be a no-op.
	err := mt.Put(nil)
	if err != nil {
		t.Fatalf("put(nil) should not error, got %v", err)
	}

	if mt.Size() != initialSize || mt.ApproximateSizeBytes() != initialBytes {
		t.Fatal("put(nil) should not change MemTable state")
	}
}

func TestMemTableOverwriteNotDoublingSize(t *testing.T) {
	mt := NewMemTable()

	entry := &engine.Entry{
		Key:    []byte("key"),
		Value:  []byte("first-value"),
		Kind:   engine.EntryKindSet,
		SeqNum: 1,
	}

	if err := mt.Put(entry); err != nil {
		t.Fatalf("first put failed: %v", err)
	}

	sizeAfterFirst := mt.ApproximateSizeBytes()

	// Overwrite with same key.
	entry.Value = []byte("second-value-much-longer")
	entry.SeqNum = 2

	if err := mt.Put(entry); err != nil {
		t.Fatalf("second put failed: %v", err)
	}

	// Size should increase (because value is longer), but Size() should stay 1.
	if mt.Size() != 1 {
		t.Fatalf("size should stay 1 after overwrite, got %d", mt.Size())
	}

	sizeAfterSecond := mt.ApproximateSizeBytes()
	if sizeAfterSecond <= sizeAfterFirst {
		t.Fatalf("size should increase on overwrite with longer value: was %d now %d", sizeAfterFirst, sizeAfterSecond)
	}
}
