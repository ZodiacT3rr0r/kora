package memtable

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"kora/internal/engine"
)

func TestSkipListPutGetAndSize(t *testing.T) {
	sl := NewSkipListWithSeed(1)

	entries := []*engine.Entry{
		{Key: []byte("alpha"), Value: []byte("1"), Kind: engine.EntryKindSet, SeqNum: 1},
		{Key: []byte("bravo"), Value: []byte("2"), Kind: engine.EntryKindSet, SeqNum: 2},
		{Key: []byte("charlie"), Value: []byte("3"), Kind: engine.EntryKindSet, SeqNum: 3},
	}

	for _, entry := range entries {
		sl.Put(entry)
	}

	if got, want := sl.Size(), len(entries); got != want {
		t.Fatalf("size mismatch: got %d want %d", got, want)
	}

	for _, entry := range entries {
		got, ok := sl.Get(entry.Key)
		if !ok {
			t.Fatalf("expected key %q to exist", entry.Key)
		}
		if !bytes.Equal(got.Value, entry.Value) {
			t.Fatalf("value mismatch for key %q: got %q want %q", entry.Key, got.Value, entry.Value)
		}
		if got.SeqNum != entry.SeqNum {
			t.Fatalf("seqnum mismatch for key %q: got %d want %d", entry.Key, got.SeqNum, entry.SeqNum)
		}
	}

	if _, ok := sl.Get([]byte("missing")); ok {
		t.Fatal("expected missing key lookup to fail")
	}
}

func TestSkipListOverwriteDoesNotGrowSize(t *testing.T) {
	sl := NewSkipListWithSeed(2)

	sl.Put(&engine.Entry{Key: []byte("same"), Value: []byte("first"), Kind: engine.EntryKindSet, SeqNum: 1})
	sl.Put(&engine.Entry{Key: []byte("same"), Value: []byte("second"), Kind: engine.EntryKindSet, SeqNum: 2})

	if got, want := sl.Size(), 1; got != want {
		t.Fatalf("size mismatch after overwrite: got %d want %d", got, want)
	}

	got, ok := sl.Get([]byte("same"))
	if !ok {
		t.Fatal("expected overwritten key to exist")
	}
	if !bytes.Equal(got.Value, []byte("second")) {
		t.Fatalf("overwrite did not replace value: got %q want %q", got.Value, []byte("second"))
	}
	if got.SeqNum != 2 {
		t.Fatalf("overwrite did not replace seqnum: got %d want %d", got.SeqNum, 2)
	}
}

func TestSkipListIteratorReturnsSortedOrder(t *testing.T) {
	sl := NewSkipListWithSeed(3)

	keys := make([]string, 0, 200)
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Sprintf("key-%04d", i))
	}

	perm := rand.New(rand.NewSource(99)).Perm(len(keys))
	for i, idx := range perm {
		key := keys[idx]
		sl.Put(&engine.Entry{Key: []byte(key), Value: []byte(fmt.Sprintf("value-%d", i)), Kind: engine.EntryKindSet, SeqNum: uint64(i + 1)})
	}

	iter := sl.Iterator()
	defer func() {
		if err := iter.Close(); err != nil {
			t.Fatalf("close failed: %v", err)
		}
	}()

	var got []string
	for iter.Next() {
		current := iter.Current()
		if current == nil {
			t.Fatal("iterator returned nil current entry")
		}
		got = append(got, string(current.Key))
	}

	want := append([]string(nil), keys...)
	sort.Strings(want)

	if len(got) != len(want) {
		t.Fatalf("iterator length mismatch: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("iterator order mismatch at %d: got %q want %q", i, got[i], want[i])
		}
	}
}

func TestSkipListSeekAndLockedIteration(t *testing.T) {
	sl := NewSkipListWithSeed(4)

	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key-%02d", i)
		sl.Put(&engine.Entry{Key: []byte(key), Value: []byte(key), Kind: engine.EntryKindSet, SeqNum: uint64(i + 1)})
	}

	iter := sl.Iterator()
	defer iter.Close()

	if !iter.Seek([]byte("key-02")) {
		t.Fatal("expected seek to find key-02")
	}
	current := iter.Current()
	if current == nil || string(current.Key) != "key-02" {
		t.Fatalf("seek returned wrong entry: got %#v", current)
	}

	if !iter.Seek([]byte("key-021")) {
		t.Fatal("expected seek to land on next larger key")
	}
	current = iter.Current()
	if current == nil || string(current.Key) != "key-03" {
		t.Fatalf("seek to in-between key landed on wrong entry: got %#v", current)
	}

	iter.Lock()
	defer iter.Unlock()

	var lockedKeys []string
	for iter.Next() {
		current = iter.Current()
		if current == nil {
			t.Fatal("locked iterator returned nil current entry")
		}
		lockedKeys = append(lockedKeys, string(current.Key))
	}

	if len(lockedKeys) == 0 {
		t.Fatal("expected locked iterator to yield entries")
	}
}

func TestEmptySkipListIterator(t *testing.T) {
	sl := NewSkipListWithSeed(5)
	iter := sl.Iterator()
	defer iter.Close()

	if iter.Next() {
		t.Fatal("expected empty iterator to return false")
	}
	if iter.Current() != nil {
		t.Fatal("expected current to be nil for empty iterator")
	}
}
