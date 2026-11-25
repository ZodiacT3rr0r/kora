package memtable

import (
	"sync"
	"sync/atomic"

	"kora/internal/engine"
	"kora/internal/util"
)

const (
	// defaultMemTableSizeBytes is the default threshold (64 MB).
	// When the MemTable reaches this size, it is marked as full and should be flushed.
	defaultMemTableSizeBytes = 64 * 1024 * 1024
)

// MemTable is an in-memory write buffer backed by a fine-grained concurrent SkipList.
// Reads (Get, Size, IsFull, IsFrozen, Iterator) are completely lock-free;
// concurrent writers run in parallel under freezeMu.RLock while Freeze() acquires
// freezeMu.Lock to wait for in-flight writes before sealing the MemTable.
type MemTable struct {
	freezeMu     sync.RWMutex
	skipList     *SkipList
	sizeBytes    atomic.Int64
	maxSizeBytes int64
	frozen       atomic.Bool
}

// NewMemTable creates a MemTable with default size threshold.
func NewMemTable() *MemTable {
	return NewMemTableWithSize(defaultMemTableSizeBytes)
}

// NewMemTableWithSize creates a MemTable with a custom size threshold.
func NewMemTableWithSize(maxSizeBytes int64) *MemTable {
	if maxSizeBytes < 1024 {
		maxSizeBytes = 1024 // Minimum 1KB to prevent instant full state
	}
	return &MemTable{
		skipList:     NewSkipList(),
		maxSizeBytes: maxSizeBytes,
	}
}

// Put inserts or replaces an entry in the MemTable concurrently.
// Multiple writers can execute Put in parallel; freezeMu.RLock only coordinates with Freeze().
func (m *MemTable) Put(entry *engine.Entry) error {
	if entry == nil {
		return nil
	}

	m.freezeMu.RLock()
	defer m.freezeMu.RUnlock()

	if m.frozen.Load() {
		return ErrMemTableFrozen
	}

	cost := int64(len(entry.Key) + len(entry.Value) + 40)
	m.skipList.Put(entry)
	m.sizeBytes.Add(cost)

	return nil
}

// Get retrieves an entry from the MemTable without acquiring any locks.
func (m *MemTable) Get(key []byte) (*engine.Entry, bool) {
	return m.skipList.Get(key)
}

// Size returns the number of unique keys in the MemTable.
func (m *MemTable) Size() int {
	return m.skipList.Size()
}

// ApproximateSizeBytes returns the estimated byte usage of the MemTable.
func (m *MemTable) ApproximateSizeBytes() int64 {
	return m.sizeBytes.Load()
}

// IsFull returns true if the MemTable has reached or exceeded the size threshold.
func (m *MemTable) IsFull() bool {
	return m.sizeBytes.Load() >= m.maxSizeBytes
}

// IsFrozen returns true if the MemTable is read-only (marked for flush).
func (m *MemTable) IsFrozen() bool {
	return m.frozen.Load()
}

// Freeze marks the MemTable as read-only after waiting for any in-flight Put calls.
func (m *MemTable) Freeze() {
	m.freezeMu.Lock()
	defer m.freezeMu.Unlock()

	m.frozen.Store(true)
}

// Iterator returns an ordered lock-free iterator over the MemTable.
func (m *MemTable) Iterator() util.Iterator {
	return m.skipList.Iterator()
}

// ErrMemTableFrozen is returned when trying to write to a frozen MemTable.
type errMemTableFrozen struct{}

func (errMemTableFrozen) Error() string {
	return "memtable is frozen (read-only)"
}

// ErrMemTableFrozen is a singleton error.
var ErrMemTableFrozen = errMemTableFrozen{}
