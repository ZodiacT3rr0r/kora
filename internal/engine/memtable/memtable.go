package memtable

import (
	"sync"

	"kora/internal/engine"
	"kora/internal/util"
)

const (
	// defaultMemTableSizeBytes is the default threshold (64 MB).
	// When the MemTable reaches this size, it is marked as full and should be flushed.
	defaultMemTableSizeBytes = 64 * 1024 * 1024
)

// MemTable is an in-memory write buffer that wraps a SkipList.
// It tracks approximate memory usage and manages freeze semantics for durability.
type MemTable struct {
	mu           sync.RWMutex
	skipList     *SkipList
	sizeBytes    int64
	maxSizeBytes int64
	frozen       bool
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

// Put inserts or replaces an entry in the MemTable.
// Returns error if the MemTable is frozen (read-only).
// Updates the approximate size counter.
func (m *MemTable) Put(entry *engine.Entry) error {
	if entry == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.frozen {
		return ErrMemTableFrozen
	}

	// Estimate the byte cost of the entry.
	cost := int64(len(entry.Key) + len(entry.Value) + 40) // 40 bytes for overhead (SeqNum, Kind, ExpiresAt, pointers)

	// Store the entry.
	m.skipList.Put(entry)

	// Update size. In a real system this would be more sophisticated (tracking deltas).
	// For now we approximate by adding the cost.
	m.sizeBytes += cost

	return nil
}

// Get retrieves an entry from the MemTable.
func (m *MemTable) Get(key []byte) (*engine.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.skipList.Get(key)
}

// Size returns the number of unique keys in the MemTable.
func (m *MemTable) Size() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.skipList.Size()
}

// ApproximateSizeBytes returns the estimated byte usage of the MemTable.
func (m *MemTable) ApproximateSizeBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.sizeBytes
}

// IsFull returns true if the MemTable has reached or exceeded the size threshold.
func (m *MemTable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.sizeBytes >= m.maxSizeBytes
}

// IsFrozen returns true if the MemTable is read-only (marked for flush).
func (m *MemTable) IsFrozen() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.frozen
}

// Freeze marks the MemTable as read-only.
// After freeze, Put operations will fail.
// The MemTable can still be read and iterated for flushing to disk.
func (m *MemTable) Freeze() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.frozen = true
}

// Iterator returns an ordered iterator over the MemTable.
func (m *MemTable) Iterator() util.Iterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.skipList.Iterator()
}

// ErrMemTableFrozen is returned when trying to write to a frozen MemTable.
type errMemTableFrozen struct{}

func (errMemTableFrozen) Error() string {
	return "memtable is frozen (read-only)"
}

// ErrMemTableFrozen is a singleton error.
var ErrMemTableFrozen = errMemTableFrozen{}
