package util

import "kora/internal/engine"

// Iterator is an interface for sequentially traversing entries from a data structure.
// It supports forward iteration through sorted entries.
// Implementations must be safe to hold Lock() across multiple calls.
type Iterator interface {
	// Next moves to the next entry. Returns true if a valid entry exists, false at end or error.
	Next() bool

	// Current returns the currently pointed-to entry.
	// Only valid to call after a successful Next() or Seek().
	Current() *engine.Entry

	// Seek moves the iterator to the first entry >= target key.
	// Returns true if such an entry exists.
	Seek(target []byte) bool

	// Lock prevents concurrent modifications (if applicable).
	// Used before a series of Next() calls to ensure isolation.
	// Not all implementations require this, but it must be safe to call.
	Lock()

	// Unlock releases the lock acquired by Lock().
	Unlock()

	// Close cleans up resources.
	Close() error
}
