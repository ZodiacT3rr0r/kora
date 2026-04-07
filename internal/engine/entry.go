package engine

import (
	"time"
)

// EntryKind represents the type of an entry in the storage engine.
type EntryKind uint8

const (
	// EntryKindSet means this is a normal write (set a value).
	EntryKindSet EntryKind = iota
	// EntryKindDelete means this entry represents a deletion (tombstone).
	EntryKindDelete
)

// Entry represents a single key-value record in the storage engine.
// Every write or delete operation creates an Entry.
// Entries are stored in the MemTable, WAL, and SSTables.
type Entry struct {
	// Key is the lookup identifier (bytes, arbitrary content).
	Key []byte
	// Value is the stored data (only meaningful if Kind == EntryKindSet).
	Value []byte
	// Kind indicates if this is a real write or a deletion marker.
	Kind EntryKind
	// SeqNum is the global sequence number, incrementing with each write/delete.
	// Used to determine which version of a key is the newest (last-write-wins).
	SeqNum uint64
	// ExpiresAt is the absolute Unix nanosecond timestamp when this key expires.
	// Zero means no expiration. After ExpiresAt, reads treat the key as absent.
	ExpiresAt int64
}

// IsExpired checks if this entry's TTL has passed.
// It returns true if ExpiresAt is set (non-zero) and is in the past.
func (e *Entry) IsExpired() bool {
	if e.ExpiresAt == 0 {
		// No expiration set; entry is not expired.
		return false
	}
	// Check if current time is after ExpiresAt.
	return time.Now().UnixNano() > e.ExpiresAt
}

// TTLRemaining returns how much time (in nanoseconds) until this entry expires.
// Returns 0 if already expired, -1 if no expiration.
func (e *Entry) TTLRemaining() int64 {
	if e.ExpiresAt == 0 {
		// No expiration.
		return -1
	}
	now := time.Now().UnixNano()
	remaining := e.ExpiresAt - now
	if remaining < 0 {
		return 0
	}
	return remaining
}

// TTLRemainingSeconds returns how much time (in seconds) until this entry expires.
// Redis convention: returns -1 if no expiration, -2 if key doesn't exist (not used here, just -1).
func (e *Entry) TTLRemainingSeconds() int64 {
	if e.ExpiresAt == 0 {
		return -1
	}
	now := time.Now().UnixNano()
	remaining := e.ExpiresAt - now
	if remaining < 0 {
		return 0
	}
	// Convert nanoseconds to seconds.
	return remaining / 1_000_000_000
}

// NewEntryWithTTL creates an Entry with an optional TTL duration.
// If ttlDuration is 0, no expiration is set (ExpiresAt = 0).
func NewEntryWithTTL(key, value []byte, seqNum uint64, ttlDuration time.Duration) *Entry {
	var expiresAt int64
	if ttlDuration > 0 {
		expiresAt = time.Now().Add(ttlDuration).UnixNano()
	}
	return &Entry{
		Key:       key,
		Value:     value,
		Kind:      EntryKindSet,
		SeqNum:    seqNum,
		ExpiresAt: expiresAt,
	}
}

// NewDeleteEntry creates a tombstone entry (deletion marker).
// The Value field is not used, but SeqNum is set to the write order.
func NewDeleteEntry(key []byte, seqNum uint64) *Entry {
	return &Entry{
		Key:    key,
		Value:  nil,
		Kind:   EntryKindDelete,
		SeqNum: seqNum,
	}
}
