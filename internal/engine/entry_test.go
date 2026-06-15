package engine

import (
	"testing"
	"time"
)

func TestNewEntryWithTTL(t *testing.T) {
	entry := NewEntryWithTTL([]byte("key"), []byte("value"), 42, 2*time.Second)
	if entry == nil {
		t.Fatal("expected entry")
	}
	if string(entry.Key) != "key" {
		t.Fatalf("unexpected key: %q", entry.Key)
	}
	if string(entry.Value) != "value" {
		t.Fatalf("unexpected value: %q", entry.Value)
	}
	if entry.Kind != EntryKindSet {
		t.Fatalf("unexpected kind: %v", entry.Kind)
	}
	if entry.SeqNum != 42 {
		t.Fatalf("unexpected seqnum: %d", entry.SeqNum)
	}
	if entry.ExpiresAt == 0 {
		t.Fatal("expected expiry to be set")
	}
}

func TestNewDeleteEntry(t *testing.T) {
	entry := NewDeleteEntry([]byte("key"), 99)
	if entry == nil {
		t.Fatal("expected entry")
	}
	if entry.Kind != EntryKindDelete {
		t.Fatalf("unexpected kind: %v", entry.Kind)
	}
	if entry.SeqNum != 99 {
		t.Fatalf("unexpected seqnum: %d", entry.SeqNum)
	}
	if entry.Value != nil {
		t.Fatalf("expected nil value, got %q", entry.Value)
	}
}

func TestEntryExpiryHelpers(t *testing.T) {
	fresh := &Entry{ExpiresAt: time.Now().Add(time.Second).UnixNano()}
	if fresh.IsExpired() {
		t.Fatal("fresh entry should not be expired")
	}
	if remaining := fresh.TTLRemaining(); remaining <= 0 {
		t.Fatalf("expected positive remaining ttl, got %d", remaining)
	}
	if seconds := fresh.TTLRemainingSeconds(); seconds < 0 {
		t.Fatalf("expected non-negative ttl seconds, got %d", seconds)
	}

	expired := &Entry{ExpiresAt: time.Now().Add(-time.Second).UnixNano()}
	if !expired.IsExpired() {
		t.Fatal("expired entry should be expired")
	}
	if remaining := expired.TTLRemaining(); remaining != 0 {
		t.Fatalf("expected zero remaining ttl, got %d", remaining)
	}
	if seconds := expired.TTLRemainingSeconds(); seconds != 0 {
		t.Fatalf("expected zero ttl seconds, got %d", seconds)
	}

	permanent := &Entry{}
	if permanent.IsExpired() {
		t.Fatal("permanent entry should not expire")
	}
	if remaining := permanent.TTLRemaining(); remaining != -1 {
		t.Fatalf("expected -1 remaining ttl, got %d", remaining)
	}
	if seconds := permanent.TTLRemainingSeconds(); seconds != -1 {
		t.Fatalf("expected -1 ttl seconds, got %d", seconds)
	}
}
