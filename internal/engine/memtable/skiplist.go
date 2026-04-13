package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"kora/internal/engine"
	"kora/internal/util"
)

const (
	defaultSkipListMaxLevel = 16
	defaultSkipListProb     = 0.25
)

type skipListNode struct {
	entry   *engine.Entry
	forward []*skipListNode
}

// SkipList stores entries in sorted key order and supports fast search, insert, and iteration.
type SkipList struct {
	mu          sync.RWMutex
	head        *skipListNode
	level       int
	size        int
	maxLevel    int
	probability float64
	rng         *rand.Rand
}

// NewSkipList creates a skip list with default tuning values.
func NewSkipList() *SkipList {
	return NewSkipListWithSeed(time.Now().UnixNano())
}

// NewSkipListWithSeed creates a skip list with deterministic randomness.
// It is useful for tests and reproducible behavior.
func NewSkipListWithSeed(seed int64) *SkipList {
	return newSkipList(defaultSkipListMaxLevel, defaultSkipListProb, seed)
}

func newSkipList(maxLevel int, probability float64, seed int64) *SkipList {
	if maxLevel < 1 {
		maxLevel = 1
	}
	if probability <= 0 || probability >= 1 {
		probability = defaultSkipListProb
	}

	return &SkipList{
		head: &skipListNode{
			forward: make([]*skipListNode, maxLevel),
		},
		level:       1,
		maxLevel:    maxLevel,
		probability: probability,
		rng:         rand.New(rand.NewSource(seed)),
	}
}

// Put inserts or replaces an entry for the provided key.
// If the key already exists, the stored entry is overwritten and size does not change.
func (s *SkipList) Put(entry *engine.Entry) {
	if entry == nil {
		return
	}

	cloned := cloneEntry(entry)

	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*skipListNode, s.maxLevel)
	x := s.head

	for i := s.level - 1; i >= 0; i-- {
		for next := x.forward[i]; next != nil && bytes.Compare(next.entry.Key, cloned.Key) < 0; next = x.forward[i] {
			x = next
		}
		update[i] = x
	}

	x = x.forward[0]
	if x != nil && bytes.Equal(x.entry.Key, cloned.Key) {
		x.entry = cloned
		return
	}

	newLevel := s.randomLevel()
	if newLevel > s.level {
		for i := s.level; i < newLevel; i++ {
			update[i] = s.head
		}
		s.level = newLevel
	}

	node := &skipListNode{
		entry:   cloned,
		forward: make([]*skipListNode, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
	}

	s.size++
}

// Get returns the entry for key if it exists.
func (s *SkipList) Get(key []byte) (*engine.Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	x := s.findNode(key)
	if x == nil {
		return nil, false
	}

	return x.entry, true
}

// Size returns the number of unique keys stored in the skip list.
func (s *SkipList) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// Iterator returns an ordered iterator over the skip list.
func (s *SkipList) Iterator() util.Iterator {
	return &skipListIterator{list: s}
}

func (s *SkipList) randomLevel() int {
	level := 1
	for level < s.maxLevel && s.rng.Float64() < s.probability {
		level++
	}
	return level
}

func (s *SkipList) findNode(key []byte) *skipListNode {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for next := x.forward[i]; next != nil && bytes.Compare(next.entry.Key, key) < 0; next = x.forward[i] {
			x = next
		}
	}

	x = x.forward[0]
	if x != nil && bytes.Equal(x.entry.Key, key) {
		return x
	}
	return nil
}

func cloneEntry(entry *engine.Entry) *engine.Entry {
	cloned := &engine.Entry{
		Kind:      entry.Kind,
		SeqNum:    entry.SeqNum,
		ExpiresAt: entry.ExpiresAt,
	}
	if len(entry.Key) > 0 {
		cloned.Key = append([]byte(nil), entry.Key...)
	}
	if len(entry.Value) > 0 {
		cloned.Value = append([]byte(nil), entry.Value...)
	}
	return cloned
}

type skipListIterator struct {
	list    *SkipList
	mu      sync.Mutex
	locked  bool
	closed  bool
	current *skipListNode
}

func (it *skipListIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return false
	}

	moveNext := func() bool {
		if it.current == nil {
			it.current = it.list.head.forward[0]
		} else {
			it.current = it.current.forward[0]
		}
		return it.current != nil
	}

	if it.locked {
		return moveNext()
	}

	it.list.mu.RLock()
	defer it.list.mu.RUnlock()
	return moveNext()
}

func (it *skipListIterator) Current() *engine.Entry {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.current == nil {
		return nil
	}

	if it.locked {
		return it.current.entry
	}

	it.list.mu.RLock()
	defer it.list.mu.RUnlock()
	return it.current.entry
}

func (it *skipListIterator) Seek(target []byte) bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return false
	}

	seek := func() bool {
		x := it.list.head
		for i := it.list.level - 1; i >= 0; i-- {
			for next := x.forward[i]; next != nil && bytes.Compare(next.entry.Key, target) < 0; next = x.forward[i] {
				x = next
			}
		}

		it.current = x.forward[0]
		return it.current != nil
	}

	if it.locked {
		return seek()
	}

	it.list.mu.RLock()
	defer it.list.mu.RUnlock()
	return seek()
}

func (it *skipListIterator) Lock() {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.locked {
		return
	}

	it.list.mu.RLock()
	it.locked = true
}

func (it *skipListIterator) Unlock() {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || !it.locked {
		return
	}

	it.list.mu.RUnlock()
	it.locked = false
}

func (it *skipListIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil
	}

	if it.locked {
		it.list.mu.RUnlock()
		it.locked = false
	}

	it.current = nil
	it.closed = true
	return nil
}
