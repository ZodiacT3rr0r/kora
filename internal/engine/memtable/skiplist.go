package memtable

import (
	"bytes"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"kora/internal/engine"
	"kora/internal/util"
)

const (
	defaultSkipListMaxLevel = 16
	defaultSkipListProb     = 0.25
)

// skipListNode is a node in the concurrent skip list.
// Key is immutable once allocated; entry and forward pointers use atomic operations
// so reads (Get, Seek, Iterator) and in-place key overwrites are completely lock-free,
// while structural insertions use fine-grained per-node locks on predecessors only
// (Herlihy et al. optimistic/lazy concurrent skip list).
type skipListNode struct {
	mu          sync.Mutex
	key         []byte
	entry       atomic.Pointer[engine.Entry]
	fullyLinked atomic.Bool
	forward     []atomic.Pointer[skipListNode]
}

// SkipList stores entries in sorted key order with O(log n) lock-free reads,
// lock-free atomic value overwrites, and fine-grained node-locked concurrent inserts.
type SkipList struct {
	head        *skipListNode
	level       atomic.Int32
	size        atomic.Int64
	maxLevel    int
	probThresh  uint64
	rngState    atomic.Uint64
	iterLockMu  sync.RWMutex
}

// NewSkipList creates a concurrent skip list with default tuning values.
func NewSkipList() *SkipList {
	return NewSkipListWithSeed(time.Now().UnixNano())
}

// NewSkipListWithSeed creates a concurrent skip list with a deterministic seed.
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

	head := &skipListNode{
		forward: make([]atomic.Pointer[skipListNode], maxLevel),
	}
	head.fullyLinked.Store(true)

	uSeed := uint64(seed)
	if uSeed == 0 {
		uSeed = 0x9e3779b97f4a7c15
	}

	sl := &SkipList{
		head:       head,
		maxLevel:   maxLevel,
		probThresh: uint64(probability * float64(^uint64(0))),
	}
	sl.level.Store(1)
	sl.rngState.Store(uSeed)
	return sl
}

// Put inserts or replaces an entry in O(log n) expected time.
// Overwriting an existing key updates its entry pointer atomically without locking.
// Inserting a new key acquires fine-grained mutexes only on the immediate predecessor
// nodes across levels [0..topLevel-1] in strictly ascending key order (deadlock-free).
func (s *SkipList) Put(entry *engine.Entry) {
	if entry == nil {
		return
	}

	cloned := cloneEntry(entry)
	topLevel := s.randomLevel()

	preds := make([]*skipListNode, s.maxLevel)
	succs := make([]*skipListNode, s.maxLevel)

	for {
		found := s.find(cloned.Key, preds, succs)
		if found != nil {
			for !found.fullyLinked.Load() {
				runtime.Gosched()
			}
			found.entry.Store(cloned)
			return
		}

		// Acquire per-node locks on distinct predecessors in ascending key order
		// (from topLevel-1 down to 0, since key(preds[i]) <= key(preds[j]) for i > j).
		lockedNodes := make([]*skipListNode, 0, topLevel)
		var prevLocked *skipListNode
		valid := true

		for i := topLevel - 1; i >= 0; i-- {
			pred := preds[i]
			if pred != prevLocked {
				pred.mu.Lock()
				lockedNodes = append(lockedNodes, pred)
				prevLocked = pred
			}
		}

		// Validate optimistic traversal assumptions while holding predecessor locks.
		for i := 0; i < topLevel; i++ {
			pred := preds[i]
			succ := succs[i]
			if !pred.fullyLinked.Load() || pred.forward[i].Load() != succ {
				valid = false
				break
			}
			if succ != nil && !succ.fullyLinked.Load() {
				valid = false
				break
			}
		}
		if valid {
			if next0 := preds[0].forward[0].Load(); next0 != nil && bytes.Equal(next0.key, cloned.Key) {
				valid = false
			}
		}

		if !valid {
			for i := len(lockedNodes) - 1; i >= 0; i-- {
				lockedNodes[i].mu.Unlock()
			}
			continue
		}

		node := &skipListNode{
			key:     append([]byte(nil), cloned.Key...),
			forward: make([]atomic.Pointer[skipListNode], topLevel),
		}
		node.entry.Store(cloned)

		for i := 0; i < topLevel; i++ {
			node.forward[i].Store(succs[i])
		}
		// Link bottom-up so level-0 visibility always precedes higher express lanes.
		for i := 0; i < topLevel; i++ {
			preds[i].forward[i].Store(node)
		}
		node.fullyLinked.Store(true)

		for {
			curLevel := s.level.Load()
			if int32(topLevel) <= curLevel || s.level.CompareAndSwap(curLevel, int32(topLevel)) {
				break
			}
		}
		s.size.Add(1)

		for i := len(lockedNodes) - 1; i >= 0; i-- {
			lockedNodes[i].mu.Unlock()
		}
		return
	}
}

// Get returns the entry for key if it exists using a completely lock-free O(log n) traversal.
func (s *SkipList) Get(key []byte) (*engine.Entry, bool) {
	x := s.findNode(key)
	if x == nil {
		return nil, false
	}
	ent := x.entry.Load()
	if ent == nil {
		return nil, false
	}
	return ent, true
}

// Size returns the number of unique keys stored in the skip list.
func (s *SkipList) Size() int {
	return int(s.size.Load())
}

// Iterator returns an ordered lock-free iterator over the skip list.
func (s *SkipList) Iterator() util.Iterator {
	return &skipListIterator{list: s}
}

// nextRand64 generates a pseudo-random uint64 atomically via SplitMix64 (lock-free).
func (s *SkipList) nextRand64() uint64 {
	z := s.rngState.Add(0x9e3779b97f4a7c15)
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func (s *SkipList) randomLevel() int {
	level := 1
	for level < s.maxLevel && s.nextRand64() < s.probThresh {
		level++
	}
	return level
}

// find populates preds and succs across all levels and returns the exact matching node if present.
func (s *SkipList) find(key []byte, preds []*skipListNode, succs []*skipListNode) *skipListNode {
	x := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		next := x.forward[i].Load()
		for next != nil && bytes.Compare(next.key, key) < 0 {
			x = next
			next = x.forward[i].Load()
		}
		preds[i] = x
		succs[i] = next
	}
	cand := succs[0]
	if cand != nil && bytes.Equal(cand.key, key) {
		return cand
	}
	return nil
}

func (s *SkipList) findNode(key []byte) *skipListNode {
	x := s.head
	top := int(s.level.Load()) - 1
	if top < 0 {
		top = 0
	}
	for i := top; i >= 0; i-- {
		for next := x.forward[i].Load(); next != nil && bytes.Compare(next.key, key) < 0; next = x.forward[i].Load() {
			x = next
		}
	}

	cand := x.forward[0].Load()
	if cand != nil && bytes.Equal(cand.key, key) {
		for !cand.fullyLinked.Load() {
			runtime.Gosched()
		}
		return cand
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

	var next *skipListNode
	if it.current == nil {
		next = it.list.head.forward[0].Load()
	} else {
		next = it.current.forward[0].Load()
	}
	for next != nil && !next.fullyLinked.Load() {
		runtime.Gosched()
	}
	it.current = next
	return it.current != nil
}

func (it *skipListIterator) Current() *engine.Entry {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.current == nil {
		return nil
	}
	return it.current.entry.Load()
}

func (it *skipListIterator) Seek(target []byte) bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return false
	}

	x := it.list.head
	top := int(it.list.level.Load()) - 1
	if top < 0 {
		top = 0
	}
	for i := top; i >= 0; i-- {
		for next := x.forward[i].Load(); next != nil && bytes.Compare(next.key, target) < 0; next = x.forward[i].Load() {
			x = next
		}
	}

	cand := x.forward[0].Load()
	for cand != nil && !cand.fullyLinked.Load() {
		runtime.Gosched()
	}
	it.current = cand
	return it.current != nil
}

func (it *skipListIterator) Lock() {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.locked {
		return
	}
	it.list.iterLockMu.RLock()
	it.locked = true
}

func (it *skipListIterator) Unlock() {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || !it.locked {
		return
	}
	it.list.iterLockMu.RUnlock()
	it.locked = false
}

func (it *skipListIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil
	}
	if it.locked {
		it.list.iterLockMu.RUnlock()
		it.locked = false
	}
	it.current = nil
	it.closed = true
	return nil
}
