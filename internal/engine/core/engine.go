package core

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"kora/internal/engine"
	"kora/internal/engine/bloom"
	"kora/internal/engine/compaction"
	"kora/internal/engine/manifest"
	"kora/internal/engine/memtable"
	"kora/internal/engine/sstable"
	"kora/internal/engine/wal"
)

var ErrNotFound = errors.New("engine: key not found")

type Options struct {
	DataDir              string
	WALPath              string
	ManifestPath         string
	MemTableMaxSize      int64
	SyncOnWrite          bool
	BackgroundCompaction bool
	CompactionInterval   time.Duration
	SchedulerConfig      *compaction.Config
}

type Engine struct {
	mu              sync.RWMutex
	flushMu         sync.Mutex
	compactMu       sync.Mutex
	wal             *wal.WAL
	manifest        *manifest.Manifest
	active          *memtable.MemTable
	immutables      []*memtable.MemTable
	nextSeq         uint64
	syncOnWrite     bool
	memTableMaxSize int64
	dataDir         string
	scheduler       *compaction.Scheduler

	bloomMu    sync.RWMutex
	bloomCache map[string]*bloom.BloomFilter

	bgEnabled bool
	flushCh   chan struct{}
	compactCh chan struct{}
	stopCh    chan struct{}
	closed    atomic.Bool
	bgWg      sync.WaitGroup
}

func Open(opts Options) (*Engine, error) {
	if opts.DataDir == "" {
		opts.DataDir = "./data"
	}
	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, err
	}
	if opts.WALPath == "" {
		opts.WALPath = filepath.Join(opts.DataDir, "kora.wal")
	}
	if opts.ManifestPath == "" {
		opts.ManifestPath = filepath.Join(opts.DataDir, "manifest.json")
	}

	// Clean up any orphan temporary WAL from an interrupted rotation before rename.
	_ = os.Remove(opts.WALPath + ".tmp")

	w, err := wal.Open(opts.WALPath)
	if err != nil {
		return nil, err
	}

	m := manifest.New(opts.ManifestPath)
	if err := m.Load(); err != nil {
		_ = w.Close()
		return nil, err
	}

	active := memtable.NewMemTable()
	if opts.MemTableMaxSize > 0 {
		active = memtable.NewMemTableWithSize(opts.MemTableMaxSize)
	}

	sched := compaction.NewScheduler()
	if opts.SchedulerConfig != nil {
		sched = compaction.NewSchedulerWithConfig(*opts.SchedulerConfig)
	}

	e := &Engine{
		wal:             w,
		manifest:        m,
		active:          active,
		syncOnWrite:     opts.SyncOnWrite,
		memTableMaxSize: opts.MemTableMaxSize,
		dataDir:         opts.DataDir,
		scheduler:       sched,
		bloomCache:      make(map[string]*bloom.BloomFilter),
		bgEnabled:       opts.BackgroundCompaction,
		flushCh:         make(chan struct{}, 1),
		compactCh:       make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
	}

	if err := e.recoverFromWAL(); err != nil {
		_ = w.Close()
		return nil, err
	}

	if opts.BackgroundCompaction {
		interval := opts.CompactionInterval
		if interval <= 0 {
			interval = 500 * time.Millisecond
		}
		e.bgWg.Add(2)
		go e.backgroundFlushLoop()
		go e.backgroundCompactLoop(interval)
		if e.ImmutableCount() > 0 {
			e.signalFlush()
		}
	}

	return e, nil
}

func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(e.stopCh)
	e.bgWg.Wait()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal == nil {
		return nil
	}
	err := e.wal.Close()
	e.wal = nil
	return err
}

func (e *Engine) signalFlush() {
	if !e.bgEnabled || e.closed.Load() {
		return
	}
	select {
	case e.flushCh <- struct{}{}:
	default:
	}
}

func (e *Engine) signalCompact() {
	if !e.bgEnabled || e.closed.Load() {
		return
	}
	select {
	case e.compactCh <- struct{}{}:
	default:
	}
}

func (e *Engine) backgroundFlushLoop() {
	defer e.bgWg.Done()
	for {
		select {
		case <-e.stopCh:
			return
		case <-e.flushCh:
			if e.closed.Load() {
				return
			}
			if err := e.FlushOnce(); err == nil {
				e.signalCompact()
			}
		}
	}
}

func (e *Engine) backgroundCompactLoop(interval time.Duration) {
	defer e.bgWg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopCh:
			return
		case <-e.compactCh:
			if !e.closed.Load() {
				_ = e.Compact()
			}
		case <-ticker.C:
			if !e.closed.Load() {
				_ = e.Compact()
			}
		}
	}
}

func (e *Engine) Put(key, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return errors.New("engine: empty key")
	}
	e.mu.Lock()
	rotated := false

	e.nextSeq++
	entry := &engine.Entry{
		Key:    cloneBytes(key),
		Value:  cloneBytes(value),
		Kind:   engine.EntryKindSet,
		SeqNum: e.nextSeq,
	}
	if ttl > 0 {
		entry.ExpiresAt = time.Now().Add(ttl).UnixNano()
	}

	if _, err := e.wal.Append(entry); err != nil {
		e.mu.Unlock()
		return err
	}
	if e.syncOnWrite {
		if err := e.wal.Sync(); err != nil {
			e.mu.Unlock()
			return err
		}
	}
	if err := e.active.Put(entry); err != nil {
		e.mu.Unlock()
		return err
	}
	if e.active.IsFull() {
		e.rotateIfFullLocked()
		rotated = true
	}
	e.mu.Unlock()

	if rotated {
		e.signalFlush()
	}
	return nil
}

func (e *Engine) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("engine: empty key")
	}
	e.mu.Lock()
	rotated := false

	e.nextSeq++
	entry := engine.NewDeleteEntry(cloneBytes(key), e.nextSeq)
	if _, err := e.wal.Append(entry); err != nil {
		e.mu.Unlock()
		return err
	}
	if e.syncOnWrite {
		if err := e.wal.Sync(); err != nil {
			e.mu.Unlock()
			return err
		}
	}
	if err := e.active.Put(entry); err != nil {
		e.mu.Unlock()
		return err
	}
	if e.active.IsFull() {
		e.rotateIfFullLocked()
		rotated = true
	}
	e.mu.Unlock()

	if rotated {
		e.signalFlush()
	}
	return nil
}

func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, nil
	}
	e.mu.RLock()
	active := e.active
	immutables := append([]*memtable.MemTable(nil), e.immutables...)
	e.mu.RUnlock()

	if ent, found := active.Get(key); found {
		if ent.Kind == engine.EntryKindDelete || ent.IsExpired() {
			return nil, false, nil
		}
		return cloneBytes(ent.Value), true, nil
	}
	for i := len(immutables) - 1; i >= 0; i-- {
		if ent, found := immutables[i].Get(key); found {
			if ent.Kind == engine.EntryKindDelete || ent.IsExpired() {
				return nil, false, nil
			}
			return cloneBytes(ent.Value), true, nil
		}
	}

	candidates := e.manifest.FindByKey(key)
	if len(candidates) == 0 {
		return nil, false, nil
	}
	// Search newest sequence range first (lower level / higher MaxSeq shadows older levels).
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].MaxSeq == candidates[j].MaxSeq {
			if candidates[i].Level != candidates[j].Level {
				return candidates[i].Level < candidates[j].Level
			}
			return candidates[i].Created > candidates[j].Created
		}
		return candidates[i].MaxSeq > candidates[j].MaxSeq
	})
	for _, meta := range candidates {
		e.bloomMu.RLock()
		bf := e.bloomCache[meta.File]
		e.bloomMu.RUnlock()
		if bf != nil && !bf.MightContain(key) {
			continue
		}

		r, err := sstable.Open(meta.File)
		if err != nil {
			continue
		}
		if bf == nil && r.Bloom() != nil {
			e.bloomMu.Lock()
			e.bloomCache[meta.File] = r.Bloom()
			e.bloomMu.Unlock()
		}
		ent, err := r.Get(key)
		_ = r.Close()
		if err != nil {
			continue
		}
		if ent.Kind == engine.EntryKindDelete || ent.IsExpired() {
			return nil, false, nil
		}
		return cloneBytes(ent.Value), true, nil
	}
	return nil, false, nil
}

func (e *Engine) Recover() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.recoverFromWALLocked()
}

func (e *Engine) ActiveSize() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.active.Size()
}

func (e *Engine) ImmutableCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.immutables)
}

func (e *Engine) recoverFromWAL() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.recoverFromWALLocked()
}

func (e *Engine) recoverFromWALLocked() error {
	entries, err := e.wal.Recover()
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if ent.SeqNum > e.nextSeq {
			e.nextSeq = ent.SeqNum
		}
		if err := e.active.Put(ent); err != nil {
			return err
		}
		e.rotateIfFullLocked()
	}
	return nil
}

func (e *Engine) rotateIfFullLocked() {
	if !e.active.IsFull() {
		return
	}
	e.active.Freeze()
	e.immutables = append(e.immutables, e.active)
	if e.memTableMaxSize > 0 {
		e.active = memtable.NewMemTableWithSize(e.memTableMaxSize)
		return
	}
	e.active = memtable.NewMemTable()
}

func uniqueFlushSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// FlushOnce flushes all currently frozen memtables (immutables) to SSTables,
// updates the manifest atomically before dropping immutables from memory (avoiding read gaps),
// and atomically rotates the WAL under lock so concurrent writes cannot be lost.
func (e *Engine) FlushOnce() error {
	e.flushMu.Lock()
	defer e.flushMu.Unlock()

	e.mu.RLock()
	if len(e.immutables) == 0 {
		e.mu.RUnlock()
		return nil
	}
	immSnapshot := append([]*memtable.MemTable(nil), e.immutables...)
	e.mu.RUnlock()

	flushedSet := make(map[*memtable.MemTable]struct{}, len(immSnapshot))

	for idx, mt := range immSnapshot {
		fname := filepath.Join(
			e.dataDir,
			fmt.Sprintf("sstable-%s-%d-%s.sst", time.Now().Format("20060102T150405"), idx, uniqueFlushSuffix()),
		)
		w, err := sstable.NewWriter(fname)
		if err != nil {
			return err
		}
		iter := mt.Iterator()
		var minKey, maxKey []byte
		var minSeq, maxSeq uint64
		first := true
		bf := bloom.New(uint64(mt.Size()+1), 0.01)

		for iter.Next() {
			ent := iter.Current()
			if ent == nil {
				continue
			}
			bf.Add(ent.Key)
			if first {
				minKey = cloneBytes(ent.Key)
				maxKey = cloneBytes(ent.Key)
				minSeq = ent.SeqNum
				maxSeq = ent.SeqNum
				first = false
			} else {
				if bytes.Compare(ent.Key, minKey) < 0 {
					minKey = cloneBytes(ent.Key)
				}
				if bytes.Compare(ent.Key, maxKey) > 0 {
					maxKey = cloneBytes(ent.Key)
				}
				if ent.SeqNum < minSeq {
					minSeq = ent.SeqNum
				}
				if ent.SeqNum > maxSeq {
					maxSeq = ent.SeqNum
				}
			}
			if err := w.Add(ent); err != nil {
				_ = iter.Close()
				_ = w.Close()
				return err
			}
		}
		_ = iter.Close()
		if err := w.Close(); err != nil {
			return err
		}

		fi, err := os.Stat(fname)
		if err != nil {
			return err
		}
		meta := manifest.SSTableMeta{
			File:    fname,
			Level:   0,
			MinKey:  minKey,
			MaxKey:  maxKey,
			Size:    fi.Size(),
			MinSeq:  minSeq,
			MaxSeq:  maxSeq,
			Created: time.Now().UnixNano(),
		}
		e.bloomMu.Lock()
		e.bloomCache[fname] = bf
		e.bloomMu.Unlock()

		if err := e.manifest.Add(meta); err != nil {
			return err
		}
		flushedSet[mt] = struct{}{}
	}

	// Atomically prune flushed immutables and rotate WAL while holding e.mu.Lock
	// so no concurrent Put/Delete can race with WAL close/rename or be lost.
	e.mu.Lock()
	defer e.mu.Unlock()

	remaining := make([]*memtable.MemTable, 0, len(e.immutables))
	for _, mt := range e.immutables {
		if _, done := flushedSet[mt]; !done {
			remaining = append(remaining, mt)
		}
	}
	e.immutables = remaining

	if e.wal == nil {
		return nil
	}

	oldPath := e.wal.Path()
	tmpPath := oldPath + ".tmp"
	newWal, err := wal.Open(tmpPath)
	if err != nil {
		return err
	}

	writeUnflushed := func(mt *memtable.MemTable) error {
		it := mt.Iterator()
		defer it.Close()
		for it.Next() {
			ent := it.Current()
			if ent == nil {
				continue
			}
			if _, err := newWal.Append(ent); err != nil {
				return err
			}
		}
		return nil
	}

	for _, mt := range e.immutables {
		if err := writeUnflushed(mt); err != nil {
			_ = newWal.Close()
			_ = os.Remove(tmpPath)
			return err
		}
	}
	if err := writeUnflushed(e.active); err != nil {
		_ = newWal.Close()
		_ = os.Remove(tmpPath)
		return err
	}

	if err := newWal.Sync(); err != nil {
		_ = newWal.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := newWal.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := e.wal.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, oldPath); err != nil {
		return err
	}
	w2, err := wal.Open(oldPath)
	if err != nil {
		return err
	}
	e.wal = w2
	return nil
}

// Compact runs multi-level compaction (cascading L0 -> L1 -> L2... as needed),
// atomically updating the manifest via Replace so concurrent FlushOnce and Get
// operations never observe missing keys or conflicting manifest updates.
func (e *Engine) Compact() error {
	e.compactMu.Lock()
	defer e.compactMu.Unlock()

	// Bound cascading steps per Compact() invocation to prevent unbounded loops.
	for step := 0; step < 8; step++ {
		files := e.manifest.List()
		job := e.scheduler.SelectCompactionJob(files, e.dataDir)
		if job == nil {
			return nil
		}

		outMeta, wroteOutput, err := compaction.ExecuteCompactionJobFull(job)
		if err != nil {
			return err
		}

		removed := make([]string, 0, len(job.InputFiles))
		for _, inMeta := range job.InputFiles {
			removed = append(removed, inMeta.File)
		}

		var added []manifest.SSTableMeta
		if wroteOutput {
			added = []manifest.SSTableMeta{outMeta}
			if r, err := sstable.Open(outMeta.File); err == nil {
				if r.Bloom() != nil {
					e.bloomMu.Lock()
					e.bloomCache[outMeta.File] = r.Bloom()
					e.bloomMu.Unlock()
				}
				_ = r.Close()
			}
		}

		if err := e.manifest.Replace(added, removed); err != nil {
			return err
		}

		e.bloomMu.Lock()
		for _, fn := range removed {
			delete(e.bloomCache, fn)
		}
		e.bloomMu.Unlock()

		for _, fn := range removed {
			_ = os.Remove(fn)
		}
	}
	return nil
}

func resolveEntry(mt *memtable.MemTable, key []byte) ([]byte, bool) {
	ent, ok := mt.Get(key)
	if !ok {
		return nil, false
	}
	if ent.Kind == engine.EntryKindDelete {
		return nil, false
	}
	if ent.IsExpired() {
		return nil, false
	}
	return cloneBytes(ent.Value), true
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
