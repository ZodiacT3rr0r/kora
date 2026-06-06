package core

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"kora/internal/engine"
	"kora/internal/engine/manifest"
	"kora/internal/engine/memtable"
	"kora/internal/engine/sstable"
	"kora/internal/engine/wal"
)

var ErrNotFound = errors.New("engine: key not found")

type Options struct {
	DataDir         string
	WALPath         string
	ManifestPath    string
	MemTableMaxSize int64
	SyncOnWrite     bool
}

type Engine struct {
	mu              sync.RWMutex
	wal             *wal.WAL
	manifest        *manifest.Manifest
	active          *memtable.MemTable
	immutables      []*memtable.MemTable
	nextSeq         uint64
	syncOnWrite     bool
	memTableMaxSize int64
	dataDir         string
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

	e := &Engine{
		wal:             w,
		manifest:        m,
		active:          active,
		syncOnWrite:     opts.SyncOnWrite,
		memTableMaxSize: opts.MemTableMaxSize,
		dataDir:         opts.DataDir,
	}

	if err := e.recoverFromWAL(); err != nil {
		_ = w.Close()
		return nil, err
	}
	return e, nil
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal == nil {
		return nil
	}
	err := e.wal.Close()
	e.wal = nil
	return err
}

func (e *Engine) Put(key, value []byte, ttl time.Duration) error {
	if len(key) == 0 {
		return errors.New("engine: empty key")
	}
	e.mu.Lock()
	defer e.mu.Unlock()

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
		return err
	}
	if e.syncOnWrite {
		if err := e.wal.Sync(); err != nil {
			return err
		}
	}
	if err := e.active.Put(entry); err != nil {
		return err
	}
	e.rotateIfFullLocked()
	return nil
}

func (e *Engine) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("engine: empty key")
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	e.nextSeq++
	entry := engine.NewDeleteEntry(cloneBytes(key), e.nextSeq)
	if _, err := e.wal.Append(entry); err != nil {
		return err
	}
	if e.syncOnWrite {
		if err := e.wal.Sync(); err != nil {
			return err
		}
	}
	if err := e.active.Put(entry); err != nil {
		return err
	}
	e.rotateIfFullLocked()
	return nil
}

func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	if len(key) == 0 {
		return nil, false, nil
	}
	e.mu.RLock()
	defer e.mu.RUnlock()

	if val, ok := resolveEntry(e.active, key); ok {
		return val, true, nil
	}
	for i := len(e.immutables) - 1; i >= 0; i-- {
		if val, ok := resolveEntry(e.immutables[i], key); ok {
			return val, true, nil
		}
	}

	candidates := e.manifest.FindByKey(key)
	if len(candidates) == 0 {
		return nil, false, nil
	}
	// Search newest sequence range first; overlapping L0 files are expected.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].MaxSeq == candidates[j].MaxSeq {
			return candidates[i].Created > candidates[j].Created
		}
		return candidates[i].MaxSeq > candidates[j].MaxSeq
	})
	for _, meta := range candidates {
		r, err := sstable.Open(meta.File)
		if err != nil {
			continue
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

// FlushOnce flushes all currently frozen memtables (immutables) to SSTables,
// updates the manifest, and replaces the WAL with a compacted WAL containing only active entries.
// This is a synchronous operation intended for tests and simple flush pipeline.
func (e *Engine) FlushOnce() error {
	e.mu.Lock()
	imm := e.immutables
	e.immutables = nil
	e.mu.Unlock()

	// For each immutable memtable, write an SSTable file.
	for idx, mt := range imm {
		// Build filename
		fname := filepath.Join(e.dataDir, "sstable-"+time.Now().Format("20060102T150405")+"-"+string(rune(idx))+".sst")
		w, err := sstable.NewWriter(fname)
		if err != nil {
			return err
		}
		iter := mt.Iterator()
		defer iter.Close()
		var minKey, maxKey []byte
		var minSeq, maxSeq uint64
		first := true
		for iter.Next() {
			ent := iter.Current()
			if ent == nil {
				continue
			}
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
				w.Close()
				return err
			}
		}
		if err := w.Close(); err != nil {
			return err
		}
		// stat file size
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
		if err := e.manifest.Add(meta); err != nil {
			return err
		}
	}

	// Replace WAL with compacted WAL that contains only active entries
	// Create temporary WAL
	oldPath := e.wal.Path()
	tmpPath := oldPath + ".tmp"
	newWal, err := wal.Open(tmpPath)
	if err != nil {
		return err
	}
	// iterate active entries
	e.mu.RLock()
	actIter := e.active.Iterator()
	e.mu.RUnlock()
	defer actIter.Close()
	for actIter.Next() {
		ent := actIter.Current()
		if ent == nil {
			continue
		}
		if _, err := newWal.Append(ent); err != nil {
			newWal.Close()
			return err
		}
	}
	if err := newWal.Sync(); err != nil {
		newWal.Close()
		return err
	}
	if err := newWal.Close(); err != nil {
		return err
	}
	// Close old WAL, atomically replace
	if err := e.wal.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, oldPath); err != nil {
		return err
	}
	// Re-open WAL
	w2, err := wal.Open(oldPath)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.wal = w2
	e.mu.Unlock()
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
