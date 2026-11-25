package compaction

import (
	"bytes"
	"container/heap"
	"os"
	"time"

	"kora/internal/engine"
	"kora/internal/engine/manifest"
	"kora/internal/engine/sstable"
)

// item represents a min-heap element during k-way merge.
// Using a min-heap yields O(N log K) single-pass merge time instead of O(K) intermediate
// disk passes that pairwise merging would incur.
type item struct {
	key []byte
	idx int
}

// MergeFiles performs a k-way merge of sorted SSTable files listed in metas
// and writes a single merged SSTable to outPath, preserving tombstones.
func MergeFiles(metas []manifest.SSTableMeta, outPath string) (manifest.SSTableMeta, error) {
	meta, _, err := MergeFilesWithFilter(metas, outPath, nil)
	return meta, err
}

// MergeFilesWithFilter performs a k-way merge of sorted SSTables in O(N log K) time.
// If canDropKey is non-nil and returns true for a key whose winning entry is a tombstone
// (EntryKindDelete) or expired TTL entry, the entry is safely purged instead of written.
// Returns (meta, wroteEntries, error). If all entries were purged, wroteEntries is false
// and outPath is removed so no empty SSTable is left behind.
func MergeFilesWithFilter(
	metas []manifest.SSTableMeta,
	outPath string,
	canDropKey func(key []byte) bool,
) (manifest.SSTableMeta, bool, error) {
	var meta manifest.SSTableMeta
	if len(metas) == 0 {
		return meta, false, nil
	}

	type src struct {
		r   *sstable.Reader
		it  *sstable.Iterator
		cur *engine.Entry
	}
	srcs := make([]*src, 0, len(metas))
	for _, m := range metas {
		r, err := sstable.Open(m.File)
		if err != nil {
			continue
		}
		it := r.Iterator()
		s := &src{r: r, it: it}
		if it.Next() {
			s.cur = it.Current()
		}
		srcs = append(srcs, s)
	}

	closeAll := func() {
		for _, s := range srcs {
			if s.r != nil {
				_ = s.r.Close()
			}
		}
	}

	h := &minHeap{}
	heap.Init(h)
	for i, s := range srcs {
		if s.cur != nil {
			heap.Push(h, &item{key: clone(s.cur.Key), idx: i})
		}
	}

	w, err := sstable.NewWriter(outPath)
	if err != nil {
		closeAll()
		return meta, false, err
	}

	var minKey, maxKey []byte
	var minSeq, maxSeq uint64
	first := true
	writtenCount := 0

	for h.Len() > 0 {
		it := heap.Pop(h).(*item)
		key := it.key
		idx := it.idx

		group := []*engine.Entry{}
		if srcs[idx].cur != nil && bytes.Equal(srcs[idx].cur.Key, key) {
			group = append(group, srcs[idx].cur)
			if srcs[idx].it.Next() {
				srcs[idx].cur = srcs[idx].it.Current()
				heap.Push(h, &item{key: clone(srcs[idx].cur.Key), idx: idx})
			} else {
				srcs[idx].cur = nil
			}
		}
		for h.Len() > 0 {
			top := (*h)[0]
			if !bytes.Equal(top.key, key) {
				break
			}
			it2 := heap.Pop(h).(*item)
			i2 := it2.idx
			if srcs[i2].cur != nil {
				group = append(group, srcs[i2].cur)
				if srcs[i2].it.Next() {
					srcs[i2].cur = srcs[i2].it.Current()
					heap.Push(h, &item{key: clone(srcs[i2].cur.Key), idx: i2})
				} else {
					srcs[i2].cur = nil
				}
			}
		}

		// Choose winner by highest SeqNum (last-write-wins).
		var winner *engine.Entry
		for _, e := range group {
			if winner == nil || e.SeqNum > winner.SeqNum {
				winner = e
			}
		}
		if winner == nil {
			continue
		}

		// Safe tombstone & expired-entry garbage collection:
		// Drop a tombstone or expired entry ONLY when no deeper level can hold an older
		// version of this key (guaranteed by canDropKey checking lower-level key ranges).
		if (winner.Kind == engine.EntryKindDelete || winner.IsExpired()) && canDropKey != nil && canDropKey(winner.Key) {
			continue
		}

		if first {
			minKey = clone(winner.Key)
			maxKey = clone(winner.Key)
			minSeq = winner.SeqNum
			maxSeq = winner.SeqNum
			first = false
		} else {
			if bytes.Compare(winner.Key, minKey) < 0 {
				minKey = clone(winner.Key)
			}
			if bytes.Compare(winner.Key, maxKey) > 0 {
				maxKey = clone(winner.Key)
			}
			if winner.SeqNum < minSeq {
				minSeq = winner.SeqNum
			}
			if winner.SeqNum > maxSeq {
				maxSeq = winner.SeqNum
			}
		}

		if err := w.Add(winner); err != nil {
			_ = w.Close()
			closeAll()
			return meta, false, err
		}
		writtenCount++
	}

	if err := w.Close(); err != nil {
		closeAll()
		return meta, false, err
	}
	closeAll()

	if writtenCount == 0 {
		_ = os.Remove(outPath)
		return meta, false, nil
	}

	fi, err := os.Stat(outPath)
	if err != nil {
		return meta, false, err
	}
	meta = manifest.SSTableMeta{
		File:    outPath,
		Level:   0,
		MinKey:  minKey,
		MaxKey:  maxKey,
		Size:    fi.Size(),
		MinSeq:  minSeq,
		MaxSeq:  maxSeq,
		Created: time.Now().UnixNano(),
	}
	return meta, true, nil
}

// clone copies a byte slice.
func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// minHeap implements a heap of *item by lexicographic key.
type minHeap []*item

func (h minHeap) Len() int            { return len(h) }
func (h minHeap) Less(i, j int) bool  { return bytes.Compare(h[i].key, h[j].key) < 0 }
func (h minHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(*item)) }
func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	*h = old[0 : n-1]
	return it
}
