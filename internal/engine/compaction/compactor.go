package compaction

import (
    "container/heap"
    "bytes"
    "os"
    "time"

    "kora/internal/engine"
    "kora/internal/engine/manifest"
    "kora/internal/engine/sstable"
)

// item represents a heap element during merge
type item struct{ key []byte; idx int }

// MergeFiles performs a k-way merge of sorted SSTable files listed in metas
// and writes a single merged SSTable to outPath. It returns the resulting
// SSTableMeta for the merged file.
func MergeFiles(metas []manifest.SSTableMeta, outPath string) (manifest.SSTableMeta, error) {
    var meta manifest.SSTableMeta
    if len(metas) == 0 {
        return meta, nil
    }

    // open readers and iterators
    type src struct {
        r   *sstable.Reader
        it  *sstable.Iterator
        cur *engine.Entry
    }
    srcs := make([]*src, 0, len(metas))
    for _, m := range metas {
        r, err := sstable.Open(m.File)
        if err != nil {
            // skip unreadable files
            continue
        }
        it := r.Iterator()
        s := &src{r: r, it: it}
        if it.Next() {
            s.cur = it.Current()
        }
        srcs = append(srcs, s)
    }

    // heap of indices by current key
    h := &minHeap{}
    heap.Init(h)
    for i, s := range srcs {
        if s.cur != nil {
            heap.Push(h, &item{key: clone(s.cur.Key), idx: i})
        }
    }

    w, err := sstable.NewWriter(outPath)
    if err != nil {
        // close readers
        for _, s := range srcs {
            if s.r != nil {
                s.r.Close()
            }
        }
        return meta, err
    }

    var minKey, maxKey []byte
    var minSeq, maxSeq uint64
    first := true

    for h.Len() > 0 {
        it := heap.Pop(h).(*item)
        key := it.key
        idx := it.idx

        // collect all entries with this key across sources
        group := []*engine.Entry{}
        // include popped one
        if srcs[idx].cur != nil && bytes.Equal(srcs[idx].cur.Key, key) {
            group = append(group, srcs[idx].cur)
            // advance this source
            if srcs[idx].it.Next() {
                srcs[idx].cur = srcs[idx].it.Current()
                heap.Push(h, &item{key: clone(srcs[idx].cur.Key), idx: idx})
            } else {
                srcs[idx].cur = nil
            }
        }
        // drain other sources with same key
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

        // choose winner: highest SeqNum (last-write-wins)
        var winner *engine.Entry
        for _, e := range group {
            if winner == nil || e.SeqNum > winner.SeqNum {
                winner = e
            }
        }
        if winner == nil {
            continue
        }

        // update meta bounds
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

        // write the winning entry (including deletes/tombstones)
        if err := w.Add(winner); err != nil {
            w.Close()
            for _, s := range srcs {
                if s.r != nil {
                    s.r.Close()
                }
            }
            return meta, err
        }
    }

    if err := w.Close(); err != nil {
        for _, s := range srcs {
            if s.r != nil {
                s.r.Close()
            }
        }
        return meta, err
    }

    // close readers
    for _, s := range srcs {
        if s.r != nil {
            s.r.Close()
        }
    }

    fi, err := os.Stat(outPath)
    if err != nil {
        return meta, err
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
    return meta, nil
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

// minHeap implements a heap of *item by lexicographic key
type minHeap []*item

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return bytes.Compare(h[i].key, h[j].key) < 0 }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x interface{}) { *h = append(*h, x.(*item)) }
func (h *minHeap) Pop() interface{} {
    old := *h
    n := len(old)
    it := old[n-1]
    *h = old[0 : n-1]
    return it
}
