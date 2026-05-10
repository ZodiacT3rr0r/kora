package bloom

import (
	"hash/fnv"
	"math"
	"sync"
)

// BloomFilter is a simple, concurrent-safe bloom filter implementation.
// It uses double hashing (two independent hash functions) to generate k indices.
// Backing storage is a []uint64 bitset.
type BloomFilter struct {
	mu    sync.RWMutex
	bits  []uint64
	m     uint64 // number of bits
	k     uint64 // number of hash functions
	n     uint64 // expected number of items (for info)
}

// New returns a BloomFilter sized for up to n expected items and target false-positive rate fp (0<fp<1).
// It computes optimal m (bits) and k (hash functions) using standard formulas.
func New(n uint64, fp float64) *BloomFilter {
	if n == 0 {
		n = 1
	}
	if fp <= 0 || fp >= 1 {
		fp = 0.01
	}
	// m = - (n * ln(fp)) / (ln(2)^2)
	mFloat := -1.0 * float64(n) * math.Log(fp) / (math.Ln2 * math.Ln2)
	m := uint64(math.Ceil(mFloat))
	// k = (m/n) * ln2
	kFloat := (float64(m) / float64(n)) * math.Ln2
	k := uint64(math.Round(kFloat))
	if k == 0 {
		k = 1
	}
	// allocate bitset
	words := (m + 63) / 64
	bf := &BloomFilter{
		bits: make([]uint64, words),
		m:    m,
		k:    k,
		n:    n,
	}
	return bf
}

// Add inserts data into the Bloom filter.
func (bf *BloomFilter) Add(data []byte) {
	if data == nil {
		return
	}
	indices := bf.indices(data)
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for _, idx := range indices {
		word := idx / 64
		bit := idx % 64
		bf.bits[word] |= 1 << bit
	}
}

// MightContain returns true if the data might be present (may be false positive), false if definitely not present.
func (bf *BloomFilter) MightContain(data []byte) bool {
	if data == nil {
		return false
	}
	indices := bf.indices(data)
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	for _, idx := range indices {
		word := idx / 64
		bit := idx % 64
		if (bf.bits[word]&(1<<bit)) == 0 {
			return false
		}
	}
	return true
}

// Clear resets the filter to empty state.
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	for i := range bf.bits {
		bf.bits[i] = 0
	}
}

// indices computes k indices for the given data using double hashing.
func (bf *BloomFilter) indices(data []byte) []uint64 {
	h1 := fnv.New64a()
	h1.Write(data)
	sum1 := h1.Sum64()

	// second hash: use fnv of reversed data as a cheap second hash
	h2 := fnv.New64()
	// reverse copy
	rev := make([]byte, len(data))
	for i := range data {
		rev[i] = data[len(data)-1-i]
	}
	h2.Write(rev)
	sum2 := h2.Sum64()

	res := make([]uint64, bf.k)
	for i := uint64(0); i < bf.k; i++ {
		// double hashing: (h1 + i*h2) mod m
		combined := sum1 + i*sum2
		idx := combined % bf.m
		res[i] = idx
	}
	return res
}

// Stats returns current configuration: (m bits, k hashes, n expected items)
func (bf *BloomFilter) Stats() (m uint64, k uint64, n uint64) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.m, bf.k, bf.n
}
