package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"
	"sync"

	"kora/internal/engine"
	"kora/internal/util"
)

// On-disk layout:
// [records...][index bytes][8:indexOffset][4:indexLen][4:crc32]
// index bytes: sequence of [4 keyLen][key][8 offset]

var (
	ErrNotFound = errors.New("sstable: key not found")
)

type Writer struct {
	mu      sync.Mutex
	f       *os.File
	path    string
	offsets []int64
	keys    [][]byte
	closed  bool
}

// NewWriter creates a new SSTable file (truncating if exists).
func NewWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, path: path}, nil
}

func (w *Writer) Add(e *engine.Entry) error {
	if e == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("writer closed")
	}
	// position where record will be written
	pos, err := w.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	payload := serializeEntry(e)
	crc := util.CRC32(payload)
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	if _, err := w.f.Write(lenBuf[:]); err != nil {
		return err
	}
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	if _, err := w.f.Write(crcBuf[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(payload); err != nil {
		return err
	}
	w.offsets = append(w.offsets, pos)
	// store a copy of key
	kcopy := make([]byte, len(e.Key))
	copy(kcopy, e.Key)
	w.keys = append(w.keys, kcopy)
	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	// write index
	idxOff, err := w.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	// build index bytes
	// sort keys with offsets by key
	pairs := make([]struct{ key []byte; off int64 }, len(w.keys))
	for i := range w.keys {
		pairs[i] = struct{ key []byte; off int64 }{key: w.keys[i], off: w.offsets[i]}
	}
	sort.Slice(pairs, func(i, j int) bool { return bytes.Compare(pairs[i].key, pairs[j].key) < 0 })
	var buf bytes.Buffer
	for _, p := range pairs {
		var klen [4]byte
		binary.LittleEndian.PutUint32(klen[:], uint32(len(p.key)))
		buf.Write(klen[:])
		buf.Write(p.key)
		var offbuf [8]byte
		binary.LittleEndian.PutUint64(offbuf[:], uint64(p.off))
		buf.Write(offbuf[:])
	}
	indexBytes := buf.Bytes()
	if _, err := w.f.Write(indexBytes); err != nil {
		return err
	}
	// write footer: indexOffset (8), indexLen (4), crc32 (4)
	var footer [16]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(idxOff))
	binary.LittleEndian.PutUint32(footer[8:12], uint32(len(indexBytes)))
	binary.LittleEndian.PutUint32(footer[12:16], util.CRC32(indexBytes))
	if _, err := w.f.Write(footer[:]); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.closed = true
	return w.f.Close()
}

// Reader provides read-only access to an SSTable file.
type Reader struct {
	mu        sync.RWMutex
	f         *os.File
	path      string
	indexKeys [][]byte
	offsets   []int64
	dataEnd   int64 // offset where data region ends (indexOffset)
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	// read footer
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	sz := fi.Size()
	if sz < 16 {
		f.Close()
		return nil, errors.New("file too small to be sstable")
	}
	// read last 16 bytes
	if _, err := f.Seek(sz-16, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	var footer [16]byte
	if _, err := io.ReadFull(f, footer[:]); err != nil {
		f.Close()
		return nil, err
	}
	idxOff := int64(binary.LittleEndian.Uint64(footer[0:8]))
	idxLen := int(binary.LittleEndian.Uint32(footer[8:12]))
	crc := binary.LittleEndian.Uint32(footer[12:16])
	if idxOff < 0 || int64(idxLen) < 0 || idxOff+int64(idxLen) > sz-16 {
		f.Close()
		return nil, errors.New("invalid index")
	}
	// read index bytes
	if _, err := f.Seek(idxOff, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	indexBytes := make([]byte, idxLen)
	if _, err := io.ReadFull(f, indexBytes); err != nil {
		f.Close()
		return nil, err
	}
	if !util.VerifyCRC32(indexBytes, crc) {
		f.Close()
		return nil, errors.New("index crc mismatch")
	}
	// parse index
	var keys [][]byte
	var offs []int64
	off := 0
	for off < len(indexBytes) {
		if off+4 > len(indexBytes) {
			f.Close()
			return nil, errors.New("corrupt index")
		}
		klen := int(binary.LittleEndian.Uint32(indexBytes[off : off+4]))
		off += 4
		if off+klen+8 > len(indexBytes) {
			f.Close()
			return nil, errors.New("corrupt index entry")
		}
		k := make([]byte, klen)
		copy(k, indexBytes[off:off+klen])
		off += klen
		offset := int64(binary.LittleEndian.Uint64(indexBytes[off : off+8]))
		off += 8
		keys = append(keys, k)
		offs = append(offs, offset)
	}
	return &Reader{f: f, path: path, indexKeys: keys, offsets: offs, dataEnd: idxOff}, nil
}

func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

// Get returns the entry for a key, or ErrNotFound.
func (r *Reader) Get(key []byte) (*engine.Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	idx := sort.Search(len(r.indexKeys), func(i int) bool { return bytes.Compare(r.indexKeys[i], key) >= 0 })
	if idx < len(r.indexKeys) && bytes.Equal(r.indexKeys[idx], key) {
		return r.readAtOffset(r.offsets[idx])
	}
	return nil, ErrNotFound
}

// Seek returns the first entry with key >= target, or nil if past end.
func (r *Reader) Seek(target []byte) (*engine.Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	idx := sort.Search(len(r.indexKeys), func(i int) bool { return bytes.Compare(r.indexKeys[i], target) >= 0 })
	if idx >= len(r.indexKeys) {
		return nil, ErrNotFound
	}
	return r.readAtOffset(r.offsets[idx])
}

func (r *Reader) readAtOffset(off int64) (*engine.Entry, error) {
	if _, err := r.f.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(r.f, lenBuf[:]); err != nil {
		return nil, err
	}
	payloadLen := binary.LittleEndian.Uint32(lenBuf[:])
	var crcBuf [4]byte
	if _, err := io.ReadFull(r.f, crcBuf[:]); err != nil {
		return nil, err
	}
	crc := binary.LittleEndian.Uint32(crcBuf[:])
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r.f, payload); err != nil {
		return nil, err
	}
	if !util.VerifyCRC32(payload, crc) {
		return nil, errors.New("record crc mismatch")
	}
	entry, err := deserializeEntry(payload)
	if err != nil {
		return nil, err
	}
	return entry, nil
}

// Iterator for sequential scanning of record region.
type Iterator struct {
	r       *Reader
	nextOff int64
	cur     *engine.Entry
	err     error
}

func (r *Reader) Iterator() *Iterator {
	return &Iterator{r: r, nextOff: 0}
}

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	if it.nextOff >= it.r.dataEnd {
		return false
	}
	ent, err := it.r.readAtOffset(it.nextOff)
	if err != nil {
		it.err = err
		return false
	}
	// advance nextOff by sizeof(record)
	// readAtOffset re-read the length, so compute: 4 + 4 + payloadLen
	// To compute payloadLen, re-serialize? safer: seek and read length again
	if _, err := it.r.f.Seek(it.nextOff, io.SeekStart); err != nil {
		it.err = err
		return false
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(it.r.f, lenBuf[:]); err != nil {
		it.err = err
		return false
	}
	payloadLen := int64(binary.LittleEndian.Uint32(lenBuf[:]))
	it.nextOff += 4 + 4 + payloadLen
	it.cur = ent
	return true
}

func (it *Iterator) Current() *engine.Entry { return it.cur }

func (it *Iterator) Err() error { return it.err }

// ---- Serialization helpers (same format as WAL) ----
func serializeEntry(e *engine.Entry) []byte {
	keyLen := uint32(len(e.Key))
	valLen := uint32(len(e.Value))
	payloadLen := 1 + 8 + 8 + 4 + 4 + keyLen + valLen
	buf := make([]byte, payloadLen)
	off := 0
	buf[off] = byte(e.Kind)
	off++
	binary.LittleEndian.PutUint64(buf[off:], e.SeqNum)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], uint64(e.ExpiresAt))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], keyLen)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], valLen)
	off += 4
	copy(buf[off:], e.Key)
	off += int(keyLen)
	copy(buf[off:], e.Value)
	return buf
}

func deserializeEntry(b []byte) (*engine.Entry, error) {
	if len(b) < 1+8+8+4+4 {
		return nil, errors.New("corrupt record")
	}
	off := 0
	kind := engine.EntryKind(b[off])
	off++
	seq := binary.LittleEndian.Uint64(b[off:])
	off += 8
	expires := int64(binary.LittleEndian.Uint64(b[off:]))
	off += 8
	keyLen := binary.LittleEndian.Uint32(b[off:])
	off += 4
	valLen := binary.LittleEndian.Uint32(b[off:])
	off += 4
	if uint32(len(b[off:])) < keyLen+valLen {
		return nil, errors.New("corrupt record")
	}
	key := make([]byte, keyLen)
	copy(key, b[off:off+int(keyLen)])
	off += int(keyLen)
	val := make([]byte, valLen)
	copy(val, b[off:off+int(valLen)])
	return &engine.Entry{Key: key, Value: val, Kind: kind, SeqNum: seq, ExpiresAt: expires}, nil
}
