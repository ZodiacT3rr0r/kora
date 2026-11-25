package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"kora/internal/engine"
	"kora/internal/util"
)

// Record format:
// [4 bytes payloadLen][4 bytes crc32][payload bytes]
// Payload format:
// [1 byte kind][8 bytes seqnum][8 bytes expiresAt][4 bytes keyLen][4 bytes valLen][key][value]

var (
	ErrCorruptRecord = errors.New("wal: corrupt record")
)

// WAL provides an append-only write-ahead log with simple recovery/truncate semantics.
type WAL struct {
	mu     sync.Mutex
	f      *os.File
	path   string
	offset int64 // next write position
}

// Open opens or creates a WAL at the given path.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	// Seek to end to determine current offset.
	off, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{f: f, path: path, offset: off}, nil
}

// Path returns the WAL file path.
func (w *WAL) Path() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.path
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// Sync flushes writes to disk.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	return w.f.Sync()
}

// Append writes an entry to the WAL and returns the file position where the record starts.
func (w *WAL) Append(e *engine.Entry) (int64, error) {
	if e == nil {
		return 0, nil
	}
	payload := serializeEntry(e)
	crc := util.CRC32(payload)
	payloadLen := uint32(len(payload))

	w.mu.Lock()
	defer w.mu.Unlock()
	pos := w.offset

	// Write length
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], payloadLen)
	if _, err := w.f.Write(lenBuf[:]); err != nil {
		return 0, err
	}
	// Write crc
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], crc)
	if _, err := w.f.Write(crcBuf[:]); err != nil {
		return 0, err
	}
	// Write payload
	if _, err := w.f.Write(payload); err != nil {
		return 0, err
	}
	w.offset += int64(4 + 4 + payloadLen)
	return pos, nil
}

// Recover reads all complete records from the WAL and returns reconstructed entries.
// If the file ends with a partial/truncated record, recovery truncates the torn tail
// back to the last valid record boundary so subsequent appends never land past stray bytes.
func (w *WAL) Recover() ([]*engine.Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	var out []*engine.Entry
	var lastValidOffset int64
	truncatedTail := false

	for {
		var lenBuf [4]byte
		_, err := io.ReadFull(w.f, lenBuf[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			// partial length field -> stop and truncate torn tail
			truncatedTail = true
			break
		}
		if err != nil {
			return nil, err
		}
		payloadLen := binary.LittleEndian.Uint32(lenBuf[:])

		var crcBuf [4]byte
		if _, err := io.ReadFull(w.f, crcBuf[:]); err != nil {
			// partial crc -> stop and truncate torn tail
			truncatedTail = true
			break
		}
		crc := binary.LittleEndian.Uint32(crcBuf[:])

		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(w.f, payload); err != nil {
			// partial payload -> stop and truncate torn tail
			truncatedTail = true
			break
		}

		if !util.VerifyCRC32(payload, crc) {
			return nil, ErrCorruptRecord
		}
		entry, err := deserializeEntry(payload)
		if err != nil {
			return nil, err
		}
		out = append(out, entry)
		lastValidOffset += int64(4 + 4 + payloadLen)
	}

	if truncatedTail {
		if err := w.f.Truncate(lastValidOffset); err != nil {
			return nil, err
		}
		if err := w.f.Sync(); err != nil {
			return nil, err
		}
	}
	if _, err := w.f.Seek(lastValidOffset, io.SeekStart); err != nil {
		return nil, err
	}
	w.offset = lastValidOffset
	return out, nil
}

// Truncate truncates the WAL file to the given position (pos must be <= current size).
func (w *WAL) Truncate(pos int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Truncate(pos); err != nil {
		return err
	}
	if _, err := w.f.Seek(pos, io.SeekStart); err != nil {
		return err
	}
	w.offset = pos
	return nil
}

// ---- serialization helpers ----

func serializeEntry(e *engine.Entry) []byte {
	// payload length deterministic calculation
	keyLen := uint32(len(e.Key))
	valLen := uint32(len(e.Value))
	payloadLen := 1 + 8 + 8 + 4 + 4 + keyLen + valLen
	buf := make([]byte, payloadLen)
	off := 0
	// kind
	buf[off] = byte(e.Kind)
	off++
	// seqnum
	binary.LittleEndian.PutUint64(buf[off:], e.SeqNum)
	off += 8
	// expiresAt (int64)
	binary.LittleEndian.PutUint64(buf[off:], uint64(e.ExpiresAt))
	off += 8
	// keyLen
	binary.LittleEndian.PutUint32(buf[off:], keyLen)
	off += 4
	// valLen
	binary.LittleEndian.PutUint32(buf[off:], valLen)
	off += 4
	// key
	copy(buf[off:], e.Key)
	off += int(keyLen)
	// value
	copy(buf[off:], e.Value)
	return buf
}

func deserializeEntry(b []byte) (*engine.Entry, error) {
	if len(b) < 1+8+8+4+4 {
		return nil, ErrCorruptRecord
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
		return nil, ErrCorruptRecord
	}
	key := make([]byte, keyLen)
	copy(key, b[off:off+int(keyLen)])
	off += int(keyLen)
	val := make([]byte, valLen)
	copy(val, b[off:off+int(valLen)])

	return &engine.Entry{
		Key:       key,
		Value:     val,
		Kind:      kind,
		SeqNum:    seq,
		ExpiresAt: expires,
	}, nil
}
