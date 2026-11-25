package manifest

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"kora/internal/util"
)

// SSTableMeta describes an SSTable file tracked in the manifest.
type SSTableMeta struct {
	File    string `json:"file"`
	Level   int    `json:"level"`
	MinKey  []byte `json:"min_key"`
	MaxKey  []byte `json:"max_key"`
	Size    int64  `json:"size"`
	MinSeq  uint64 `json:"min_seq"`
	MaxSeq  uint64 `json:"max_seq"`
	Created int64  `json:"created"` // unix nanos
}

// Manifest manages metadata for SSTables.
// File format: [4 bytes payloadLen][4 bytes crc32][payload JSON]
// Saved crash-safely via write-to-temp + fsync + atomic POSIX rename.
type Manifest struct {
	mu     sync.Mutex
	path   string
	items  []SSTableMeta
	loaded bool
}

// New creates a Manifest object for a given path (file will be created on Save).
func New(path string) *Manifest {
	return &Manifest{path: path}
}

// Load reads manifest file and populates items. Returns error if file corrupt or unreadable.
// Also cleans up any leftover .tmp file from an interrupted save before rename.
func (m *Manifest) Load() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	_ = os.Remove(m.path + ".tmp")

	f, err := os.Open(m.path)
	if err != nil {
		if os.IsNotExist(err) {
			m.items = nil
			m.loaded = true
			return nil
		}
		return err
	}
	defer f.Close()
	var lenBuf [4]byte
	if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
		return err
	}
	payloadLen := binary.LittleEndian.Uint32(lenBuf[:])
	var crcBuf [4]byte
	if _, err := io.ReadFull(f, crcBuf[:]); err != nil {
		return err
	}
	crc := binary.LittleEndian.Uint32(crcBuf[:])
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(f, payload); err != nil {
		return err
	}
	if !util.VerifyCRC32(payload, crc) {
		return errors.New("manifest: crc mismatch")
	}
	var items []SSTableMeta
	if err := json.Unmarshal(payload, &items); err != nil {
		return err
	}
	m.items = items
	m.loaded = true
	return nil
}

// Save writes current manifest items to disk atomically (write .tmp, fsync, atomic rename).
func (m *Manifest) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saveLocked()
}

func (m *Manifest) saveLocked() error {
	if err := os.MkdirAll(filepath.Dir(m.path), 0755); err != nil {
		return err
	}

	b, err := json.Marshal(m.items)
	if err != nil {
		return err
	}
	crc := util.CRC32(b)

	tmpPath := m.path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	writeErr := func() error {
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(b)))
		if _, err := f.Write(lenBuf[:]); err != nil {
			return err
		}
		var crcBuf [4]byte
		binary.LittleEndian.PutUint32(crcBuf[:], crc)
		if _, err := f.Write(crcBuf[:]); err != nil {
			return err
		}
		if _, err := f.Write(b); err != nil {
			return err
		}
		return f.Sync()
	}()

	closeErr := f.Close()
	if writeErr != nil {
		_ = os.Remove(tmpPath)
		return writeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return closeErr
	}

	if err := os.Rename(tmpPath, m.path); err != nil {
		return err
	}

	// Best-effort fsync on parent directory so the rename entry is durable.
	if dirFile, err := os.Open(filepath.Dir(m.path)); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	return nil
}

// Add appends a new SSTableMeta and saves manifest to disk atomically under one lock.
func (m *Manifest) Add(meta SSTableMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.items = append(m.items, meta)
	return m.saveLocked()
}

// Remove removes an SSTable file entry by filename and saves manifest atomically.
func (m *Manifest) Remove(filename string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i := range m.items {
		if m.items[i].File == filename {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil
	}
	m.items = append(m.items[:idx], m.items[idx+1:]...)
	return m.saveLocked()
}

// Replace atomically removes all filenames in removedFiles and adds all metas in added
// within a single manifest lock and single atomic file swap. Used by compaction.
func (m *Manifest) Replace(added []SSTableMeta, removedFiles []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	removeSet := make(map[string]struct{}, len(removedFiles))
	for _, fn := range removedFiles {
		removeSet[fn] = struct{}{}
	}

	next := make([]SSTableMeta, 0, len(m.items)+len(added))
	for _, item := range m.items {
		if _, drop := removeSet[item.File]; !drop {
			next = append(next, item)
		}
	}
	next = append(next, added...)
	m.items = next
	return m.saveLocked()
}

// List returns a copy of current manifest items.
func (m *Manifest) List() []SSTableMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]SSTableMeta, len(m.items))
	copy(out, m.items)
	return out
}

// FindByKey returns sstables whose range might contain the key.
func (m *Manifest) FindByKey(key []byte) []SSTableMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []SSTableMeta
	for _, it := range m.items {
		if len(it.MinKey) == 0 || len(it.MaxKey) == 0 {
			out = append(out, it)
			continue
		}
		if bytesCompare(key, it.MinKey) >= 0 && bytesCompare(key, it.MaxKey) <= 0 {
			out = append(out, it)
		}
	}
	return out
}

func bytesCompare(a, b []byte) int {
	la := len(a)
	lb := len(b)
	min := la
	if lb < min {
		min = lb
	}
	for i := 0; i < min; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if la < lb {
		return -1
	}
	if la > lb {
		return 1
	}
	return 0
}
