package compaction

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"kora/internal/engine/manifest"
)

var idFallbackSeq atomic.Uint64

// Config tunes leveled compaction thresholds across L0..MaxLevels.
type Config struct {
	Level0FileThreshold int
	Level0SizeThreshold int64
	BaseLevelSize       int64 // L1 byte budget; L(k) budget = BaseLevelSize * LevelMultiplier^(k-1)
	LevelMultiplier     int64
	MaxLevels           int
}

// DefaultConfig returns standard RocksDB-inspired leveled compaction defaults.
func DefaultConfig() Config {
	return Config{
		Level0FileThreshold: 4,
		Level0SizeThreshold: 10 * 1024 * 1024, // 10 MB
		BaseLevelSize:       10 * 1024 * 1024, // L1 = 10 MB, L2 = 100 MB, L3 = 1 GB...
		LevelMultiplier:     10,
		MaxLevels:           6,
	}
}

// Scheduler orchestrates multi-level LSM compaction (L0 -> L1 -> L2 -> ... -> MaxLevels).
type Scheduler struct {
	mu                  sync.Mutex
	level0FileThreshold int
	level0SizeThreshold int64
	baseLevelSize       int64
	levelMultiplier     int64
	maxLevels           int
}

// NewScheduler creates a Scheduler with default leveled compaction policies.
func NewScheduler() *Scheduler {
	return NewSchedulerWithConfig(DefaultConfig())
}

// NewSchedulerWithConfig creates a Scheduler with custom thresholds (useful for tests).
func NewSchedulerWithConfig(cfg Config) *Scheduler {
	if cfg.Level0FileThreshold <= 0 {
		cfg.Level0FileThreshold = 4
	}
	if cfg.Level0SizeThreshold <= 0 {
		cfg.Level0SizeThreshold = 10 * 1024 * 1024
	}
	if cfg.BaseLevelSize <= 0 {
		cfg.BaseLevelSize = 10 * 1024 * 1024
	}
	if cfg.LevelMultiplier <= 1 {
		cfg.LevelMultiplier = 10
	}
	if cfg.MaxLevels < 2 {
		cfg.MaxLevels = 6
	}
	return &Scheduler{
		level0FileThreshold: cfg.Level0FileThreshold,
		level0SizeThreshold: cfg.Level0SizeThreshold,
		baseLevelSize:       cfg.BaseLevelSize,
		levelMultiplier:     cfg.LevelMultiplier,
		maxLevels:           cfg.MaxLevels,
	}
}

// CompactionJob describes a leveled compaction task.
type CompactionJob struct {
	InputFiles           []manifest.SSTableMeta
	OutputPath           string
	TargetLevel          int
	LowerLevelFiles      []manifest.SSTableMeta
	CheckBottomTombstone bool
}

// SelectCompactionJob examines the manifest across all levels and returns the highest-priority
// compaction job: first L0 -> L1 (merging overlapping L1 SSTables), then L(k) -> L(k+1)
// whenever level k exceeds its exponentially scaled byte budget.
func (s *Scheduler) SelectCompactionJob(files []manifest.SSTableMeta, dataDir string) *CompactionJob {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(files) == 0 {
		return nil
	}

	byLevel := make(map[int][]manifest.SSTableMeta)
	levelBytes := make(map[int]int64)
	for _, f := range files {
		byLevel[f.Level] = append(byLevel[f.Level], f)
		levelBytes[f.Level] += f.Size
	}

	// Priority 1: Level 0 -> Level 1 compaction
	l0 := byLevel[0]
	if len(l0) >= s.level0FileThreshold || levelBytes[0] >= s.level0SizeThreshold {
		if len(l0) > 0 {
			sort.Slice(l0, func(i, j int) bool {
				return l0[i].Created < l0[j].Created
			})
			minK, maxK := boundsOf(l0)
			inputs := append([]manifest.SSTableMeta(nil), l0...)
			for _, l1File := range byLevel[1] {
				if rangesOverlap(minK, maxK, l1File.MinKey, l1File.MaxKey) {
					inputs = append(inputs, l1File)
				}
			}
			return &CompactionJob{
				InputFiles:           inputs,
				OutputPath:           generateCompactedSSTablePath(dataDir),
				TargetLevel:          1,
				LowerLevelFiles:      collectLevelsAbove(files, 1),
				CheckBottomTombstone: true,
			}
		}
	}

	// Priority 2: Leveled compaction L(k) -> L(k+1) for k = 1 .. maxLevels-1
	budget := s.baseLevelSize
	for level := 1; level < s.maxLevels; level++ {
		lvlFiles := byLevel[level]
		if len(lvlFiles) > 0 && levelBytes[level] >= budget {
			sort.Slice(lvlFiles, func(i, j int) bool {
				return lvlFiles[i].Created < lvlFiles[j].Created
			})
			victim := lvlFiles[0]
			targetLevel := level + 1
			inputs := []manifest.SSTableMeta{victim}
			for _, nextFile := range byLevel[targetLevel] {
				if rangesOverlap(victim.MinKey, victim.MaxKey, nextFile.MinKey, nextFile.MaxKey) {
					inputs = append(inputs, nextFile)
				}
			}
			return &CompactionJob{
				InputFiles:           inputs,
				OutputPath:           generateCompactedSSTablePath(dataDir),
				TargetLevel:          targetLevel,
				LowerLevelFiles:      collectLevelsAbove(files, targetLevel),
				CheckBottomTombstone: true,
			}
		}
		budget *= s.levelMultiplier
	}

	return nil
}

func boundsOf(metas []manifest.SSTableMeta) ([]byte, []byte) {
	var minK, maxK []byte
	for _, m := range metas {
		if len(m.MinKey) == 0 || len(m.MaxKey) == 0 {
			return nil, nil
		}
		if minK == nil || bytes.Compare(m.MinKey, minK) < 0 {
			minK = m.MinKey
		}
		if maxK == nil || bytes.Compare(m.MaxKey, maxK) > 0 {
			maxK = m.MaxKey
		}
	}
	return minK, maxK
}

func rangesOverlap(minA, maxA, minB, maxB []byte) bool {
	if len(minA) == 0 || len(maxA) == 0 || len(minB) == 0 || len(maxB) == 0 {
		return true
	}
	return bytes.Compare(minA, maxB) <= 0 && bytes.Compare(minB, maxA) <= 0
}

func collectLevelsAbove(files []manifest.SSTableMeta, targetLevel int) []manifest.SSTableMeta {
	var out []manifest.SSTableMeta
	for _, f := range files {
		if f.Level > targetLevel {
			out = append(out, f)
		}
	}
	return out
}

func keyMayExistInLowerLevels(key []byte, lower []manifest.SSTableMeta) bool {
	for _, f := range lower {
		if len(f.MinKey) == 0 || len(f.MaxKey) == 0 {
			return true
		}
		if bytes.Compare(key, f.MinKey) >= 0 && bytes.Compare(key, f.MaxKey) <= 0 {
			return true
		}
	}
	return false
}

// generateCompactedSSTablePath generates a collision-free filename for a compacted SSTable.
func generateCompactedSSTablePath(dataDir string) string {
	return filepath.Join(dataDir, "sstable-compacted-"+randomID()+".sst")
}

// randomID generates a cryptographically random 16-hex-character identifier via crypto/rand.
func randomID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	seq := idFallbackSeq.Add(1)
	return fmt.Sprintf("%016x", uint64(time.Now().UnixNano())^seq)
}

// ExecuteCompactionJob runs a compaction job and returns (outMeta, err).
// If all entries were dropped tombstones at the bottommost level, outMeta.File is empty.
func ExecuteCompactionJob(job *CompactionJob) (manifest.SSTableMeta, error) {
	outMeta, _, err := ExecuteCompactionJobFull(job)
	return outMeta, err
}

// ExecuteCompactionJobFull runs a compaction job and reports whether an output SSTable
// was produced (wroteOutput is false when every entry in the merge was a safely dropped tombstone).
func ExecuteCompactionJobFull(job *CompactionJob) (manifest.SSTableMeta, bool, error) {
	var dropFilter func(key []byte) bool
	if job.CheckBottomTombstone {
		lower := job.LowerLevelFiles
		dropFilter = func(key []byte) bool {
			return !keyMayExistInLowerLevels(key, lower)
		}
	}

	outMeta, wrote, err := MergeFilesWithFilter(job.InputFiles, job.OutputPath, dropFilter)
	if err != nil || !wrote {
		return outMeta, wrote, err
	}
	outMeta.Level = job.TargetLevel
	return outMeta, true, nil
}
