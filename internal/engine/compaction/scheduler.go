package compaction

import (
	"sort"
	"sync"

	"kora/internal/engine/manifest"
)

// Scheduler orchestrates background compaction operations.
type Scheduler struct {
	mu sync.Mutex
	// Policy parameters
	level0FileThreshold int // trigger compaction if Level 0 has >= this many files
	level0SizeThreshold int64 // trigger compaction if Level 0 total size >= this many bytes
}

// NewScheduler creates a new Scheduler with default policies.
func NewScheduler() *Scheduler {
	return &Scheduler{
		level0FileThreshold: 4,     // compact when 4+ L0 files
		level0SizeThreshold: 10 * 1024 * 1024, // 10 MB
	}
}

// CompactionJob describes a compaction task.
type CompactionJob struct {
	// Files to merge
	InputFiles []manifest.SSTableMeta
	// Where to write the merged output
	OutputPath string
	// Target level for output
	TargetLevel int
}

// SelectCompactionJob examines the manifest and returns a compaction job if one is needed, or nil.
func (s *Scheduler) SelectCompactionJob(files []manifest.SSTableMeta, dataDir string) *CompactionJob {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(files) == 0 {
		return nil
	}

	// Count Level 0 files and total size
	var level0Files []manifest.SSTableMeta
	var level0Size int64
	for _, f := range files {
		if f.Level == 0 {
			level0Files = append(level0Files, f)
			level0Size += f.Size
		}
	}

	// Check if compaction is needed
	if len(level0Files) < s.level0FileThreshold && level0Size < s.level0SizeThreshold {
		return nil // no compaction needed
	}

	// Trigger compaction: merge all Level 0 files
	if len(level0Files) == 0 {
		return nil
	}

	// Sort by creation time for deterministic ordering
	sort.Slice(level0Files, func(i, j int) bool {
		return level0Files[i].Created < level0Files[j].Created
	})

	// Generate output filename
	outputPath := generateCompactedSSTablePath(dataDir)

	job := &CompactionJob{
		InputFiles: level0Files,
		OutputPath: outputPath,
		TargetLevel: 1, // Level 0 files -> Level 1
	}
	return job
}

// generateCompactedSSTablePath generates a unique filename for a compacted SSTable.
func generateCompactedSSTablePath(dataDir string) string {
	return dataDir + "/sstable-compacted-" + randomID() + ".sst"
}

// randomID generates a simple random identifier (in production, use a better method).
func randomID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	id := make([]byte, 8)
	for i := range id {
		id[i] = chars[i%len(chars)]
	}
	return string(id)
}

// ExecuteCompactionJob runs a compaction job: merges input files and returns metadata for the output.
// The caller is responsible for updating the manifest and removing old files.
func ExecuteCompactionJob(job *CompactionJob) (manifest.SSTableMeta, error) {
	outMeta, err := MergeFiles(job.InputFiles, job.OutputPath)
	if err != nil {
		return outMeta, err
	}
	// Update target level
	outMeta.Level = job.TargetLevel
	return outMeta, nil
}
