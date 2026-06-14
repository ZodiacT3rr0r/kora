package compaction

import (
	"os"
	"path/filepath"
	"testing"

	"kora/internal/engine"
	"kora/internal/engine/manifest"
	"kora/internal/engine/sstable"
)

func TestSchedulerSelectsCompactionWhenL0FileCountExceeded(t *testing.T) {
	tmpDir := t.TempDir()
	sched := NewScheduler()

	// Create multiple Level 0 SSTables
	var metas []manifest.SSTableMeta
	for i := 0; i < 5; i++ {
		path := filepath.Join(tmpDir, "sst"+string(rune('0'+i))+".sst")
		w, _ := sstable.NewWriter(path)
		w.Add(&engine.Entry{
			Key:    []byte("key" + string(rune('0'+i))),
			Value:  []byte("val" + string(rune('0'+i))),
			Kind:   engine.EntryKindSet,
			SeqNum: uint64(i + 1),
		})
		w.Close()

		fi, _ := os.Stat(path)
		metas = append(metas, manifest.SSTableMeta{
			File:    path,
			Level:   0,
			MinKey:  []byte("key" + string(rune('0'+i))),
			MaxKey:  []byte("key" + string(rune('0'+i))),
			Size:    fi.Size(),
			MinSeq:  uint64(i + 1),
			MaxSeq:  uint64(i + 1),
			Created: int64(i),
		})
	}

	// Select job (should trigger because we have 5 L0 files and threshold is 4)
	job := sched.SelectCompactionJob(metas, tmpDir)
	if job == nil {
		t.Error("expected compaction job, got nil")
		return
	}

	if len(job.InputFiles) != 5 {
		t.Errorf("expected 5 input files, got %d", len(job.InputFiles))
	}
	if job.TargetLevel != 1 {
		t.Errorf("expected target level 1, got %d", job.TargetLevel)
	}
}

func TestSchedulerNoCompactionWhenL0BelowThreshold(t *testing.T) {
	tmpDir := t.TempDir()
	sched := NewScheduler()

	// Create only 2 Level 0 SSTables (below threshold of 4)
	var metas []manifest.SSTableMeta
	for i := 0; i < 2; i++ {
		path := filepath.Join(tmpDir, "sst"+string(rune('0'+i))+".sst")
		w, _ := sstable.NewWriter(path)
		w.Add(&engine.Entry{
			Key:    []byte("key" + string(rune('0'+i))),
			Value:  []byte("val" + string(rune('0'+i))),
			Kind:   engine.EntryKindSet,
			SeqNum: uint64(i + 1),
		})
		w.Close()

		fi, _ := os.Stat(path)
		metas = append(metas, manifest.SSTableMeta{
			File:    path,
			Level:   0,
			MinKey:  []byte("key" + string(rune('0'+i))),
			MaxKey:  []byte("key" + string(rune('0'+i))),
			Size:    fi.Size(),
			MinSeq:  uint64(i + 1),
			MaxSeq:  uint64(i + 1),
			Created: int64(i),
		})
	}

	// Select job (should return nil because we have 2 L0 files and threshold is 4)
	job := sched.SelectCompactionJob(metas, tmpDir)
	if job != nil {
		t.Error("expected no compaction job, got one")
	}
}

func TestExecuteCompactionJobMergesFiles(t *testing.T) {
	tmpDir := t.TempDir()

	// Create two Level 0 SSTables
	path1 := filepath.Join(tmpDir, "sst1.sst")
	w1, _ := sstable.NewWriter(path1)
	w1.Add(&engine.Entry{Key: []byte("a"), Value: []byte("val_a"), Kind: engine.EntryKindSet, SeqNum: 1})
	w1.Add(&engine.Entry{Key: []byte("c"), Value: []byte("val_c_old"), Kind: engine.EntryKindSet, SeqNum: 2})
	w1.Close()

	path2 := filepath.Join(tmpDir, "sst2.sst")
	w2, _ := sstable.NewWriter(path2)
	w2.Add(&engine.Entry{Key: []byte("b"), Value: []byte("val_b"), Kind: engine.EntryKindSet, SeqNum: 3})
	w2.Add(&engine.Entry{Key: []byte("c"), Value: []byte("val_c_new"), Kind: engine.EntryKindSet, SeqNum: 4})
	w2.Close()

	fi1, _ := os.Stat(path1)
	fi2, _ := os.Stat(path2)

	// Create compaction job
	job := &CompactionJob{
		InputFiles: []manifest.SSTableMeta{
			{
				File:    path1,
				Level:   0,
				MinKey:  []byte("a"),
				MaxKey:  []byte("c"),
				Size:    fi1.Size(),
				MinSeq:  1,
				MaxSeq:  2,
				Created: 0,
			},
			{
				File:    path2,
				Level:   0,
				MinKey:  []byte("b"),
				MaxKey:  []byte("c"),
				Size:    fi2.Size(),
				MinSeq:  3,
				MaxSeq:  4,
				Created: 1,
			},
		},
		OutputPath:  filepath.Join(tmpDir, "compacted.sst"),
		TargetLevel: 1,
	}

	// Execute compaction
	outMeta, err := ExecuteCompactionJob(job)
	if err != nil {
		t.Fatalf("compaction job failed: %v", err)
	}

	if outMeta.Level != 1 {
		t.Errorf("expected output level 1, got %d", outMeta.Level)
	}

	// Verify merged SSTable contents
	r, _ := sstable.Open(outMeta.File)
	defer r.Close()

	e_a, _ := r.Get([]byte("a"))
	if e_a == nil || e_a.SeqNum != 1 {
		t.Error("merged sst missing key a")
	}

	e_b, _ := r.Get([]byte("b"))
	if e_b == nil || e_b.SeqNum != 3 {
		t.Error("merged sst missing key b")
	}

	e_c, _ := r.Get([]byte("c"))
	if e_c == nil || e_c.SeqNum != 4 {
		t.Error("merged sst key c should have seqnum 4 (LWW)")
	}
}
