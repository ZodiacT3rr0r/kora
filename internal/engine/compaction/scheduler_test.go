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

func TestRandomIDGeneratesUniqueIdentifiers(t *testing.T) {
	seen := make(map[string]struct{}, 64)
	for i := 0; i < 64; i++ {
		id := randomID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate randomID generated on iteration %d: %s", i, id)
		}
		seen[id] = struct{}{}
	}
}

func TestTombstoneDroppedAtBottomLevelAndPreservedAboveLowerLevel(t *testing.T) {
	tmpDir := t.TempDir()

	p1 := filepath.Join(tmpDir, "l0_1.sst")
	w1, _ := sstable.NewWriter(p1)
	_ = w1.Add(&engine.Entry{Key: []byte("drop_me"), Value: []byte("old"), Kind: engine.EntryKindSet, SeqNum: 1})
	_ = w1.Add(&engine.Entry{Key: []byte("keep_tomb"), Value: []byte("v"), Kind: engine.EntryKindSet, SeqNum: 2})
	_ = w1.Add(&engine.Entry{Key: []byte("live"), Value: []byte("ok"), Kind: engine.EntryKindSet, SeqNum: 3})
	_ = w1.Close()

	p2 := filepath.Join(tmpDir, "l0_2.sst")
	w2, _ := sstable.NewWriter(p2)
	_ = w2.Add(engine.NewDeleteEntry([]byte("drop_me"), 4))
	_ = w2.Add(engine.NewDeleteEntry([]byte("keep_tomb"), 5))
	_ = w2.Close()

	fi1, _ := os.Stat(p1)
	fi2, _ := os.Stat(p2)

	// Simulate an L2 SSTable whose range ["keep_a", "keep_z"] overlaps "keep_tomb" but NOT "drop_me".
	job := &CompactionJob{
		InputFiles: []manifest.SSTableMeta{
			{File: p1, Level: 0, MinKey: []byte("drop_me"), MaxKey: []byte("live"), Size: fi1.Size(), MinSeq: 1, MaxSeq: 3},
			{File: p2, Level: 0, MinKey: []byte("drop_me"), MaxKey: []byte("keep_tomb"), Size: fi2.Size(), MinSeq: 4, MaxSeq: 5},
		},
		OutputPath:  filepath.Join(tmpDir, "out_l1.sst"),
		TargetLevel: 1,
		LowerLevelFiles: []manifest.SSTableMeta{
			{File: "l2.sst", Level: 2, MinKey: []byte("keep_a"), MaxKey: []byte("keep_z"), Size: 100},
		},
		CheckBottomTombstone: true,
	}

	outMeta, wrote, err := ExecuteCompactionJobFull(job)
	if err != nil || !wrote {
		t.Fatalf("ExecuteCompactionJobFull failed: wrote=%v err=%v", wrote, err)
	}

	r, err := sstable.Open(outMeta.File)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// "drop_me" has no overlapping lower level -> tombstone must be purged.
	if _, err := r.Get([]byte("drop_me")); err == nil {
		t.Fatal("expected tombstone for drop_me to be dropped when no lower level overlaps")
	}
	// "keep_tomb" overlaps L2 ["keep_a", "keep_z"] -> tombstone must be preserved to prevent ghost reads.
	entKeep, err := r.Get([]byte("keep_tomb"))
	if err != nil || entKeep.Kind != engine.EntryKindDelete {
		t.Fatalf("expected tombstone for keep_tomb to be preserved, got ent=%+v err=%v", entKeep, err)
	}
	// "live" must remain intact.
	entLive, err := r.Get([]byte("live"))
	if err != nil || string(entLive.Value) != "ok" {
		t.Fatalf("expected live=ok, got ent=%+v err=%v", entLive, err)
	}
}

func TestLeveledCompactionCascadesL1ToL2(t *testing.T) {
	tmpDir := t.TempDir()
	sched := NewSchedulerWithConfig(Config{
		Level0FileThreshold: 4,
		Level0SizeThreshold: 10 * 1024 * 1024,
		BaseLevelSize:       50, // tiny L1 budget so L1 -> L2 triggers
		LevelMultiplier:     10,
		MaxLevels:           4,
	})

	l1Path := filepath.Join(tmpDir, "l1.sst")
	w1, _ := sstable.NewWriter(l1Path)
	_ = w1.Add(&engine.Entry{Key: []byte("b"), Value: []byte("newer"), Kind: engine.EntryKindSet, SeqNum: 5})
	_ = w1.Close()
	fi1, _ := os.Stat(l1Path)

	l2Path := filepath.Join(tmpDir, "l2.sst")
	w2, _ := sstable.NewWriter(l2Path)
	_ = w2.Add(&engine.Entry{Key: []byte("a"), Value: []byte("va"), Kind: engine.EntryKindSet, SeqNum: 1})
	_ = w2.Add(&engine.Entry{Key: []byte("b"), Value: []byte("older"), Kind: engine.EntryKindSet, SeqNum: 2})
	_ = w2.Close()
	fi2, _ := os.Stat(l2Path)

	metas := []manifest.SSTableMeta{
		{File: l1Path, Level: 1, MinKey: []byte("b"), MaxKey: []byte("b"), Size: fi1.Size(), MinSeq: 5, MaxSeq: 5, Created: 2},
		{File: l2Path, Level: 2, MinKey: []byte("a"), MaxKey: []byte("b"), Size: fi2.Size(), MinSeq: 1, MaxSeq: 2, Created: 1},
	}

	job := sched.SelectCompactionJob(metas, tmpDir)
	if job == nil {
		t.Fatal("expected L1 -> L2 compaction job when L1 exceeds BaseLevelSize")
	}
	if job.TargetLevel != 2 {
		t.Fatalf("expected TargetLevel 2, got %d", job.TargetLevel)
	}
	if len(job.InputFiles) != 2 {
		t.Fatalf("expected L1 victim + overlapping L2 file (2 inputs), got %d", len(job.InputFiles))
	}
}
