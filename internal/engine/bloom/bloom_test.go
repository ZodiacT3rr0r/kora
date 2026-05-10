package bloom

import (
	"testing"
)

func TestBloomBasic(t *testing.T) {
	bf := New(100, 0.01)
	m, k, n := bf.Stats()
	if m == 0 || k == 0 {
		t.Fatalf("invalid stats: m=%d k=%d n=%d", m, k, n)
	}

	items := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
		[]byte("elderberry"),
	}
	for _, it := range items {
		bf.Add(it)
	}

	for _, it := range items {
		if !bf.MightContain(it) {
			t.Fatalf("expected item present: %s", it)
		}
	}

	// A value not added should usually return false. With proper sizing this should be false.
	if bf.MightContain([]byte("fig")) {
		// Not a test failure necessarily (false positive possible), but warn.
		t.Log("note: false positive observed for " + "fig")
	}
}

func TestBloomClear(t *testing.T) {
	bf := New(10, 0.01)
	bf.Add([]byte("x"))
	if !bf.MightContain([]byte("x")) {
		t.Fatalf("expected x present")
	}
	bf.Clear()
	if bf.MightContain([]byte("x")) {
		t.Fatalf("expected x to be cleared")
	}
}

func TestBloomConcurrency(t *testing.T) {
	bf := New(1000, 0.01)
	done := make(chan struct{})
	// concurrent writers
	for i := 0; i < 10; i++ {
		go func(i int) {
			for j := 0; j < 100; j++ {
				bf.Add([]byte(string(rune('a'+i)) + string(rune(j))))
			}
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	// spot check
	if !bf.MightContain([]byte("a0")) {
		t.Fatalf("expected a0 present")
	}
}
