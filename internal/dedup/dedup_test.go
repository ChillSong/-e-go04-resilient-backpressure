package dedup

import (
	"strconv"
	"testing"
)

func TestNewIdIsNotSeen(t *testing.T) {
	w := New(10)
	if w.Seen("a") {
		t.Error("first sighting of 'a' should be new, got seen")
	}
}

func TestDuplicateIsSeen(t *testing.T) {
	w := New(10)
	w.Seen("a")
	if !w.Seen("a") {
		t.Error("second sighting of 'a' should be a duplicate")
	}
}

func TestAdjacentDuplicateCaught(t *testing.T) {
	// Mirrors the generator: duplicate arrives right after the original.
	w := New(100)
	dups := 0
	stream := []string{"e1", "e1", "e2", "e3", "e3", "e4"}
	for _, id := range stream {
		if w.Seen(id) {
			dups++
		}
	}
	if dups != 2 {
		t.Errorf("got %d duplicates, want 2", dups)
	}
}

func TestEvictionDropsOldest(t *testing.T) {
	w := New(2)
	w.Seen("a") // {a}
	w.Seen("b") // {a,b}
	w.Seen("c") // inserting c evicts oldest 'a' -> {b,c}

	// Check the survivors first: a hit does not mutate the window.
	if !w.Seen("c") {
		t.Error("'c' should still be in the window")
	}
	if !w.Seen("b") {
		t.Error("'b' should still be in the window")
	}
	// 'a' was evicted, so it reads as new (and re-inserting it is fine).
	if w.Seen("a") {
		t.Error("'a' was evicted; seeing it again must read as new, not duplicate")
	}
}

func TestCapacityBoundsMemory(t *testing.T) {
	w := New(1000)
	for i := 0; i < 100000; i++ {
		w.Seen(strconv.Itoa(i))
	}
	if w.Len() > w.Cap() {
		t.Errorf("window holds %d ids, exceeds cap %d", w.Len(), w.Cap())
	}
}
