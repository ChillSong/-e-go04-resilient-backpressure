// Package dedup implements a bounded deduplication window keyed by event_id.
//
// Why a window and not an unbounded set: a real consumer cannot remember every
// event_id forever. The retries in this dataset arrive at most ~3 minutes after
// the original (the generator delays a duplicate by 2s–180s), and the duplicate
// travels the stream close behind its original. So we only need to remember the
// recent past: a window over arrival order, large enough that an original is
// still remembered when its retry shows up.
//
// The window is an LRU set with a fixed capacity N: inserting the (N+1)-th
// distinct id evicts the least-recently-inserted one. Memory is bounded to N
// ids regardless of how many events flow through. See decisions.md for how N is
// sized against the retry gap and the measured throughput.
package dedup

import "container/list"

// Window is a fixed-capacity set of event_ids in arrival order. Not safe for
// concurrent use; the consumer drives it from a single goroutine.
type Window struct {
	cap   int
	items map[string]*list.Element
	order *list.List // front = most recently inserted, back = oldest
}

// New creates a window that remembers at most capacity distinct event_ids.
func New(capacity int) *Window {
	if capacity < 1 {
		capacity = 1
	}
	return &Window{
		cap:   capacity,
		items: make(map[string]*list.Element, capacity),
		order: list.New(),
	}
}

// Seen reports whether id was already recorded in the window. If it is new, Seen
// records it (evicting the oldest id when at capacity) and returns false. If it
// was already present, it is a duplicate and Seen returns true without changing
// the aggregate.
func (w *Window) Seen(id string) bool {
	if _, ok := w.items[id]; ok {
		return true
	}
	w.items[id] = w.order.PushFront(id)
	if w.order.Len() > w.cap {
		oldest := w.order.Back()
		if oldest != nil {
			w.order.Remove(oldest)
			delete(w.items, oldest.Value.(string))
		}
	}
	return false
}

// Len is the number of ids currently remembered.
func (w *Window) Len() int { return w.order.Len() }

// Cap is the window capacity.
func (w *Window) Cap() int { return w.cap }
