// Package backpressure holds the pieces that decide what gets delayed when the
// inventory service cannot keep up: a bounded in-memory wait buffer, an
// adaptive concurrency controller, and a gauge for the backlog signal.
package backpressure

import "sync"

// Buffer is a bounded FIFO of work waiting for the inventory service.
//
// Capacity counts both queued items AND items in flight. That is what makes
// retries safe: a failed check goes back into the slot it already holds, so a
// Requeue can never find the buffer full and never has to drop anything. Only
// brand-new work (Offer) can be refused — and refused work is not lost, it
// simply stays in the durable Redis stream until there is room (see
// decisions.md, "no perder datos").
type Buffer[T any] struct {
	mu       sync.Mutex
	queue    []T
	inflight int
	capacity int
	lowWater int
	maxDepth int
}

// NewBuffer creates a buffer holding at most capacity items (queued + in
// flight). It asks for a refill once occupancy drops below lowWater.
func NewBuffer[T any](capacity, lowWater int) *Buffer[T] {
	if capacity < 1 {
		capacity = 1
	}
	if lowWater < 1 || lowWater > capacity {
		lowWater = capacity / 2
	}
	return &Buffer[T]{capacity: capacity, lowWater: lowWater}
}

// Offer admits new work if there is room and reports whether it did.
func (b *Buffer[T]) Offer(x T) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.queue)+b.inflight >= b.capacity {
		return false
	}
	b.queue = append(b.queue, x)
	b.track()
	return true
}

// Take removes the oldest queued item and marks it in flight.
func (b *Buffer[T]) Take() (T, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var zero T
	if len(b.queue) == 0 {
		return zero, false
	}
	x := b.queue[0]
	b.queue[0] = zero
	b.queue = b.queue[1:]
	b.inflight++
	return x, true
}

// Requeue puts a failed in-flight item back at the tail. It reuses the slot the
// item already held, so it always succeeds.
func (b *Buffer[T]) Requeue(x T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.inflight--
	b.queue = append(b.queue, x)
}

// Release frees the slot of an in-flight item that finished successfully.
func (b *Buffer[T]) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.inflight--
}

// Queued is the number of items waiting (not in flight).
func (b *Buffer[T]) Queued() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)
}

// InFlight is the number of items currently being checked.
func (b *Buffer[T]) InFlight() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inflight
}

// Occupancy is queued + in flight — what counts against capacity.
func (b *Buffer[T]) Occupancy() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue) + b.inflight
}

// NeedsRefill reports whether occupancy fell below the low-water mark.
func (b *Buffer[T]) NeedsRefill() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.queue)+b.inflight < b.lowWater
}

// Room is how many new items Offer would accept right now.
func (b *Buffer[T]) Room() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.capacity - len(b.queue) - b.inflight
}

// MaxDepth is the highest occupancy ever observed.
func (b *Buffer[T]) MaxDepth() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.maxDepth
}

// Capacity is the configured bound.
func (b *Buffer[T]) Capacity() int { return b.capacity }

func (b *Buffer[T]) track() {
	if d := len(b.queue) + b.inflight; d > b.maxDepth {
		b.maxDepth = d
	}
}
