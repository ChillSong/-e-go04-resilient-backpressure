package backpressure

import (
	"sync"
	"testing"
)

func TestOfferRefusedWhenFull(t *testing.T) {
	b := NewBuffer[int](2, 1)
	if !b.Offer(1) || !b.Offer(2) {
		t.Fatal("first two offers must fit")
	}
	if b.Offer(3) {
		t.Fatal("buffer at capacity must refuse new work")
	}
}

func TestInFlightCountsAgainstCapacity(t *testing.T) {
	b := NewBuffer[int](2, 1)
	b.Offer(1)
	b.Offer(2)
	b.Take() // 1 in flight, 1 queued: still full
	if b.Offer(3) {
		t.Fatal("in-flight items hold their slot; buffer is still full")
	}
}

func TestRequeueNeverDrops(t *testing.T) {
	b := NewBuffer[int](2, 1)
	b.Offer(1)
	b.Offer(2)
	x, _ := b.Take()
	b.Requeue(x) // must fit even though the buffer is "full"
	if b.Occupancy() != 2 || b.Queued() != 2 {
		t.Fatalf("requeue lost an item: occupancy=%d queued=%d", b.Occupancy(), b.Queued())
	}
	// FIFO: 2 first, then the retried 1.
	if y, _ := b.Take(); y != 2 {
		t.Fatalf("want 2 next, got %d", y)
	}
	if y, _ := b.Take(); y != 1 {
		t.Fatalf("want retried 1 last, got %d", y)
	}
}

func TestReleaseFreesSlot(t *testing.T) {
	b := NewBuffer[int](1, 1)
	b.Offer(1)
	b.Take()
	b.Release()
	if !b.Offer(2) {
		t.Fatal("a released slot must accept new work")
	}
}

func TestRefillAndMaxDepth(t *testing.T) {
	b := NewBuffer[int](10, 4)
	if !b.NeedsRefill() {
		t.Fatal("empty buffer is below low water")
	}
	for i := 0; i < 7; i++ {
		b.Offer(i)
	}
	if b.NeedsRefill() {
		t.Fatal("7 >= low water 4: no refill needed")
	}
	if b.MaxDepth() != 7 {
		t.Fatalf("max depth: got %d want 7", b.MaxDepth())
	}
	if b.Room() != 3 {
		t.Fatalf("room: got %d want 3", b.Room())
	}
}

func TestBufferConcurrentNoLoss(t *testing.T) {
	b := NewBuffer[int](100, 50)
	for i := 0; i < 100; i++ {
		b.Offer(i)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	done := map[int]int{}
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				x, ok := b.Take()
				if !ok {
					return
				}
				mu.Lock()
				done[x]++
				first := done[x] == 1
				mu.Unlock()
				if first && x%3 == 0 {
					b.Requeue(x) // fail once, retry
					continue
				}
				b.Release()
			}
		}()
	}
	wg.Wait()
	for i := 0; i < 100; i++ {
		want := 1
		if i%3 == 0 {
			want = 2
		}
		if done[i] != want {
			t.Fatalf("item %d processed %d times, want %d", i, done[i], want)
		}
	}
	if b.Occupancy() != 0 {
		t.Fatalf("buffer should be empty, occupancy=%d", b.Occupancy())
	}
}

func TestControllerAIMD(t *testing.T) {
	c := NewController(4, 256, 4, 0.5)
	if got := c.Tick(10, 0, false, 1000); got != 8 {
		t.Fatalf("backlog waiting, no errors: additive +4 -> want 8, got %d", got)
	}
	c.Tick(10, 0, false, 1000) // 12
	c.Tick(10, 0, false, 1000) // 16
	if got := c.Tick(10, 6, false, 1000); got != 8 {
		t.Fatalf("60%% errors > 50%%: halve 16 -> want 8, got %d", got)
	}
	if got := c.Tick(10, 4, false, 1000); got != 12 {
		t.Fatalf("40%% errors <= 50%%: keep growing -> want 12, got %d", got)
	}
	if got := c.Tick(10, 0, true, 1000); got != 4 {
		t.Fatalf("breaker open: drop to min -> want 4, got %d", got)
	}
}

func TestControllerHoldsWithoutBacklogAndCaps(t *testing.T) {
	c := NewController(2, 20, 4, 0.5)
	for i := 0; i < 20; i++ {
		c.Tick(10, 0, false, 1000)
	}
	if c.Limit() != 20 {
		t.Fatalf("must cap at max 20, got %d", c.Limit())
	}
	if got := c.Tick(10, 0, false, 0); got != 20 {
		t.Fatalf("no backlog: hold, want 20, got %d", got)
	}
	if c.Peak() != 20 {
		t.Fatalf("peak: got %d want 20", c.Peak())
	}
}

func TestGauge(t *testing.T) {
	var g Gauge
	g.Set(5)
	g.Set(12)
	g.Set(3)
	if g.Value() != 3 || g.Max() != 12 {
		t.Fatalf("value=%d max=%d, want 3 and 12", g.Value(), g.Max())
	}
}
