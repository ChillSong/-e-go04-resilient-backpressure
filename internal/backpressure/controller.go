package backpressure

import "sync"

// Controller adapts how many inventory checks run concurrently — i.e. how hard
// the consumer leans on the external service — using additive-increase /
// multiplicative-decrease (AIMD), the same idea TCP uses for congestion.
//
// Every tick it looks at three signals:
//   - breaker open        -> drop to Min (the service collapsed; restart gently)
//   - error rate > MaxErr -> halve (the service is saturating; back off fast)
//   - backlog > limit     -> add Step (there is work waiting; probe for more)
//
// Growth is deliberately linear, with no TCP-style slow start. Doubling was
// tried and measured worse: this service opens one TCP connection per request
// and has a tiny accept queue, so doubling concurrency launches a burst of
// simultaneous connects, the accept queue overflows, refusals arrive in a
// clustered run, and the breaker trips spuriously. +Step per second keeps each
// burst small. See decisions.md section 3.
type Controller struct {
	mu     sync.Mutex
	min    int
	max    int
	step   int
	maxErr float64
	limit  int
	peak   int
}

// NewController starts at min concurrency.
func NewController(min, max, step int, maxErr float64) *Controller {
	if min < 1 {
		min = 1
	}
	if max < min {
		max = min
	}
	if step < 1 {
		step = 1
	}
	return &Controller{min: min, max: max, step: step, maxErr: maxErr, limit: min, peak: min}
}

// Tick updates the limit from the last interval's signals and returns it.
func (c *Controller) Tick(attempts, failures int64, breakerOpen bool, backlog int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case breakerOpen:
		c.limit = c.min
	case attempts > 0 && float64(failures)/float64(attempts) > c.maxErr:
		c.limit /= 2
		if c.limit < c.min {
			c.limit = c.min
		}
	case backlog > int64(c.limit):
		c.limit += c.step
		if c.limit > c.max {
			c.limit = c.max
		}
	}
	if c.limit > c.peak {
		c.peak = c.limit
	}
	return c.limit
}

// Limit is the current concurrency limit.
func (c *Controller) Limit() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.limit
}

// Peak is the highest limit reached.
func (c *Controller) Peak() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peak
}
