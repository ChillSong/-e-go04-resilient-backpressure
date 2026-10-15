package backpressure

import "sync/atomic"

// Gauge records a level (e.g. pending verifications) and remembers its peak.
type Gauge struct {
	cur atomic.Int64
	max atomic.Int64
}

// Set stores v and raises the peak if needed.
func (g *Gauge) Set(v int64) {
	g.cur.Store(v)
	for {
		m := g.max.Load()
		if v <= m || g.max.CompareAndSwap(m, v) {
			return
		}
	}
}

// Value is the last level set.
func (g *Gauge) Value() int64 { return g.cur.Load() }

// Max is the highest level ever set.
func (g *Gauge) Max() int64 { return g.max.Load() }
