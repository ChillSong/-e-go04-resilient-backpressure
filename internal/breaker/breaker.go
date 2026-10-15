// Package breaker implements a three-state circuit breaker (closed, open,
// half-open) that wraps calls to the external inventory service.
//
//   - Closed: calls flow. FailureThreshold consecutive failures open it.
//   - Open: every call is rejected immediately (ErrOpen) — no request reaches
//     the overloaded service, so it gets room to recover. After the cooldown it
//     moves to half-open.
//   - Half-open: only HalfOpenProbes trial calls are let through. If all of them
//     succeed the breaker closes; if any fails it re-opens with a doubled
//     cooldown (capped at MaxCooldown), so a service that is still sick is
//     probed less and less often.
//
// Calls are tracked with a Ticket stamped with the breaker's generation. A call
// admitted under one state that finishes after the breaker has already moved on
// (e.g. a slow request that completes after the breaker opened) is a stale
// result and does not change the current state.
package breaker

import (
	"errors"
	"sync"
	"time"
)

// State of the breaker.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

func (s State) String() string {
	switch s {
	case Closed:
		return "closed"
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	}
	return "unknown"
}

// ErrOpen is returned by Allow when the breaker rejects a call.
var ErrOpen = errors.New("circuit breaker open")

// Config holds the breaker parameters. See decisions.md for how each was chosen.
type Config struct {
	FailureThreshold int           // consecutive failures that open the breaker
	Cooldown         time.Duration // base time spent open before probing
	MaxCooldown      time.Duration // cap for the exponential cooldown backoff
	HalfOpenProbes   int           // trial calls in half-open; all must succeed to close
}

// Transition describes one state change, for logging and evidence.
type Transition struct {
	From, To State
	At       time.Time
	Reason   string
	Cooldown time.Duration // cooldown armed when To == Open
}

// Stats is a snapshot of the breaker's counters.
type Stats struct {
	State        State
	Opens        int           // times the breaker entered Open
	TimeOpen     time.Duration // total time spent in Open (incl. the current period)
	TimeHalfOpen time.Duration // total time spent in HalfOpen
	Rejected     int64         // calls rejected with ErrOpen
}

// Ticket is issued by Allow for every admitted call and must be finished with
// Done exactly once.
type Ticket struct {
	b     *Breaker
	gen   uint64
	probe bool
}

// Breaker is safe for concurrent use.
type Breaker struct {
	mu  sync.Mutex
	cfg Config
	now func() time.Time

	state        State
	gen          uint64
	enteredAt    time.Time
	consecFails  int
	cooldown     time.Duration
	reopenStreak int

	probesInFlight int
	probeOK        int

	opens        int
	timeOpen     time.Duration
	timeHalfOpen time.Duration
	rejected     int64

	onTransition func(Transition)
}

// New creates a closed breaker. now may be nil (uses time.Now); tests inject a
// fake clock. onTransition may be nil; it is called outside the lock.
func New(cfg Config, now func() time.Time, onTransition func(Transition)) *Breaker {
	if now == nil {
		now = time.Now
	}
	if cfg.FailureThreshold < 1 {
		cfg.FailureThreshold = 1
	}
	if cfg.HalfOpenProbes < 1 {
		cfg.HalfOpenProbes = 1
	}
	if cfg.MaxCooldown < cfg.Cooldown {
		cfg.MaxCooldown = cfg.Cooldown
	}
	return &Breaker{cfg: cfg, now: now, state: Closed, enteredAt: now(), onTransition: onTransition}
}

// Allow asks permission for one call. It returns ErrOpen if the call must not
// be made; otherwise a Ticket whose Done must be called with the outcome.
func (b *Breaker) Allow() (*Ticket, error) {
	b.mu.Lock()
	var fired []Transition
	defer func() {
		b.mu.Unlock()
		b.emit(fired)
	}()

	if b.state == Open {
		if b.now().Sub(b.enteredAt) < b.cooldown {
			b.rejected++
			return nil, ErrOpen
		}
		fired = append(fired, b.transition(HalfOpen, "cooldown elapsed"))
	}

	if b.state == HalfOpen {
		if b.probesInFlight+b.probeOK >= b.cfg.HalfOpenProbes {
			b.rejected++
			return nil, ErrOpen
		}
		b.probesInFlight++
		return &Ticket{b: b, gen: b.gen, probe: true}, nil
	}

	return &Ticket{b: b, gen: b.gen}, nil
}

// Ready reports, without side effects, whether Allow would admit a call right
// now. The dispatcher checks it before taking work off the buffer, so while the
// breaker is open no item is taken, rejected and put back in a busy loop.
func (b *Breaker) Ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		return b.now().Sub(b.enteredAt) >= b.cooldown
	case HalfOpen:
		return b.probesInFlight+b.probeOK < b.cfg.HalfOpenProbes
	}
	return true
}

// Done reports the outcome of the call this ticket admitted.
func (t *Ticket) Done(success bool) {
	b := t.b
	b.mu.Lock()
	var fired []Transition
	defer func() {
		b.mu.Unlock()
		b.emit(fired)
	}()

	if t.gen != b.gen {
		return // stale: the breaker changed state while this call was in flight
	}

	switch b.state {
	case Closed:
		if success {
			b.consecFails = 0
			return
		}
		b.consecFails++
		if b.consecFails >= b.cfg.FailureThreshold {
			b.reopenStreak = 1
			b.cooldown = b.cfg.Cooldown
			fired = append(fired, b.transition(Open, "consecutive failures"))
		}

	case HalfOpen:
		if !t.probe {
			return
		}
		b.probesInFlight--
		if !success {
			b.reopenStreak++
			b.cooldown = b.backoff()
			fired = append(fired, b.transition(Open, "half-open probe failed"))
			return
		}
		b.probeOK++
		if b.probeOK >= b.cfg.HalfOpenProbes {
			b.reopenStreak = 0
			fired = append(fired, b.transition(Closed, "half-open probes succeeded"))
		}
	}
}

// State returns the current state with no side effects. An expired cooldown is
// only acted on by Allow, which is what moves Open to HalfOpen.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Stats returns a snapshot; time in the current state is included.
func (b *Breaker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := Stats{State: b.state, Opens: b.opens, TimeOpen: b.timeOpen, TimeHalfOpen: b.timeHalfOpen, Rejected: b.rejected}
	elapsed := b.now().Sub(b.enteredAt)
	switch b.state {
	case Open:
		s.TimeOpen += elapsed
	case HalfOpen:
		s.TimeHalfOpen += elapsed
	}
	return s
}

// transition must be called with the lock held.
func (b *Breaker) transition(to State, reason string) Transition {
	now := b.now()
	elapsed := now.Sub(b.enteredAt)
	switch b.state {
	case Open:
		b.timeOpen += elapsed
	case HalfOpen:
		b.timeHalfOpen += elapsed
	}
	tr := Transition{From: b.state, To: to, At: now, Reason: reason}
	b.state = to
	b.enteredAt = now
	b.gen++
	b.consecFails = 0
	b.probesInFlight = 0
	b.probeOK = 0
	if to == Open {
		b.opens++
		tr.Cooldown = b.cooldown
	}
	return tr
}

// backoff doubles the base cooldown per consecutive re-open, capped.
func (b *Breaker) backoff() time.Duration {
	d := b.cfg.Cooldown
	for i := 1; i < b.reopenStreak; i++ {
		d *= 2
		if d >= b.cfg.MaxCooldown {
			return b.cfg.MaxCooldown
		}
	}
	return d
}

func (b *Breaker) emit(trs []Transition) {
	if b.onTransition == nil {
		return
	}
	for _, tr := range trs {
		b.onTransition(tr)
	}
}
