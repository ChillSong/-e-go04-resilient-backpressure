package breaker

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTest(cfg Config) (*Breaker, *fakeClock, *[]Transition) {
	clk := &fakeClock{t: time.Unix(0, 0)}
	var log []Transition
	b := New(cfg, clk.now, func(tr Transition) { log = append(log, tr) })
	return b, clk, &log
}

var cfg = Config{FailureThreshold: 3, Cooldown: 2 * time.Second, MaxCooldown: 8 * time.Second, HalfOpenProbes: 2}

func fail(t *testing.T, b *Breaker) {
	t.Helper()
	tk, err := b.Allow()
	if err != nil {
		t.Fatalf("expected admission, got %v", err)
	}
	tk.Done(false)
}

func succeed(t *testing.T, b *Breaker) {
	t.Helper()
	tk, err := b.Allow()
	if err != nil {
		t.Fatalf("expected admission, got %v", err)
	}
	tk.Done(true)
}

func TestOpensAfterConsecutiveFailures(t *testing.T) {
	b, _, _ := newTest(cfg)
	fail(t, b)
	fail(t, b)
	if b.State() != Closed {
		t.Fatal("2 failures < threshold 3: must stay closed")
	}
	fail(t, b)
	if b.State() != Open {
		t.Fatalf("3 consecutive failures: want open, got %s", b.State())
	}
}

func TestSuccessResetsConsecutiveCount(t *testing.T) {
	b, _, _ := newTest(cfg)
	fail(t, b)
	fail(t, b)
	succeed(t, b) // breaks the streak
	fail(t, b)
	fail(t, b)
	if b.State() != Closed {
		t.Fatal("failures were not consecutive: must stay closed")
	}
}

func TestOpenRejectsUntilCooldown(t *testing.T) {
	b, clk, _ := newTest(cfg)
	fail(t, b)
	fail(t, b)
	fail(t, b)
	clk.advance(1999 * time.Millisecond)
	if _, err := b.Allow(); err != ErrOpen {
		t.Fatalf("inside cooldown: want ErrOpen, got %v", err)
	}
	clk.advance(1 * time.Millisecond)
	tk, err := b.Allow()
	if err != nil {
		t.Fatalf("cooldown elapsed: want a probe admitted, got %v", err)
	}
	if b.State() != HalfOpen {
		t.Fatalf("want half-open, got %s", b.State())
	}
	tk.Done(true)
}

func TestHalfOpenLimitsProbes(t *testing.T) {
	b, clk, _ := newTest(cfg)
	fail(t, b)
	fail(t, b)
	fail(t, b)
	clk.advance(2 * time.Second)
	t1, _ := b.Allow()
	t2, _ := b.Allow()
	if _, err := b.Allow(); err != ErrOpen {
		t.Fatal("only 2 probes allowed in half-open; third must be rejected")
	}
	t1.Done(true)
	t2.Done(true)
}

func TestHalfOpenClosesAfterAllProbesSucceed(t *testing.T) {
	b, clk, log := newTest(cfg)
	fail(t, b)
	fail(t, b)
	fail(t, b)
	clk.advance(2 * time.Second)
	succeed(t, b)
	if b.State() != HalfOpen {
		t.Fatal("1 of 2 probes ok: still half-open")
	}
	succeed(t, b)
	if b.State() != Closed {
		t.Fatalf("2 of 2 probes ok: want closed, got %s", b.State())
	}
	want := []State{Open, HalfOpen, Closed}
	if len(*log) != len(want) {
		t.Fatalf("transitions: got %d want %d", len(*log), len(want))
	}
	for i, tr := range *log {
		if tr.To != want[i] {
			t.Errorf("transition %d: got %s want %s", i, tr.To, want[i])
		}
	}
}

func TestProbeFailureReopensWithDoubledCooldown(t *testing.T) {
	b, clk, log := newTest(cfg)
	fail(t, b)
	fail(t, b)
	fail(t, b) // open, cooldown 2s
	clk.advance(2 * time.Second)
	fail(t, b) // probe fails -> reopen, cooldown 4s
	if b.State() != Open {
		t.Fatalf("probe failed: want open, got %s", b.State())
	}
	last := (*log)[len(*log)-1]
	if last.Cooldown != 4*time.Second {
		t.Fatalf("want doubled cooldown 4s, got %s", last.Cooldown)
	}
	clk.advance(3999 * time.Millisecond)
	if _, err := b.Allow(); err != ErrOpen {
		t.Fatal("still inside the 4s cooldown")
	}
}

func TestCooldownCapped(t *testing.T) {
	b, clk, log := newTest(cfg)
	fail(t, b)
	fail(t, b)
	fail(t, b) // 2s
	for i := 0; i < 5; i++ {
		clk.advance(time.Minute)
		fail(t, b) // 4s, 8s, 8s, 8s, 8s
	}
	if c := (*log)[len(*log)-1].Cooldown; c != 8*time.Second {
		t.Fatalf("cooldown must cap at 8s, got %s", c)
	}
}

func TestCloseResetsBackoff(t *testing.T) {
	b, clk, log := newTest(cfg)
	fail(t, b)
	fail(t, b)
	fail(t, b) // 2s
	clk.advance(2 * time.Second)
	fail(t, b) // 4s
	clk.advance(4 * time.Second)
	succeed(t, b)
	succeed(t, b) // closed
	fail(t, b)
	fail(t, b)
	fail(t, b) // fresh open: back to base 2s
	if c := (*log)[len(*log)-1].Cooldown; c != 2*time.Second {
		t.Fatalf("after closing, cooldown must reset to base 2s, got %s", c)
	}
}

func TestStaleResultIgnored(t *testing.T) {
	b, _, _ := newTest(cfg)
	slow, _ := b.Allow() // admitted while closed
	fail(t, b)
	fail(t, b)
	fail(t, b) // opens; slow call still in flight
	slow.Done(true)
	if b.State() != Open {
		t.Fatal("a result from before the breaker opened must not close it")
	}
}

func TestStatsTrackOpenTime(t *testing.T) {
	b, clk, _ := newTest(cfg)
	fail(t, b)
	fail(t, b)
	fail(t, b)
	clk.advance(2 * time.Second)
	succeed(t, b) // half-open
	clk.advance(500 * time.Millisecond)
	succeed(t, b) // closed
	s := b.Stats()
	if s.Opens != 1 {
		t.Errorf("opens: got %d want 1", s.Opens)
	}
	if s.TimeOpen != 2*time.Second {
		t.Errorf("time open: got %s want 2s", s.TimeOpen)
	}
	if s.TimeHalfOpen != 500*time.Millisecond {
		t.Errorf("time half-open: got %s want 500ms", s.TimeHalfOpen)
	}
}

func TestReadyHasNoSideEffects(t *testing.T) {
	b, clk, _ := newTest(cfg)
	if !b.Ready() {
		t.Fatal("closed breaker is ready")
	}
	fail(t, b)
	fail(t, b)
	fail(t, b)
	if b.Ready() {
		t.Fatal("open inside cooldown: not ready")
	}
	clk.advance(2 * time.Second)
	if !b.Ready() {
		t.Fatal("cooldown elapsed: ready to probe")
	}
	if b.State() != Open {
		t.Fatal("Ready must not transition; still open until Allow")
	}
	b.Allow()
	b.Allow() // both probes admitted
	if b.Ready() {
		t.Fatal("half-open with all probes in flight: not ready")
	}
}
