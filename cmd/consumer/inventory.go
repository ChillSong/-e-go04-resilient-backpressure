package main

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"e-go04/internal/backpressure"
	"e-go04/internal/breaker"
	"e-go04/internal/inventory"
)

// invItem is one pending verification. streamID is its entry in the durable
// inventory_checks stream; it is acknowledged there only after success.
type invItem struct {
	streamID, eventID, orderID, country string
	attempts                            int
}

// inventoryPipeline drains inventory_checks through the breaker:
//
//	inventory_checks (Redis, durable) --refill--> Buffer (bounded) --dispatch--> workers --HTTP--> service
//	                     ^                                                          |
//	                     +---------------- XACK only on success -------------------+
//
// Three goroutines: refill pulls from Redis only while the buffer is below its
// low-water mark; dispatch launches workers up to the controller's concurrency
// limit and only while the breaker is ready; tick (1 Hz) feeds the controller
// with the last second's error rate and the backlog, and writes the timeline.
type inventoryPipeline struct {
	ctx    context.Context
	cancel context.CancelFunc
	rdb    *redis.Client
	c      *config
	st     *stats

	buf     *backpressure.Buffer[invItem]
	ctl     *backpressure.Controller
	br      *breaker.Breaker
	client  *inventory.Client
	backlog backpressure.Gauge

	calls, failures                 atomic.Int64 // completed calls, failed ones
	fail503, failTimeout, failOther atomic.Int64
	retries                         atomic.Int64

	concSum, concN int64 // for average concurrency (tick goroutine only)
	t0             time.Time

	wake chan struct{}
	done chan struct{}
	once sync.Once

	trMu        sync.Mutex
	transitions []breaker.Transition
	brCSV       *csv.Writer
	brFile      *os.File
}

func newInventoryPipeline(parent context.Context, rdb *redis.Client, c *config, st *stats) *inventoryPipeline {
	ctx, cancel := context.WithCancel(parent)
	p := &inventoryPipeline{
		ctx: ctx, cancel: cancel, rdb: rdb, c: c, st: st,
		buf:    backpressure.NewBuffer[invItem](c.bufCap, c.bufLow),
		ctl:    backpressure.NewController(c.concMin, c.concMax, c.concStep, c.concMaxErr),
		client: inventory.New(c.invURL, c.httpTimeout),
		wake:   make(chan struct{}, 1),
		done:   make(chan struct{}),
		t0:     time.Now(),
	}
	p.br = breaker.New(breaker.Config{
		FailureThreshold: c.brThreshold,
		Cooldown:         c.brCooldown,
		MaxCooldown:      c.brMaxCooldown,
		HalfOpenProbes:   c.brProbes,
	}, nil, p.onTransition)
	return p
}

func (p *inventoryPipeline) start() {
	if err := p.client.Reset(p.ctx); err != nil {
		log.Fatalf("inventory service unreachable at %s: %v (start it: python3 inventory_service.py)", p.c.invURL, err)
	}
	if p.c.breakerLog != "" {
		f, err := os.Create(p.c.breakerLog)
		if err != nil {
			log.Fatalf("breaker log: %v", err)
		}
		p.brFile = f
		p.brCSV = csv.NewWriter(f)
		p.brCSV.Write([]string{"elapsed_s", "from", "to", "reason", "cooldown_s"})
		p.brCSV.Flush()
	}
	go p.refillLoop()
	go p.dispatchLoop()
	go p.tickLoop()
}

func (p *inventoryPipeline) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

func (p *inventoryPipeline) wait() {
	<-p.done
	p.cancel()
	if p.brFile != nil {
		p.brCSV.Flush()
		p.brFile.Close()
	}
}

func (p *inventoryPipeline) finish() { p.once.Do(func() { close(p.done) }) }

func (p *inventoryPipeline) onTransition(tr breaker.Transition) {
	el := tr.At.Sub(p.t0).Seconds()
	msg := fmt.Sprintf("[breaker] t=%.1fs %s -> %s (%s)", el, tr.From, tr.To, tr.Reason)
	if tr.To == breaker.Open {
		msg += fmt.Sprintf(", cooldown %s", tr.Cooldown)
	}
	log.Print(msg)

	p.trMu.Lock()
	defer p.trMu.Unlock()
	p.transitions = append(p.transitions, tr)
	if p.brCSV != nil {
		p.brCSV.Write([]string{
			strconv.FormatFloat(el, 'f', 3, 64), tr.From.String(), tr.To.String(), tr.Reason,
			strconv.FormatFloat(tr.Cooldown.Seconds(), 'f', 1, 64),
		})
		p.brCSV.Flush()
	}
}

// refillLoop moves pending verifications from Redis into the bounded buffer,
// and only when the buffer has dropped below its low-water mark. While the
// buffer is full, new work simply stays in the inventory_checks stream: that is
// the backpressure decision — delay, never drop.
//
// On start it first re-reads this consumer's own pending entries (ID "0"):
// anything delivered to a previous run but never acknowledged — i.e. never
// verified — is picked up again. That is how a crash loses nothing.
func (p *inventoryPipeline) refillLoop() {
	recovering := true
	cursor := "0"
	var overflow []invItem

	for p.ctx.Err() == nil {
		for len(overflow) > 0 && p.buf.Offer(overflow[0]) {
			overflow = overflow[1:]
		}
		room := p.buf.Room()
		if len(overflow) > 0 || room <= 0 || !p.buf.NeedsRefill() {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		id, block := ">", 200*time.Millisecond
		if recovering {
			id, block = cursor, -1 // pending reads never block; -1 omits BLOCK
		}
		res, err := p.rdb.XReadGroup(p.ctx, &redis.XReadGroupArgs{
			Group: p.c.invGroup, Consumer: p.c.consumer,
			Streams: []string{p.c.invStream, id},
			Count:   int64(room), Block: block,
		}).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if p.ctx.Err() == nil {
				log.Printf("[inv] refill: %v", err)
				time.Sleep(100 * time.Millisecond)
			}
			continue
		}

		var msgs []redis.XMessage
		if len(res) > 0 {
			msgs = res[0].Messages
		}
		if recovering {
			if len(msgs) == 0 {
				recovering = false
				continue
			}
			cursor = msgs[len(msgs)-1].ID
			log.Printf("[inv] recovered %d unverified checks from a previous run", len(msgs))
		}
		for _, m := range msgs {
			it := invItem{
				streamID: m.ID,
				eventID:  str(m.Values["event_id"]),
				orderID:  str(m.Values["order_id"]),
				country:  str(m.Values["country"]),
			}
			if !p.buf.Offer(it) {
				overflow = append(overflow, it)
			}
		}
		p.notify()
	}
}

// dispatchLoop launches one worker per item while in-flight work is under the
// controller's limit and the breaker is ready. While the breaker is open it
// takes nothing: no call reaches the service and items wait in the buffer.
func (p *inventoryPipeline) dispatchLoop() {
	for p.ctx.Err() == nil {
		for p.buf.InFlight() < p.ctl.Limit() && p.br.Ready() {
			it, ok := p.buf.Take()
			if !ok {
				break
			}
			tk, err := p.br.Allow()
			if err != nil { // lost a race with another probe; put it back
				p.buf.Requeue(it)
				break
			}
			go p.work(it, tk)
		}
		select {
		case <-p.wake:
		case <-time.After(20 * time.Millisecond):
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *inventoryPipeline) work(it invItem, tk *breaker.Ticket) {
	avail, err := p.client.Check(p.ctx, it.orderID, it.country)
	tk.Done(err == nil)
	p.calls.Add(1)

	if err == nil {
		p.ack(it.streamID)
		if avail {
			p.st.available.Add(1)
		}
		p.st.verified.Add(1)
		p.buf.Release()
		p.notify()
		return
	}

	p.failures.Add(1)
	var nerr net.Error
	switch {
	case errors.Is(err, inventory.ErrOverloaded):
		p.fail503.Add(1)
	case errors.As(err, &nerr) && nerr.Timeout():
		p.failTimeout.Add(1)
	default:
		p.failOther.Add(1)
	}
	it.attempts++
	p.retries.Add(1)
	p.buf.Requeue(it) // keeps its slot: a retry can never be dropped
	p.notify()
}

// ack confirms a verification in Redis. It retries because an un-acked entry
// would be checked again after a restart (harmless, but it would skew counts).
func (p *inventoryPipeline) ack(id string) {
	for i := 0; i < 5; i++ {
		if err := p.rdb.XAck(context.Background(), p.c.invStream, p.c.invGroup, id).Err(); err == nil {
			return
		}
		time.Sleep(time.Duration(i+1) * 50 * time.Millisecond)
	}
	log.Printf("[inv] WARNING: could not XACK %s after retries", id)
}

// tickLoop runs once a second: it computes the backpressure signals, lets the
// controller adapt concurrency, logs the timeline, and detects completion.
func (p *inventoryPipeline) tickLoop() {
	var tl *csv.Writer
	var tf *os.File
	if p.c.timeline != "" {
		f, err := os.Create(p.c.timeline)
		if err != nil {
			log.Fatalf("timeline: %v", err)
		}
		tf = f
		tl = csv.NewWriter(f)
		tl.Write([]string{"elapsed_s", "main_processed", "main_per_s", "handed_off", "verified", "verified_per_s",
			"backlog", "buffer", "inflight", "conc_limit", "calls_per_s", "failures_per_s", "error_rate", "breaker"})
	}
	defer func() {
		if tf != nil {
			tl.Flush()
			tf.Close()
		}
	}()

	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var prevCalls, prevFail, prevVer, prevProc int64
	sec := 0

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-tick.C:
		}
		sec++
		calls, fails := p.calls.Load(), p.failures.Load()
		ver, proc := p.st.verified.Load(), p.st.processed.Load()
		dCalls, dFail := calls-prevCalls, fails-prevFail
		dVer, dProc := ver-prevVer, proc-prevProc
		prevCalls, prevFail, prevVer, prevProc = calls, fails, ver, proc

		backlog := p.st.handedOff.Load() - ver
		if backlog < 0 {
			backlog = 0 // a check can finish a hair before its hand-off is counted
		}
		p.backlog.Set(backlog)

		state := p.br.State()
		limit := p.ctl.Tick(dCalls, dFail, state == breaker.Open, backlog)
		p.concSum += int64(limit)
		p.concN++

		errRate := 0.0
		if dCalls > 0 {
			errRate = float64(dFail) / float64(dCalls)
		}
		if tl != nil {
			tl.Write([]string{
				strconv.Itoa(sec), strconv.FormatInt(proc, 10), strconv.FormatInt(dProc, 10),
				strconv.FormatInt(p.st.handedOff.Load(), 10), strconv.FormatInt(ver, 10), strconv.FormatInt(dVer, 10),
				strconv.FormatInt(backlog, 10), strconv.Itoa(p.buf.Occupancy()), strconv.Itoa(p.buf.InFlight()),
				strconv.Itoa(limit), strconv.FormatInt(dCalls, 10), strconv.FormatInt(dFail, 10),
				strconv.FormatFloat(errRate, 'f', 3, 64), state.String(),
			})
			tl.Flush()
		}
		if sec%30 == 0 {
			bs := p.br.Stats()
			log.Printf("[inv] t=%ds verified %d/%d backlog=%d buffer=%d conc=%d err=%.0f%% breaker=%s opens=%d",
				sec, ver, p.st.handedOff.Load(), backlog, p.buf.Occupancy(), limit, errRate*100, state, bs.Opens)
		}

		if p.st.mainDone.Load() && ver >= p.st.handedOff.Load() && p.buf.Occupancy() == 0 {
			p.finish()
			return
		}
	}
}

func (p *inventoryPipeline) avgConc() float64 {
	if p.concN == 0 {
		return 0
	}
	return float64(p.concSum) / float64(p.concN)
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
