// Command consumer reads the Hot Sale stream through a Redis consumer group,
// deduplicates by event_id, and keeps the per-country aggregate the operations
// team watches (all of that is Go03, unchanged).
//
// E-Go04 adds inventory verification: every unique inventory_reserved event must
// be confirmed against the external inventory service, which degrades under
// load. The main loop does NOT call the service. It hands each inventory event
// off to a dedicated, durable Redis stream (inventory_checks) in the same
// MULTI/EXEC that acknowledges the main batch, and a separate pipeline drains
// that stream through a circuit breaker at an adaptive concurrency. So:
//
//   - aggregation stays real-time even while the service is down;
//   - what gets delayed is only the verification, and it waits in Redis, not in
//     an unbounded in-memory queue;
//   - an inventory check is acknowledged only after the service confirmed it,
//     so nothing is lost even if the consumer dies mid-run.
package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"e-go04/internal/aggregate"
	"e-go04/internal/dedup"
	"e-go04/internal/event"
)

const eofType = "__eof__"

type config struct {
	addr, stream, group, consumer string
	invStream, invGroup           string
	window, count                 int
	reset                         bool

	invURL      string
	httpTimeout time.Duration

	brThreshold   int
	brCooldown    time.Duration
	brMaxCooldown time.Duration
	brProbes      int

	concMin, concMax, concStep int
	concMaxErr                 float64

	bufCap, bufLow int
	maxBacklog     int64

	timeline, breakerLog string

	expectUnique, expectDups, expectInventory int64
}

func main() {
	var c config
	flag.StringVar(&c.addr, "addr", "localhost:6381", "Redis address")
	flag.StringVar(&c.stream, "stream", "hotsale", "main stream key")
	flag.StringVar(&c.group, "group", "g1", "main consumer group")
	flag.StringVar(&c.consumer, "consumer", "c1", "consumer name")
	flag.StringVar(&c.invStream, "inv-stream", "inventory_checks", "durable stream of pending inventory verifications")
	flag.StringVar(&c.invGroup, "inv-group", "inv", "consumer group for inventory verifications")
	flag.IntVar(&c.window, "window", 1_000_000, "dedup window capacity (distinct event_ids remembered)")
	flag.IntVar(&c.count, "count", 1000, "XREADGROUP COUNT per call on the main stream")
	flag.BoolVar(&c.reset, "reset", false, "delete both streams and recreate them empty (fresh run)")

	flag.StringVar(&c.invURL, "inv-url", "http://localhost:8090", "inventory service base URL")
	flag.DurationVar(&c.httpTimeout, "http-timeout", 3*time.Second, "per-call timeout to the inventory service")

	flag.IntVar(&c.brThreshold, "breaker-threshold", 20, "consecutive failures that open the breaker")
	flag.DurationVar(&c.brCooldown, "breaker-cooldown", 2*time.Second, "base time open before half-open")
	flag.DurationVar(&c.brMaxCooldown, "breaker-max-cooldown", 30*time.Second, "cap for exponential cooldown backoff")
	flag.IntVar(&c.brProbes, "breaker-probes", 3, "half-open trial calls; all must succeed to close")

	flag.IntVar(&c.concMin, "conc-min", 4, "minimum concurrent inventory checks")
	flag.IntVar(&c.concMax, "conc-max", 256, "maximum concurrent inventory checks")
	flag.IntVar(&c.concStep, "conc-step", 4, "additive increase per second while backlog waits")
	flag.Float64Var(&c.concMaxErr, "conc-max-err", 0.5, "error rate above which concurrency is halved")

	flag.IntVar(&c.bufCap, "buffer", 2000, "in-memory wait buffer capacity (queued + in flight)")
	flag.IntVar(&c.bufLow, "low-water", 1000, "refill the buffer from Redis below this occupancy")
	flag.Int64Var(&c.maxBacklog, "max-backlog", 2_000_000, "pause the main stream if pending verifications exceed this")

	flag.StringVar(&c.timeline, "timeline", "consumer_timeline.csv", "per-second metrics log")
	flag.StringVar(&c.breakerLog, "breaker-log", "breaker_transitions.csv", "breaker state transitions log")

	flag.Int64Var(&c.expectUnique, "expect-unique", 3_035_188, "unique events reported by the generator")
	flag.Int64Var(&c.expectDups, "expect-dups", 91_562, "duplicates reported by the generator")
	flag.Int64Var(&c.expectInventory, "expect-inventory", 815_590, "inventory_reserved events reported by the generator")
	flag.Parse()

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: c.addr, PoolSize: c.concMax + 32})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v (is it up? docker compose up -d)", err)
	}

	if c.reset {
		rdb.Del(ctx, c.stream, c.invStream)
	}
	for _, g := range [][2]string{{c.stream, c.group}, {c.invStream, c.invGroup}} {
		// MKSTREAM creates the stream if absent; ignore BUSYGROUP if the group exists.
		if err := rdb.XGroupCreateMkStream(ctx, g[0], g[1], "0").Err(); err != nil &&
			!strings.Contains(err.Error(), "BUSYGROUP") {
			log.Fatalf("create group %s/%s: %v", g[0], g[1], err)
		}
	}

	st := &stats{}
	inv := newInventoryPipeline(ctx, rdb, &c, st)
	inv.start()

	t0 := time.Now()
	runMain(ctx, rdb, &c, st, inv)
	log.Printf("[main] stream fully consumed in %s; waiting for inventory verifications to drain (%d pending)",
		time.Since(t0).Round(time.Millisecond), st.handedOff.Load()-st.verified.Load())

	inv.wait()
	report(&c, st, inv, time.Since(t0))
}

// stats are the counters shared between the main loop, the inventory pipeline,
// and the per-second timeline.
type stats struct {
	processed  atomic.Int64 // main-stream messages handled (incl. duplicates)
	duplicates atomic.Int64
	handedOff  atomic.Int64 // unique inventory_reserved moved to inventory_checks
	verified   atomic.Int64 // inventory checks confirmed by the service
	available  atomic.Int64 // ...of which the service said available=true
	mainPauses atomic.Int64 // seconds the main loop paused on backlog
	mainDone   atomic.Bool

	agg       *aggregate.Aggregate
	latencies []int64 // main path end-to-end ms (Go03 metric)
	firstAt   time.Time
	lastAt    time.Time
}

// runMain is the Go03 consumer loop. Two changes: inventory_reserved events are
// handed off atomically to the inventory stream, and the loop pauses if the
// verification backlog ever exceeds --max-backlog (last line of defence for
// Redis memory; see decisions.md).
func runMain(ctx context.Context, rdb *redis.Client, c *config, st *stats, inv *inventoryPipeline) {
	win := dedup.New(c.window)
	st.agg = aggregate.New()
	st.latencies = make([]int64, 0, 3_200_000)

	idleReads := 0
	ackIDs := make([]string, 0, c.count)
	handoff := make([]event.Event, 0, c.count)

	for {
		// Second-level backpressure: stop pulling the main stream while too
		// much verification work is outstanding. The unread messages stay in
		// the main stream; nothing is dropped.
		for st.handedOff.Load()-st.verified.Load() > c.maxBacklog {
			st.mainPauses.Add(1)
			time.Sleep(time.Second)
		}

		res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    c.group,
			Consumer: c.consumer,
			Streams:  []string{c.stream, ">"},
			Count:    int64(c.count),
			Block:    2 * time.Second,
		}).Result()
		if err == redis.Nil {
			idleReads++
			if idleReads >= 30 {
				log.Printf("[main] no data for %ds and no sentinel seen — stopping", 2*idleReads)
				break
			}
			continue
		}
		if err != nil {
			log.Fatalf("xreadgroup: %v", err)
		}
		idleReads = 0

		done := false
		ackIDs = ackIDs[:0]
		handoff = handoff[:0]
		for _, s := range res {
			for _, msg := range s.Messages {
				now := time.Now()
				if st.firstAt.IsZero() {
					st.firstAt = now
				}
				st.lastAt = now
				ackIDs = append(ackIDs, msg.ID)

				raw, _ := msg.Values["data"].(string)
				ev, perr := event.Parse([]byte(raw))
				if perr != nil {
					log.Printf("skip unparseable line: %v", perr)
					continue
				}
				if ev.EventType == eofType {
					done = true
					break
				}

				// End-to-end latency: broker ingest (stream ID ms) -> now.
				if ms, ok := streamIDMillis(msg.ID); ok {
					st.latencies = append(st.latencies, now.UnixMilli()-ms)
				}

				if win.Seen(ev.EventID) {
					st.duplicates.Add(1)
				} else {
					st.agg.Apply(ev)
					if ev.EventType == event.TypeInventoryReserved {
						handoff = append(handoff, ev)
					}
				}
				st.processed.Add(1)
			}
			if done {
				break
			}
		}

		// Hand-off + ack in one MULTI/EXEC: either the inventory events land in
		// the durable inventory stream AND the main batch is acknowledged, or
		// neither happens. A crash can never ack an event whose verification
		// was not yet recorded somewhere durable.
		if len(ackIDs) > 0 {
			_, err := rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
				for _, ev := range handoff {
					p.XAdd(ctx, &redis.XAddArgs{Stream: c.invStream, Values: map[string]any{
						"event_id": ev.EventID, "order_id": ev.OrderID, "country": ev.Country,
					}})
				}
				p.XAck(ctx, c.stream, c.group, ackIDs...)
				return nil
			})
			if err != nil {
				log.Fatalf("hand-off tx: %v", err)
			}
			st.handedOff.Add(int64(len(handoff)))
			inv.notify()
		}
		if done {
			break
		}
	}
	st.mainDone.Store(true)
	inv.notify()
}

// streamIDMillis extracts the millisecond timestamp from a Redis stream ID
// "<ms>-<seq>". This ms is when Redis ingested the XADD — the publication instant.
func streamIDMillis(id string) (int64, bool) {
	dash := strings.IndexByte(id, '-')
	if dash < 0 {
		return 0, false
	}
	var ms int64
	for _, ch := range id[:dash] {
		if ch < '0' || ch > '9' {
			return 0, false
		}
		ms = ms*10 + int64(ch-'0')
	}
	return ms, true
}
