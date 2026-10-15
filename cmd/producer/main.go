// Command producer publishes the Hot Sale event log to a Redis Stream in file
// order — one stream entry per JSONL line, exactly as a gateway would emit it,
// duplicates included. Deduplication is the consumer's job, not the producer's.
//
// E-Go04 extends the Go03 producer with real pacing: a schedule of phases, each
// with its own rate and duration, so a run can hold a normal rate, jump to a
// 10x spike for a fixed window, and return to normal. Every second it records
// how many messages it actually published to producer_timeline.csv — the
// evidence that the spike really happened at the intended rate.
package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// EOFType is the sentinel event_type the producer appends after the last line so
// the consumer knows the stream is complete and can stop cleanly.
const EOFType = "__eof__"

// phase is one step of the publishing schedule. Dur == 0 means "until EOF".
type phase struct {
	Rate  int
	Dur   time.Duration
	Label string
}

func main() {
	file := flag.String("file", "data/eventos_hotsale.jsonl", "path to the JSONL event log")
	addr := flag.String("addr", "localhost:6381", "Redis address")
	stream := flag.String("stream", "hotsale", "stream key")
	batch := flag.Int("batch", 2000, "XADD pipeline batch size when unthrottled")
	rate := flag.Int("rate", 0, "single constant rate in msg/s (0 = firehose); ignored if --schedule is set")
	schedule := flag.String("schedule", "", `phases "rate:duration,..." e.g. "2500:20s,25000:121s,2500:0" (0 = until EOF)`)
	limit := flag.Int64("limit", 0, "publish at most N lines (0 = whole file); for smoke tests")
	timeline := flag.String("timeline", "producer_timeline.csv", "per-second publish log (empty = off)")
	reset := flag.Bool("reset", true, "delete the stream before publishing")
	flag.Parse()

	phases, err := buildPhases(*schedule, *rate)
	if err != nil {
		log.Fatalf("schedule: %v", err)
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: *addr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("redis ping: %v (is it up? docker compose up -d)", err)
	}

	if *reset {
		if err := rdb.Del(ctx, *stream).Err(); err != nil {
			log.Fatalf("reset stream: %v", err)
		}
	}

	f, err := os.Open(*file)
	if err != nil {
		log.Fatalf("open file: %v", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	var published atomic.Int64
	var phaseIdx atomic.Int32
	t0 := time.Now()

	stopTimeline := startTimeline(*timeline, t0, &published, &phaseIdx, phases)

	pipe := rdb.Pipeline()
	pending := 0
	flush := func() {
		if pending == 0 {
			return
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("xadd flush: %v", err)
		}
		pending = 0
	}

	pi := 0
	ph := phases[pi]
	phStart := time.Now()
	var phPublished int64
	log.Printf("[producer] phase %d %-6s start: %d msg/s for %s", pi+1, ph.Label, ph.Rate, durLabel(ph.Dur))

	endPhase := func() {
		d := time.Since(phStart)
		log.Printf("[producer] phase %d %-6s end:   %d msgs in %s (achieved %.0f msg/s)",
			pi+1, ph.Label, phPublished, d.Round(time.Millisecond), float64(phPublished)/d.Seconds())
	}

	for sc.Scan() {
		if *limit > 0 && published.Load() >= *limit {
			break
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		// Copy: Scanner reuses its buffer; the pipeline holds the value until Exec.
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: *stream, Values: map[string]any{"data": string(line)}})
		pending++
		published.Add(1)
		phPublished++

		if pending < flushSize(ph.Rate, *batch) {
			continue
		}
		flush()

		// Pace inside the phase: sleep until the clock catches up with how long
		// phPublished messages should take at this phase's rate.
		if ph.Rate > 0 {
			target := time.Duration(float64(phPublished) / float64(ph.Rate) * float64(time.Second))
			if ahead := target - time.Since(phStart); ahead > 0 {
				time.Sleep(ahead)
			}
		}

		// Timed phase over? Move to the next one.
		if ph.Dur > 0 && time.Since(phStart) >= ph.Dur && pi+1 < len(phases) {
			endPhase()
			pi++
			ph = phases[pi]
			phaseIdx.Store(int32(pi))
			phStart = time.Now()
			phPublished = 0
			log.Printf("[producer] phase %d %-6s start: %d msg/s for %s", pi+1, ph.Label, ph.Rate, durLabel(ph.Dur))
		}
	}
	if err := sc.Err(); err != nil {
		log.Fatalf("scan: %v", err)
	}
	flush()
	endPhase()
	if ph.Dur > 0 && time.Since(phStart) < ph.Dur {
		log.Printf("[producer] WARNING: input ended before phase %d (%s) reached its %s", pi+1, ph.Label, ph.Dur)
	}

	// Sentinel so the consumer can detect end-of-stream deterministically.
	if err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: *stream,
		Values: map[string]any{"data": `{"event_type":"` + EOFType + `"}`},
	}).Err(); err != nil {
		log.Fatalf("xadd sentinel: %v", err)
	}
	stopTimeline()

	elapsed := time.Since(t0)
	log.Printf("[producer] published %d lines to %q in %s (%.0f msg/s avg)",
		published.Load(), *stream, elapsed.Round(time.Millisecond), float64(published.Load())/elapsed.Seconds())
}

// buildPhases parses --schedule, or falls back to Go03's single --rate.
func buildPhases(spec string, rate int) ([]phase, error) {
	if strings.TrimSpace(spec) == "" {
		return []phase{{Rate: rate, Label: "normal"}}, nil
	}
	var out []phase
	maxRate := 0
	for _, part := range strings.Split(spec, ",") {
		rs, ds, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("phase %q: want rate:duration", part)
		}
		r, err := strconv.Atoi(rs)
		if err != nil || r < 0 {
			return nil, fmt.Errorf("phase %q: bad rate", part)
		}
		var d time.Duration
		if ds != "0" {
			if d, err = time.ParseDuration(ds); err != nil {
				return nil, fmt.Errorf("phase %q: bad duration", part)
			}
		}
		out = append(out, phase{Rate: r, Dur: d})
		if r > maxRate {
			maxRate = r
		}
	}
	for i := range out {
		out[i].Label = "normal"
		if len(out) > 1 && out[i].Rate == maxRate {
			out[i].Label = "spike"
		}
	}
	return out, nil
}

// flushSize sends ~50 pipeline flushes per second when paced, so pacing has
// ~20ms granularity at any rate; unthrottled it uses the fixed batch.
func flushSize(rate, batch int) int {
	if rate <= 0 {
		return batch
	}
	if n := rate / 50; n > 1 {
		return n
	}
	return 1
}

func durLabel(d time.Duration) string {
	if d == 0 {
		return "until EOF"
	}
	return d.String()
}

// startTimeline writes one CSV row per second: elapsed, phase, target rate,
// messages actually published in that second, and the running total.
func startTimeline(path string, t0 time.Time, published *atomic.Int64, phaseIdx *atomic.Int32, phases []phase) func() {
	if path == "" {
		return func() {}
	}
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("timeline: %v", err)
	}
	w := csv.NewWriter(f)
	w.Write([]string{"elapsed_s", "phase", "label", "target_rate", "published_in_second", "published_total"})

	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		var last int64
		sec := 0
		row := func() {
			sec++
			cur := published.Load()
			p := phases[phaseIdx.Load()]
			w.Write([]string{
				strconv.Itoa(sec), strconv.Itoa(int(phaseIdx.Load()) + 1), p.Label,
				strconv.Itoa(p.Rate), strconv.FormatInt(cur-last, 10), strconv.FormatInt(cur, 10),
			})
			w.Flush()
			last = cur
		}
		for {
			select {
			case <-tick.C:
				row()
			case <-done:
				row()
				return
			}
		}
	}()
	return func() {
		close(done)
		<-finished
		f.Close()
	}
}
