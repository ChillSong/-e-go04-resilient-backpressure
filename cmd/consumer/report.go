package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

func report(c *config, st *stats, p *inventoryPipeline, total time.Duration) {
	agg := st.agg
	fmt.Println("\n==================== HOT SALE AGGREGATE ====================")
	fmt.Printf("  %-4s %12s %14s %12s %14s %12s\n",
		"pais", "ordenes", "monto_orden", "pagos", "monto_pago", "inventario")
	fmt.Println("  " + strings.Repeat("-", 74))
	for _, cc := range agg.Countries() {
		s := agg.Country(cc)
		fmt.Printf("  %-4s %12d %14.2f %12d %14.2f %12d\n",
			cc, s.Orders, s.OrderAmountUSD, s.PaymentsConfirmed, s.PaymentAmountUSD, s.InventoryReserved)
	}
	fmt.Println("  " + strings.Repeat("-", 74))
	fmt.Printf("  eventos unicos aplicados: %d\n", agg.Unique)
	fmt.Println("===========================================================")

	fmt.Printf("\n  Mensajes procesados (lineas del stream): %d\n", st.processed.Load())
	fmt.Printf("  Duplicados detectados y descartados:     %d\n", st.duplicates.Load())
	if !st.firstAt.IsZero() && st.lastAt.After(st.firstAt) {
		el := st.lastAt.Sub(st.firstAt)
		fmt.Printf("  Stream principal: %.0f msg/s en %s\n", float64(st.processed.Load())/el.Seconds(), el.Round(time.Millisecond))
	}
	if len(st.latencies) > 0 {
		fmt.Printf("  Latencia e2e del stream principal: p50=%dms p95=%dms p99=%dms max=%dms\n",
			pct(st.latencies, 50), pct(st.latencies, 95), pct(st.latencies, 99), pct(st.latencies, 100))
	}

	bs := p.br.Stats()
	calls := p.calls.Load()
	fmt.Println("\n==================== INVENTARIO / RESILIENCIA ==============")
	fmt.Printf("  Verificaciones entregadas al stream durable: %d\n", st.handedOff.Load())
	fmt.Printf("  Verificaciones confirmadas por el servicio:  %d  (available=true: %d)\n", st.verified.Load(), st.available.Load())
	fmt.Printf("  Llamadas al servicio: %d  (exito %d, fallo %d -> 503=%d timeout=%d otro=%d)\n",
		calls, calls-p.failures.Load(), p.failures.Load(), p.fail503.Load(), p.failTimeout.Load(), p.failOther.Load())
	fmt.Printf("  Reintentos (cada uno volvio al buffer, ninguno descartado): %d\n", p.retries.Load())

	fmt.Println("\n  Circuit breaker")
	fmt.Printf("    veces abierto:          %d\n", bs.Opens)
	fmt.Printf("    tiempo total abierto:   %s\n", bs.TimeOpen.Round(time.Millisecond))
	fmt.Printf("    tiempo en half-open:    %s\n", bs.TimeHalfOpen.Round(time.Millisecond))
	p.trMu.Lock()
	nTr := len(p.transitions)
	closes := 0
	reopens := 0
	for _, tr := range p.transitions {
		if tr.To.String() == "closed" {
			closes++
		}
		if tr.From.String() == "half-open" && tr.To.String() == "open" {
			reopens++
		}
	}
	p.trMu.Unlock()
	fmt.Printf("    transiciones:           %d (half-open->closed=%d, half-open->open=%d)\n", nTr, closes, reopens)
	fmt.Printf("    parametros:             umbral=%d fallos consecutivos, cooldown=%s (max %s), probes=%d\n",
		c.brThreshold, c.brCooldown, c.brMaxCooldown, c.brProbes)

	fmt.Println("\n  Backpressure")
	fmt.Printf("    backlog maximo (verificaciones pendientes): %d\n", p.backlog.Max())
	fmt.Printf("    buffer en memoria: profundidad maxima %d de capacidad %d\n", p.buf.MaxDepth(), p.buf.Capacity())
	fmt.Printf("    concurrencia: pico %d, promedio %.1f (min %d, max %d)\n", p.ctl.Peak(), p.avgConc(), c.concMin, c.concMax)
	fmt.Printf("    pausas del stream principal por backlog: %ds (limite %d)\n", st.mainPauses.Load(), c.maxBacklog)

	fmt.Println("\n==================== CERO PERDIDA ==========================")
	check("eventos unicos", agg.Unique, c.expectUnique)
	check("duplicados descartados", st.duplicates.Load(), c.expectDups)
	check("inventory_reserved entregados", st.handedOff.Load(), c.expectInventory)
	check("inventory_reserved verificados", st.verified.Load(), c.expectInventory)
	pending := st.handedOff.Load() - st.verified.Load()
	fmt.Printf("  %-34s %d\n", "verificaciones sin confirmar:", pending)
	ok := agg.Unique == c.expectUnique && st.duplicates.Load() == c.expectDups &&
		st.verified.Load() == c.expectInventory && pending == 0
	if ok {
		fmt.Println("  RESULTADO: OK — ningun evento perdido, solo retrasado")
	} else {
		fmt.Println("  RESULTADO: DISCREPANCIA — revisar")
	}
	fmt.Println("===========================================================")
	fmt.Printf("  Corrida total: %s\n\n", total.Round(time.Second))
}

func check(label string, got, want int64) {
	mark := "OK"
	if got != want {
		mark = "DIFF"
	}
	fmt.Printf("  %-34s %10d  esperado %10d  [%s]\n", label+":", got, want, mark)
}

// pct returns the p-th percentile (0-100) of ms latencies.
func pct(lat []int64, p int) int64 {
	if len(lat) == 0 {
		return 0
	}
	s := make([]int64, len(lat))
	copy(s, lat)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := (p * len(s)) / 100
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}
