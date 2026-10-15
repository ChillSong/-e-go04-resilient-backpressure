// Package aggregate maintains the real-time Hot Sale totals the operations team
// watches: per country, how many orders were placed, how much payment was
// confirmed, and how much inventory was reserved. Every counted event is unique
// — duplicates are filtered by the dedup window before they reach here.
package aggregate

import (
	"sort"

	"e-go04/internal/event"
)

// CountryStats holds one country's running totals.
type CountryStats struct {
	Orders            int64   // order_placed events
	PaymentsConfirmed int64   // payment_confirmed events
	InventoryReserved int64   // inventory_reserved events
	OrderAmountUSD    float64 // sum of amount over order_placed
	PaymentAmountUSD  float64 // sum of amount over payment_confirmed
}

// Aggregate is the full set of per-country stats.
type Aggregate struct {
	byCountry map[string]*CountryStats
	Unique    int64 // total unique events applied
}

// New returns an empty aggregate.
func New() *Aggregate {
	return &Aggregate{byCountry: make(map[string]*CountryStats)}
}

// Apply folds one already-deduplicated event into the totals.
func (a *Aggregate) Apply(e event.Event) {
	s := a.byCountry[e.Country]
	if s == nil {
		s = &CountryStats{}
		a.byCountry[e.Country] = s
	}
	switch e.EventType {
	case event.TypeOrderPlaced:
		s.Orders++
		if e.AmountUSD != nil {
			s.OrderAmountUSD += *e.AmountUSD
		}
	case event.TypePaymentConfirmed:
		s.PaymentsConfirmed++
		if e.AmountUSD != nil {
			s.PaymentAmountUSD += *e.AmountUSD
		}
	case event.TypeInventoryReserved:
		s.InventoryReserved++
	}
	a.Unique++
}

// Country returns one country's stats (nil if none seen).
func (a *Aggregate) Country(c string) *CountryStats { return a.byCountry[c] }

// Countries returns the country codes present, sorted, for deterministic output.
func (a *Aggregate) Countries() []string {
	out := make([]string, 0, len(a.byCountry))
	for c := range a.byCountry {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
