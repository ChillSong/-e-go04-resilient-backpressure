// Package event defines the Hot Sale event shape and its JSON decoding.
package event

import "encoding/json"

// Event types emitted by the marketplace, chained by order_id.
const (
	TypeOrderPlaced       = "order_placed"
	TypePaymentConfirmed  = "payment_confirmed"
	TypeInventoryReserved = "inventory_reserved"
)

// Event is one line of the JSONL log — one message as it would arrive on a
// stream. AmountUSD is null for inventory_reserved, hence a pointer.
type Event struct {
	EventID   string   `json:"event_id"`
	EventType string   `json:"event_type"`
	OrderID   string   `json:"order_id"`
	Country   string   `json:"country"`
	AmountUSD *float64 `json:"amount_usd"`
	Timestamp string   `json:"timestamp"`
}

// Parse decodes one JSONL line into an Event.
func Parse(line []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(line, &e)
	return e, err
}
