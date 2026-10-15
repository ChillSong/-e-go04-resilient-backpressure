// Package inventory is the HTTP client for the external inventory service
// (inventory_service.py). It only reports outcomes; retry and protection
// decisions belong to the breaker and the backpressure controller.
package inventory

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ErrOverloaded is a 503 from the service.
var ErrOverloaded = errors.New("inventory service overloaded (503)")

// Client calls POST /check-inventory.
type Client struct {
	http *http.Client
	base string
}

// New builds a client with a hard per-call timeout. A call that hangs past the
// timeout counts as a failure instead of holding a worker indefinitely.
func New(baseURL string, timeout time.Duration) *Client {
	return &Client{
		base: baseURL,
		http: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				// The Python service speaks HTTP/1.0 and closes every connection;
				// say so explicitly instead of keeping dead idle connections.
				DisableKeepAlives: true,
			},
		},
	}
}

// Check asks whether inventory is still available for an order. It returns
// (available, nil) on 200, ErrOverloaded on 503, and a transport error on
// timeout or connection failure.
func (c *Client) Check(ctx context.Context, orderID, country string) (bool, error) {
	body, _ := json.Marshal(map[string]string{"order_id": orderID, "country": country})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/check-inventory", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	switch resp.StatusCode {
	case http.StatusOK:
		var out struct {
			Available bool `json:"available"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return false, fmt.Errorf("decode 200 body: %w", err)
		}
		return out.Available, nil
	case http.StatusServiceUnavailable:
		return false, ErrOverloaded
	default:
		return false, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
}

// Reset clears the service's counters between test runs (POST /reset).
func (c *Client) Reset(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/reset", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}
