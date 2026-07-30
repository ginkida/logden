// Package logden is a tiny client for the logden ingest gateway (stdlib only).
//
//	c := logden.New("http://logs.internal:8080", token, "billing-api")
//	c.Error("payment timeout", map[string]any{"order_id": 123})
//
// Optionally with batching:
//
//	c := logden.New(ep, token, "web", logden.WithBatch(500, time.Second))
//	defer c.Close()
package logden

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Event is a single event in the /logs contract.
type Event struct {
	Project string         `json:"project"`
	Level   string         `json:"level,omitempty"`
	Message string         `json:"message"`
	Context map[string]any `json:"context,omitempty"`
	// Timestamp is RFC3339; the client stamps it when the event is recorded, so
	// batching delay and gateway queueing don't move the event in time.
	Timestamp string `json:"timestamp,omitempty"`
}

// Client sends events to the gateway. Safe for concurrent use.
type Client struct {
	endpoint string
	token    string
	project  string
	http     *http.Client

	mu        sync.Mutex
	buf       []Event
	batch     int
	interval  time.Duration
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// maxBatch mirrors the gateway's default MAX_BATCH_EVENTS limit and maxBodyBytes
// its default MAX_BODY_BYTES: the gateway rejects an oversized request as a
// whole (413), so the client splits instead of losing the batch.
const (
	maxBatch     = 1000
	maxBodyBytes = 4 << 20
)

type Option func(*Client)

// WithHTTPClient swaps in a custom HTTP client (timeouts, transport).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithBatch enables async batching: events accumulate until size is reached or
// are flushed every interval. Requires Close() on shutdown.
func WithBatch(size int, interval time.Duration) Option {
	return func(c *Client) { c.batch = size; c.interval = interval }
}

func New(endpoint, token, project string, opts ...Option) *Client {
	c := &Client{
		endpoint: strings.TrimRight(endpoint, "/"),
		token:    token,
		project:  project,
		http:     &http.Client{Timeout: 5 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	if c.batch > maxBatch {
		c.batch = maxBatch
	}
	if c.batch > 0 {
		if c.interval <= 0 {
			c.interval = time.Second
		}
		c.stop = make(chan struct{})
		c.wg.Add(1)
		go c.loop()
	}
	return c
}

// Log records an event: it buffers in batch mode, otherwise sends immediately.
func (c *Client) Log(level, message string, fields map[string]any) error {
	e := Event{
		Project:   c.project,
		Level:     level,
		Message:   message,
		Context:   fields,
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
	}
	if c.batch > 0 {
		c.mu.Lock()
		c.buf = append(c.buf, e)
		full := len(c.buf) >= c.batch
		c.mu.Unlock()
		if full {
			return c.Flush()
		}
		return nil
	}
	return c.send([]Event{e})
}

func (c *Client) Info(msg string, f map[string]any) error  { return c.Log("info", msg, f) }
func (c *Client) Warn(msg string, f map[string]any) error  { return c.Log("warning", msg, f) }
func (c *Client) Error(msg string, f map[string]any) error { return c.Log("error", msg, f) }

// Flush sends what is buffered right now, in wire batches of at most maxBatch
// events. The chunking matters: concurrent Log() calls can push the buffer past
// the gateway's MAX_BATCH_EVENTS between two flushes, and an oversized batch is
// rejected as a whole — every event in it would be lost.
func (c *Client) Flush() error {
	c.mu.Lock()
	pending := len(c.buf)
	c.mu.Unlock()

	var firstErr error
	for sent := 0; sent < pending; sent += maxBatch {
		c.mu.Lock()
		n := min(len(c.buf), maxBatch)
		if n == 0 {
			c.mu.Unlock()
			break
		}
		chunk := c.buf[:n:n] // capped: later appends can't reach into this chunk
		c.buf = c.buf[n:]
		c.mu.Unlock()
		if err := c.send(chunk); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close stops the background flusher and sends what's left (batch mode).
func (c *Client) Close() error {
	if c.batch > 0 {
		c.closeOnce.Do(func() { // idempotent: a second Close won't panic
			close(c.stop)
			c.wg.Wait()
		})
	}
	return c.Flush()
}

func (c *Client) loop() {
	defer c.wg.Done()
	t := time.NewTicker(c.interval)
	defer t.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-t.C:
			_ = c.Flush()
		}
	}
}

func (c *Client) send(events []Event) error {
	body, err := json.Marshal(events)
	if err != nil {
		return err
	}
	// Bound the request by bytes too: a few hundred events with large contexts
	// stay under MAX_BATCH_EVENTS but exceed MAX_BODY_BYTES, and the gateway
	// answers 413 for the whole batch.
	if len(body) > maxBodyBytes && len(events) > 1 {
		// Both halves are always attempted: returning on the first error would
		// silently drop the sibling half (and everything above it in the
		// recursion), which the caller has already removed from the buffer.
		half := len(events) / 2
		firstErr := c.send(events[:half])
		if err := c.send(events[half:]); firstErr == nil {
			firstErr = err
		}
		return firstErr
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, c.endpoint+"/logs", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("logden: gateway returned %d", resp.StatusCode)
	}
	return nil
}
