// Package logden is a tiny client for the logden ingest gateway (stdlib only).
//
//	c := logden.New("http://logs.internal:8080", token, "billing-api")
//	c.Error("payment timeout", map[string]any{"order_id": 123})
//
// Optionally with batching:
//
//	c := logden.New(ep, token, "web", logden.WithBatch(500, time.Second),
//		logden.WithOnError(func(err error) { myLogger.Warn(err) }))
//	defer c.Close()
//
// Direct mode reports a failed send through the return value of Log and the
// level helpers. Batch mode cannot: when the call returns, the event is only
// buffered, and the request happens later on the client's own goroutine. So
// batch mode reports through the WithOnError sink instead, and a recording call
// never performs network I/O — a slow gateway must not leak its latency into
// whichever application code path happened to log at the moment the batch
// filled up.
//
// The client does not retry or spool; reliability lives on the gateway leg.
package logden

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

	maxBatch     int
	maxBodyBytes int
	maxBuffer    int
	onError      func(error)

	mu        sync.Mutex
	buf       []Event
	dropped   int // events dropped since the last report, reported by Flush
	batch     int
	interval  time.Duration
	wake      chan struct{} // capacity 1: a full batch nudges the flusher, never blocks
	stop      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
}

// defaultMaxBatch mirrors the gateway's default MAX_BATCH_EVENTS limit and
// defaultMaxBodyBytes its default MAX_BODY_BYTES: the gateway rejects an
// oversized request as a whole (413), so the client splits instead of losing the
// batch. Both are overridable with WithLimits for a retuned gateway.
//
// defaultMaxBuffer caps how many events batch mode may hold while the gateway is
// unreachable. Without a cap an outage turns into unbounded heap growth in the
// application — the client would OOM the process it is only supposed to observe.
// It is deliberately generous: at a few hundred events per second it covers a
// gateway restart, so nothing is dropped in normal operation.
//
// defaultTimeout is the per-request deadline, shared with the Node and Python
// clients rather than tuned per language: a full 1000-event / 4 MiB flush over a
// loaded link needs more than a couple of seconds, and a timeout there costs the
// whole chunk, which Flush has already taken out of the buffer. Override it with
// WithHTTPClient.
const (
	defaultMaxBatch     = 1000
	defaultMaxBodyBytes = 4 << 20
	defaultMaxBuffer    = 10000
	defaultTimeout      = 5 * time.Second
)

// Sentinels for the two losses the client can inflict on its own, so an
// application can tell them apart from a transport or gateway failure (both are
// wrapped with fmt.Errorf, so errors.Is sees them through the detail text).
var (
	// ErrBufferFull means batch mode discarded events because the buffer hit its
	// WithMaxBuffer cap.
	ErrBufferFull = errors.New("logden: batch buffer full")
	// ErrEventTooLarge means a single event exceeded the body limit and could
	// not be split any further.
	ErrEventTooLarge = errors.New("logden: event exceeds the request body limit")
)

// GatewayError is a non-2xx answer from the gateway. Reason carries the
// gateway's own {"error":"<reason>"} body — that is what turns an opaque 400
// into "all_invalid" — and is empty when the answer was not that shape
// (a proxy, a captive portal, a wrong URL).
type GatewayError struct {
	StatusCode int
	Reason     string
}

func (e *GatewayError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("logden: gateway returned %d", e.StatusCode)
	}
	return fmt.Sprintf("logden: gateway returned %d: %s", e.StatusCode, e.Reason)
}

type Option func(*Client)

// WithHTTPClient swaps in a custom HTTP client (timeouts, transport).
func WithHTTPClient(h *http.Client) Option { return func(c *Client) { c.http = h } }

// WithBatch enables async batching: events accumulate until size is reached or
// are flushed every interval. Requires Close() on shutdown.
func WithBatch(size int, interval time.Duration) Option {
	return func(c *Client) { c.batch = size; c.interval = interval }
}

// WithLimits overrides the gateway caps the client splits its requests against,
// for an operator who retuned MAX_BATCH_EVENTS / MAX_BODY_BYTES. Keep both at or
// below the gateway's values: too high and every request comes back 413, which
// costs the whole request, not one event. A non-positive value keeps the default.
func WithLimits(maxBatchEvents, maxBodyBytes int) Option {
	return func(c *Client) {
		if maxBatchEvents > 0 {
			c.maxBatch = maxBatchEvents
		}
		if maxBodyBytes > 0 {
			c.maxBodyBytes = maxBodyBytes
		}
	}
}

// WithMaxBuffer caps how many events batch mode holds. On overflow the OLDEST
// events are dropped: the newest ones describe the state the application is in
// right now — usually the incident that made the gateway unreachable in the
// first place — and dropping them instead would hide an ongoing problem behind
// the head of a burst that is already history. The drop is counted and reported
// through the WithOnError sink; the caller is never blocked to apply
// backpressure, because that would put the outage back on the recording path.
// A non-positive value keeps the default.
func WithMaxBuffer(events int) Option {
	return func(c *Client) {
		if events > 0 {
			c.maxBuffer = events
		}
	}
}

// WithOnError installs the sink for failures batch mode has no caller to return
// to: a failed background send, a dropped event, an oversized one. Without it a
// bad token or an unreachable gateway would discard every log forever with no
// signal at all. The default writes one line per failure through the standard
// log package; pass func(error) {} for silence. A nil argument keeps the
// default, and a panicking sink is contained (see report) rather than allowed to
// kill the application from the client's own goroutine.
func WithOnError(fn func(error)) Option {
	return func(c *Client) {
		if fn != nil {
			c.onError = fn
		}
	}
}

func New(endpoint, token, project string, opts ...Option) *Client {
	c := &Client{
		endpoint:     strings.TrimRight(endpoint, "/"),
		token:        token,
		project:      project,
		http:         &http.Client{Timeout: defaultTimeout},
		maxBatch:     defaultMaxBatch,
		maxBodyBytes: defaultMaxBodyBytes,
		maxBuffer:    defaultMaxBuffer,
		onError:      defaultOnError,
	}
	for _, o := range opts {
		o(c)
	}
	// Clamped after the options so their order cannot change the outcome.
	if c.batch > c.maxBatch {
		c.batch = c.maxBatch
	}
	// A flush trigger above the buffer cap can never fire: the buffer would drop
	// its oldest event for every new one and wait for the ticker instead, so the
	// cap silently becomes the batch size. Make that explicit.
	if c.batch > c.maxBuffer {
		c.batch = c.maxBuffer
	}
	if c.batch > 0 {
		if c.interval <= 0 {
			c.interval = time.Second
		}
		c.stop = make(chan struct{})
		c.wake = make(chan struct{}, 1)
		c.wg.Add(1)
		go c.loop()
	}
	return c
}

// Log records an event: it buffers in batch mode, otherwise sends immediately.
// In batch mode it always returns nil — the send has not happened yet — and any
// later failure reaches the application through the WithOnError sink.
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
		if over := len(c.buf) - c.maxBuffer; over > 0 {
			// Zero the vacated slots before re-slicing past them: the backing
			// array outlives the reslice, so the dropped events' contexts would
			// otherwise stay reachable until the next append reallocates.
			clear(c.buf[:over])
			c.buf = c.buf[over:]
			c.dropped += over
		}
		full := len(c.buf) >= c.batch
		c.mu.Unlock()
		if full {
			// Hand the send to the background flusher instead of running it
			// here: doing the HTTP request on the caller's goroutine injected
			// the gateway's latency into whichever code path happened to fill
			// the batch.
			select {
			case c.wake <- struct{}{}:
			default: // a wake is already pending; the flusher will see the buffer
			}
		}
		return nil
	}
	return c.send([]Event{e})
}

func (c *Client) Debug(msg string, f map[string]any) error    { return c.Log("debug", msg, f) }
func (c *Client) Info(msg string, f map[string]any) error     { return c.Log("info", msg, f) }
func (c *Client) Notice(msg string, f map[string]any) error   { return c.Log("notice", msg, f) }
func (c *Client) Warning(msg string, f map[string]any) error  { return c.Log("warning", msg, f) }
func (c *Client) Error(msg string, f map[string]any) error    { return c.Log("error", msg, f) }
func (c *Client) Critical(msg string, f map[string]any) error { return c.Log("critical", msg, f) }

// Warn is the original name for Warning, kept as an alias so existing callers
// keep compiling; "warning" is the canonical PSR-3 level all clients expose.
func (c *Client) Warn(msg string, f map[string]any) error { return c.Warning(msg, f) }

// Flush sends what is buffered right now, in wire batches of at most maxBatch
// events. The chunking matters: concurrent Log() calls can push the buffer past
// the gateway's MAX_BATCH_EVENTS between two flushes, and an oversized batch is
// rejected as a whole — every event in it would be lost.
//
// It is synchronous on purpose: an explicit Flush (or Close) is the caller
// asking to wait for delivery.
func (c *Client) Flush() error {
	c.reportDrops()

	c.mu.Lock()
	pending := len(c.buf)
	c.mu.Unlock()

	var errs []error
	for sent := 0; sent < pending; {
		c.mu.Lock()
		n := min(len(c.buf), c.maxBatch)
		if n == 0 {
			c.mu.Unlock()
			break
		}
		chunk := c.buf[:n:n] // capped: later appends can't reach into this chunk
		c.buf = c.buf[n:]
		c.mu.Unlock()
		sent += n
		// Joined, not first-wins: chunks fail for independent reasons (one
		// oversized event, one 400 on a bad project) and the caller can only see
		// what the error value carries.
		errs = append(errs, c.send(chunk))
	}
	return errors.Join(errs...)
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
			c.flushReporting()
		case <-c.wake:
			c.flushReporting()
		}
	}
}

// flushReporting is the flusher's Flush: there is no caller left to hand the
// error to, so it goes to the sink.
func (c *Client) flushReporting() {
	if err := c.Flush(); err != nil {
		c.report(err)
	}
}

// reportDrops surfaces buffer overflow once per flush rather than once per
// dropped event: a sustained overflow would otherwise call the sink on every
// Log, which is both a flood and network-adjacent work back on the caller's
// goroutine. The sink is invoked outside the lock — it is application code and
// may well log again.
func (c *Client) reportDrops() {
	c.mu.Lock()
	n := c.dropped
	c.dropped = 0
	c.mu.Unlock()
	if n > 0 {
		c.report(fmt.Errorf("%w: dropped %d oldest event(s) (max_buffer=%d)", ErrBufferFull, n, c.maxBuffer))
	}
}

func (c *Client) report(err error) {
	if err == nil {
		return
	}
	// The sink usually runs on the client's background goroutine, where a panic
	// has no caller frame to recover it and would take the whole process down.
	// A logging client must never be the thing that kills the application.
	defer func() { _ = recover() }()
	c.onError(err)
}

func defaultOnError(err error) { log.Print(err) }

func (c *Client) send(events []Event) error {
	if len(events) == 0 {
		return nil
	}
	body, err := json.Marshal(events)
	if err != nil {
		// Encoding is all-or-nothing, so one context the encoder refuses (a NaN,
		// a chan, a func) would take every sibling with it — and Flush has
		// already removed them from the buffer, so nothing can resend them.
		// Halve until the offender stands alone.
		if len(events) > 1 {
			return c.split(events)
		}
		// Kept separate from the byte split below on purpose: a lone
		// unencodable event is still worth sending with its context replaced,
		// while a lone oversized one cannot be rescued at all.
		return c.sendWithoutContext(events[0], err)
	}
	// Bound the request by bytes too: a few hundred events with large contexts
	// stay under MAX_BATCH_EVENTS but exceed MAX_BODY_BYTES, and the gateway
	// answers 413 for the whole batch.
	if len(body) > c.maxBodyBytes {
		if len(events) > 1 {
			return c.split(events)
		}
		return c.oversized(events[0], len(body))
	}
	return c.post(body)
}

// split halves an oversized batch. Both halves are always attempted: returning
// on the first error would silently drop the sibling half (and everything above
// it in the recursion), which the caller has already removed from the buffer.
func (c *Client) split(events []Event) error {
	half := len(events) / 2
	return errors.Join(c.send(events[:half]), c.send(events[half:]))
}

// oversized is where the byte split bottoms out: a single event that does not
// fit the body limit on its own. Posting it anyway buys nothing — the gateway
// rejects the whole request with 413 — and returning quietly is worse: the event
// has already left the buffer, so it would vanish without a trace. Report it
// with what is needed to find the call site that produced it — but never the
// payload, which is both the reason the event is oversized and the last thing
// that belongs in an error line.
func (c *Client) oversized(e Event, bodyBytes int) error {
	return fmt.Errorf("%w: dropped 1 event (project=%q level=%q timestamp=%q message=%dB request=%dB limit=%dB)",
		ErrEventTooLarge, e.Project, e.Level, e.Timestamp, len(e.Message), bodyBytes, c.maxBodyBytes)
}

// sendWithoutContext resends a single event whose context could not be encoded,
// with the context replaced by a marker: message, level and timestamp are still
// worth keeping, and only the context is lost. The key is deliberately not the
// gateway's own _invalid_json/_truncated, so the two causes stay apart in
// ClickHouse. cause is always returned — the dropped context is a loss, and the
// caller learns about it exactly like a failed send.
func (c *Client) sendWithoutContext(e Event, cause error) error {
	// e is a copy, so the caller's event and its map are left untouched.
	e.Context = map[string]any{"_unserializable": true}
	body, err := json.Marshal([]Event{e})
	if err != nil {
		return cause // nothing encodable left; the event is gone
	}
	if len(body) > c.maxBodyBytes {
		// Losing the context did not make it fit; the message alone is over the
		// limit and there is nothing left to split.
		return errors.Join(cause, c.oversized(e, len(body)))
	}
	if err := c.post(body); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (c *Client) post(body []byte) error {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, c.endpoint+"/logs", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		// Wrapped so the sink's one line names the client: a bare *url.Error in
		// an application log says nothing about who tried to talk to whom.
		return fmt.Errorf("logden: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return &GatewayError{StatusCode: resp.StatusCode, Reason: readReason(resp.Body)}
	}
	// Drain the (empty, 204) body so the connection returns to the pool instead
	// of being closed after every batch.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
	return nil
}

// maxErrorBody is all that is read from a failed response: the gateway answers
// {"error":"<reason>"} in a few dozen bytes, and an interposed proxy's HTML
// error page is not worth pulling into the application's memory.
const maxErrorBody = 4 << 10

// readReason extracts the gateway's machine-readable reason. Anything else —
// plain text from a proxy, an HTML page, an empty body — yields "" and the
// caller falls back to the status code alone.
func readReason(r io.Reader) string {
	data, err := io.ReadAll(io.LimitReader(r, maxErrorBody))
	if err != nil || len(data) == 0 {
		return ""
	}
	var payload struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(data, &payload) != nil {
		return ""
	}
	return sanitizeReason(payload.Error)
}

// sanitizeReason bounds what a remote endpoint can put into the application's
// logs. The gateway's own reasons are short lowercase literals, but the client
// cannot be sure it is talking to the gateway, and a hostile or merely broken
// endpoint should not be able to inject newlines (a forged log record) or
// terminal escapes into the line the sink prints.
func sanitizeReason(s string) string {
	const maxRunes = 120
	var b strings.Builder
	n := 0
	for _, r := range s {
		if n == maxRunes {
			break
		}
		if r < 0x20 || r == 0x7f {
			continue
		}
		b.WriteRune(r)
		n++
	}
	return b.String()
}
