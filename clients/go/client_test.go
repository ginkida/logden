package logden

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitFor polls a condition the background flusher satisfies. Batch mode is
// asynchronous by contract, so a bare assertion right after Log would be a race
// against the flusher rather than a test of it.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// errSink collects what the client reports through WithOnError. The sink runs on
// the flusher's goroutine, so it needs its own lock.
type errSink struct {
	mu   sync.Mutex
	errs []error
}

func (s *errSink) add(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
}

func (s *errSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.errs)
}

func (s *errSink) all() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]error(nil), s.errs...)
}

func TestClientSendAndBatch(t *testing.T) {
	var total atomic.Int64
	var sawAuth atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer tok" {
			sawAuth.Store(true)
		}
		b, _ := io.ReadAll(r.Body)
		var arr []map[string]any
		_ = json.Unmarshal(b, &arr)
		total.Add(int64(len(arr)))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// no batching — sent immediately
	c := New(srv.URL, "tok", "proj")
	if err := c.Error("boom", map[string]any{"x": 1}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if total.Load() != 1 {
		t.Fatalf("non-batch: want 1 got %d", total.Load())
	}
	if !sawAuth.Load() {
		t.Fatal("Authorization header not propagated")
	}

	// batching by size
	bc := New(srv.URL, "tok", "proj", WithBatch(3, time.Hour))
	_ = bc.Info("a", nil)
	_ = bc.Info("b", nil)
	if total.Load() != 1 {
		t.Fatalf("should not flush before reaching batch size, got %d", total.Load())
	}
	_ = bc.Info("c", nil) // reached 3 -> the background flusher is woken
	waitFor(t, "the batch-full flush", func() bool { return total.Load() == 4 })
	_ = bc.Warn("d", nil)
	if err := bc.Close(); err != nil { // send the remainder
		t.Fatalf("close: %v", err)
	}
	if total.Load() != 5 {
		t.Fatalf("close flush: want 5 got %d", total.Load())
	}
	if err := bc.Close(); err != nil { // a second Close must be safe
		t.Fatalf("second close: %v", err)
	}
}

// Every event must carry the time it was recorded, not the time the gateway
// happens to insert it (which after a ClickHouse outage can be hours later).
func TestClientStampsTimestamp(t *testing.T) {
	got := make(chan []Event, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []Event
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &arr)
		got <- arr
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	if err := New(srv.URL, "tok", "proj").Info("hello", nil); err != nil {
		t.Fatalf("send: %v", err)
	}
	events := <-got
	if len(events) != 1 {
		t.Fatalf("got %d events", len(events))
	}
	ts, err := time.Parse(time.RFC3339Nano, events[0].Timestamp)
	if err != nil {
		t.Fatalf("timestamp %q is not RFC3339: %v", events[0].Timestamp, err)
	}
	if d := time.Since(ts); d < -time.Minute || d > time.Minute {
		t.Fatalf("timestamp %q is %s away from now", events[0].Timestamp, d)
	}
}

// Concurrent Log() calls can push the buffer past MAX_BATCH_EVENTS between two
// flushes. Each request must still stay within the cap, or the gateway 413s the
// batch and every event in it is lost.
func TestFlushChunksOversizedBuffer(t *testing.T) {
	var requests, events atomic.Int64
	var oversized atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []Event
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &arr)
		requests.Add(1)
		events.Add(int64(len(arr)))
		if len(arr) > defaultMaxBatch {
			oversized.Add(1)
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// Batching is on but the flush trigger is far away, so the buffer grows past
	// the wire cap before Flush runs.
	c := New(srv.URL, "tok", "proj", WithBatch(defaultMaxBatch, time.Hour))
	const n = defaultMaxBatch*2 + 7
	for i := 0; i < n; i++ {
		c.mu.Lock()
		c.buf = append(c.buf, Event{Project: "proj", Message: "m"})
		c.mu.Unlock()
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if oversized.Load() != 0 {
		t.Fatalf("%d requests exceeded MAX_BATCH_EVENTS", oversized.Load())
	}
	if got := events.Load(); got != n {
		t.Fatalf("delivered %d events, want %d", got, n)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("requests = %d, want 3 (1000+1000+7)", got)
	}
}

// A batch that is small in events but large in bytes must be split, not lost.
func TestSendSplitsOversizedBody(t *testing.T) {
	var requests atomic.Int64
	var maxBody atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		requests.Add(1)
		if int64(len(b)) > maxBody.Load() {
			maxBody.Store(int64(len(b)))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	big := make([]byte, 128<<10)
	for i := range big {
		big[i] = 'x'
	}
	events := make([]Event, 64) // 64 x 128KiB = 8MiB, over MAX_BODY_BYTES
	for i := range events {
		events[i] = Event{Project: "proj", Message: string(big)}
	}

	c := New(srv.URL, "tok", "proj")
	if err := c.send(events); err != nil {
		t.Fatalf("send: %v", err)
	}
	if requests.Load() < 2 {
		t.Fatalf("oversized body was not split: %d request(s)", requests.Load())
	}
	if got := maxBody.Load(); got > defaultMaxBodyBytes {
		t.Fatalf("a request of %d bytes exceeds MAX_BODY_BYTES", got)
	}
}

// A failing sub-request must not cancel its siblings: those events are already
// out of the buffer, so giving up on them loses them for good.
func TestSendSplitDeliversSiblingsAfterFailure(t *testing.T) {
	var delivered atomic.Int64
	var failed atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []Event
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &arr)
		// Reject the first half, accept everything else.
		if len(arr) > 0 && strings.HasPrefix(arr[0].Message, "first") {
			failed.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		delivered.Add(int64(len(arr)))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	big := strings.Repeat("x", 128<<10)
	events := make([]Event, 64) // 8 MiB total => split
	for i := range events {
		half := "first"
		if i >= len(events)/2 {
			half = "second"
		}
		events[i] = Event{Project: "proj", Message: half + big}
	}

	c := New(srv.URL, "tok", "proj")
	err := c.send(events)
	if err == nil {
		t.Fatal("the failing half must be reported")
	}
	if failed.Load() == 0 {
		t.Fatal("the stub never saw the failing half")
	}
	if got := delivered.Load(); got != int64(len(events)/2) {
		t.Fatalf("delivered %d events from the healthy half, want %d", got, len(events)/2)
	}
}

// One context the encoder refuses must cost only its own event: the chunk is
// already out of the buffer, so failing the whole encode loses up to maxBatch
// events that were perfectly valid.
func TestSendKeepsSiblingsWhenOneContextIsUnserializable(t *testing.T) {
	var good, marked atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []Event
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &arr); err != nil {
			t.Errorf("gateway received invalid JSON: %v", err)
		}
		for _, e := range arr {
			if e.Message == "poison" {
				if e.Context["_unserializable"] != true {
					t.Errorf("poisoned event arrived with context %v", e.Context)
				}
				marked.Add(1)
				continue
			}
			good.Add(1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	events := make([]Event, 11)
	for i := range events {
		events[i] = Event{Project: "proj", Message: "ok"}
	}
	// math.NaN() has no JSON representation, so encoding the array fails.
	events[7] = Event{Project: "proj", Message: "poison", Context: map[string]any{"ratio": math.NaN()}}

	c := New(srv.URL, "tok", "proj")
	if err := c.send(events); err == nil {
		t.Fatal("the dropped context must be reported to the caller")
	}
	if got := good.Load(); got != 10 {
		t.Fatalf("delivered %d valid events, want 10", got)
	}
	if got := marked.Load(); got != 1 {
		t.Fatalf("the offending event arrived %d times, want 1 (message kept, context replaced)", got)
	}
	if events[7].Context["ratio"] == nil {
		t.Fatal("the caller's own event was mutated by the fallback")
	}
}

// The package documents the client as safe for concurrent use: Log, the ticker
// and an explicit Flush all touch buf, and a lost or duplicated event here is
// invisible in production. Meaningful only under -race.
func TestConcurrentLogAndFlush(t *testing.T) {
	var delivered atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []Event
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &arr)
		delivered.Add(int64(len(arr)))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// A small batch size and a fast ticker keep all three writers to buf active
	// at once.
	c := New(srv.URL, "tok", "proj", WithBatch(16, time.Millisecond))
	const writers, perWriter = 8, 200

	stopFlusher := make(chan struct{})
	var flusher sync.WaitGroup
	flusher.Add(1)
	go func() {
		defer flusher.Done()
		for {
			select {
			case <-stopFlusher:
				return
			default:
				if err := c.Flush(); err != nil {
					t.Errorf("concurrent flush: %v", err)
				}
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := c.Info("m", map[string]any{"writer": w, "i": i}); err != nil {
					t.Errorf("concurrent log: %v", err)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stopFlusher)
	flusher.Wait()

	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := delivered.Load(); got != writers*perWriter {
		t.Fatalf("delivered %d events, want %d (events were lost or duplicated)", got, writers*perWriter)
	}
}

// In batch mode the event sits in buf until the ticker or Close fires, so a
// timestamp taken at flush time would drift by the whole batching delay.
func TestTimestampStampedAtLogNotFlush(t *testing.T) {
	got := make(chan []Event, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []Event
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &arr)
		got <- arr
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// Nothing flushes on its own: the batch is far from full and the ticker is
	// an hour away, so only Close sends.
	c := New(srv.URL, "tok", "proj", WithBatch(100, time.Hour))
	before := time.Now().UTC()
	if err := c.Info("hello", nil); err != nil {
		t.Fatalf("log: %v", err)
	}
	logged := time.Now().UTC()
	time.Sleep(50 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	events := <-got
	if len(events) != 1 {
		t.Fatalf("got %d events", len(events))
	}
	ts, err := time.Parse(time.RFC3339Nano, events[0].Timestamp)
	if err != nil {
		t.Fatalf("timestamp %q is not RFC3339: %v", events[0].Timestamp, err)
	}
	if ts.Before(before) {
		t.Fatalf("timestamp %s predates the Log call at %s", ts, before)
	}
	if ts.After(logged) {
		t.Fatalf("timestamp %s is past the return of Log at %s: it was stamped at flush time", ts, logged)
	}
}

// Filling the batch must not turn the recording call into an HTTP request: a
// slow gateway would then stall whichever application path happened to log the
// event that completed the batch.
func TestBatchFullDoesNotBlockCaller(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }

	var inFlight, delivered atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []Event
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &arr)
		inFlight.Add(1)
		<-release // the gateway is slow; hold the request open
		delivered.Add(int64(len(arr)))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close() // registered first so it runs last: Close waits for the handler
	defer releaseAll()

	c := New(srv.URL, "tok", "proj", WithBatch(2, time.Hour), WithOnError(func(error) {}))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Info("a", nil)
		_ = c.Info("b", nil) // completes the batch
		_ = c.Info("c", nil)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		releaseAll() // unblock the handler so the test can shut down
		t.Fatal("Log blocked on the background send")
	}
	// The send really is in flight elsewhere, i.e. the calls above returned
	// while the request they triggered was still open.
	waitFor(t, "the background send to reach the stub", func() bool { return inFlight.Load() >= 1 })

	releaseAll()
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := delivered.Load(); got != 3 {
		t.Fatalf("delivered %d events, want 3", got)
	}
}

// A gateway that is down must cost a bounded amount of the application's heap,
// and the loss must be visible instead of silent.
func TestBufferDropsOldestAndReportsThroughSink(t *testing.T) {
	got := make(chan []Event, 4)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []Event
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &arr)
		got <- arr
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	sink := &errSink{}
	c := New(srv.URL, "tok", "proj", WithBatch(1000, time.Hour), WithOnError(sink.add))
	// Set the cap directly: New clamps the flush trigger down to it, and this
	// test needs the buffer to overflow without any flush firing in between.
	c.maxBuffer = 4

	for i := 0; i < 6; i++ {
		_ = c.Info(fmt.Sprintf("e%d", i), nil)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	events := <-got
	if len(events) != 4 {
		t.Fatalf("delivered %d events, want the 4 that fit the cap", len(events))
	}
	for i, e := range events {
		if want := fmt.Sprintf("e%d", i+2); e.Message != want {
			t.Fatalf("event %d is %q, want %q: the OLDEST events must be the ones dropped", i, e.Message, want)
		}
	}

	errs := sink.all()
	if len(errs) != 1 {
		t.Fatalf("sink got %d errors, want 1 coalesced drop report: %v", len(errs), errs)
	}
	if !errors.Is(errs[0], ErrBufferFull) {
		t.Fatalf("drop reported as %v, want ErrBufferFull", errs[0])
	}
	if !strings.Contains(errs[0].Error(), "dropped 2") {
		t.Fatalf("drop report %q does not say how many events were lost", errs[0])
	}
}

// A flush trigger above the buffer cap can never fire, so the cap would silently
// become the batch size; the limits must also be overridable for a retuned
// gateway.
func TestOptionClamping(t *testing.T) {
	cases := []struct {
		name                             string
		opts                             []Option
		batch, maxBatch, maxBody, maxBuf int
	}{
		{"defaults", nil, 0, defaultMaxBatch, defaultMaxBodyBytes, defaultMaxBuffer},
		{"batch clamped to the gateway cap", []Option{WithBatch(5000, time.Hour)}, defaultMaxBatch, defaultMaxBatch, defaultMaxBodyBytes, defaultMaxBuffer},
		{"batch clamped to a custom cap", []Option{WithBatch(5000, time.Hour), WithLimits(100, 0)}, 100, 100, defaultMaxBodyBytes, defaultMaxBuffer},
		{"option order does not matter", []Option{WithLimits(100, 0), WithBatch(5000, time.Hour)}, 100, 100, defaultMaxBodyBytes, defaultMaxBuffer},
		{"batch clamped to the buffer cap", []Option{WithBatch(1000, time.Hour), WithMaxBuffer(4)}, 4, defaultMaxBatch, defaultMaxBodyBytes, 4},
		{"non-positive overrides keep the defaults", []Option{WithLimits(0, -1), WithMaxBuffer(0)}, 0, defaultMaxBatch, defaultMaxBodyBytes, defaultMaxBuffer},
		{"custom body cap", []Option{WithLimits(0, 4096)}, 0, defaultMaxBatch, 4096, defaultMaxBuffer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := New("http://127.0.0.1:1", "tok", "proj", append(tc.opts, WithOnError(func(error) {}))...)
			defer c.Close() // nothing is buffered, so no request is attempted
			if c.batch != tc.batch || c.maxBatch != tc.maxBatch || c.maxBodyBytes != tc.maxBody || c.maxBuffer != tc.maxBuf {
				t.Fatalf("batch=%d maxBatch=%d maxBodyBytes=%d maxBuffer=%d, want %d/%d/%d/%d",
					c.batch, c.maxBatch, c.maxBodyBytes, c.maxBuffer, tc.batch, tc.maxBatch, tc.maxBody, tc.maxBuf)
			}
		})
	}
}

// WithLimits exists for a retuned gateway: the split must follow the configured
// caps, not the compiled-in defaults.
func TestWithLimitsDrivesTheSplit(t *testing.T) {
	t.Run("events", func(t *testing.T) {
		var sizes []int
		var mu sync.Mutex
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var arr []Event
			b, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(b, &arr)
			mu.Lock()
			sizes = append(sizes, len(arr))
			mu.Unlock()
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		c := New(srv.URL, "tok", "proj", WithBatch(3, time.Hour), WithLimits(3, 0))
		for i := 0; i < 7; i++ {
			c.mu.Lock()
			c.buf = append(c.buf, Event{Project: "proj", Message: "m"})
			c.mu.Unlock()
		}
		if err := c.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(sizes) != 3 {
			t.Fatalf("requests = %v, want three chunks of at most 3", sizes)
		}
		for _, n := range sizes {
			if n > 3 {
				t.Fatalf("a request carried %d events, over the configured cap of 3", n)
			}
		}
	})

	t.Run("bytes", func(t *testing.T) {
		var maxBody atomic.Int64
		var requests atomic.Int64
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			requests.Add(1)
			if int64(len(b)) > maxBody.Load() {
				maxBody.Store(int64(len(b)))
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		defer srv.Close()

		c := New(srv.URL, "tok", "proj", WithLimits(0, 4096))
		events := make([]Event, 8)
		for i := range events {
			events[i] = Event{Project: "proj", Message: strings.Repeat("x", 1024)}
		}
		if err := c.send(events); err != nil {
			t.Fatalf("send: %v", err)
		}
		if requests.Load() < 3 {
			t.Fatalf("8 KiB of events produced %d request(s) at a 4 KiB cap", requests.Load())
		}
		if got := maxBody.Load(); got > 4096 {
			t.Fatalf("a request of %d bytes exceeds the configured cap", got)
		}
	})
}

// The gateway answers 4xx/5xx with {"error":"<reason>"}; surfacing it is what
// turns an opaque 400 into "all_invalid".
func TestGatewayReasonIsSurfaced(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantReason string
		wantText   string
	}{
		{"json reason", 400, `{"error":"all_invalid"}`, "all_invalid", "logden: gateway returned 400: all_invalid"},
		{"auth", 401, `{"error":"auth"}` + "\n", "auth", "logden: gateway returned 401: auth"},
		{"plain text body", 400, "Bad Request\n", "", "logden: gateway returned 400"},
		{"empty body", 503, "", "", "logden: gateway returned 503"},
		{"other json shape", 500, `{"detail":"nope"}`, "", "logden: gateway returned 500"},
		// A wrong URL can land on anything; the reason must not be able to forge
		// a log line or drive a terminal.
		{"control characters stripped", 400, "{\"error\":\"bad\\nreason\\u0007\"}", "badreason", "logden: gateway returned 400: badreason"},
		{"long reason bounded", 400, `{"error":"` + strings.Repeat("a", 500) + `"}`, strings.Repeat("a", 120), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			err := New(srv.URL, "tok", "proj").Error("boom", nil)
			var ge *GatewayError
			if !errors.As(err, &ge) {
				t.Fatalf("got %v (%T), want a *GatewayError", err, err)
			}
			if ge.StatusCode != tc.status {
				t.Fatalf("StatusCode = %d, want %d", ge.StatusCode, tc.status)
			}
			if ge.Reason != tc.wantReason {
				t.Fatalf("Reason = %q, want %q", ge.Reason, tc.wantReason)
			}
			if tc.wantText != "" && err.Error() != tc.wantText {
				t.Fatalf("error text = %q, want %q", err.Error(), tc.wantText)
			}
		})
	}
}

// A bad token in batch mode used to discard every log forever with no signal.
func TestBatchSendFailureReachesSink(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":"auth"}`)
	}))
	defer srv.Close()

	sink := &errSink{}
	c := New(srv.URL, "wrong-token", "proj", WithBatch(1, time.Hour), WithOnError(sink.add))
	_ = c.Info("x", nil)
	waitFor(t, "the failed background send to be reported", func() bool { return sink.len() >= 1 })
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var ge *GatewayError
	if !errors.As(sink.all()[0], &ge) {
		t.Fatalf("sink got %v, want a *GatewayError", sink.all()[0])
	}
	if ge.Reason != "auth" {
		t.Fatalf("sink error reason = %q, want the gateway's %q", ge.Reason, "auth")
	}
}

// The byte split bottoms out at one event. A single event over the cap is doomed
// (the gateway 413s the whole request), so it must be reported rather than
// posted and forgotten.
func TestOversizedSingleEventIsReportedNotSent(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusRequestEntityTooLarge)
	}))
	defer srv.Close()

	c := New(srv.URL, "tok", "proj", WithLimits(0, 1024))
	payload := strings.Repeat("x", 4096)
	err := c.Error(payload, nil)
	if !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("got %v, want ErrEventTooLarge", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("a doomed request was sent anyway (%d)", requests.Load())
	}
	text := err.Error()
	for _, want := range []string{`project="proj"`, `level="error"`, "message=4096B", "limit=1024B"} {
		if !strings.Contains(text, want) {
			t.Fatalf("report %q does not identify the event (missing %q)", text, want)
		}
	}
	if strings.Contains(text, payload[:64]) {
		t.Fatalf("the report echoed the payload: %q", text)
	}
}

// The unencodable-context fallback can itself produce an oversized body; the
// event must not disappear between the two guards.
func TestOversizedAfterContextFallbackIsReported(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := New(srv.URL, "tok", "proj", WithLimits(0, 1024))
	e := Event{
		Project: "proj",
		Level:   "error",
		Message: strings.Repeat("x", 4096),
		Context: map[string]any{"ratio": math.NaN()}, // has no JSON representation
	}
	err := c.send([]Event{e})
	if !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("got %v, want ErrEventTooLarge", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("an oversized request was sent anyway (%d)", requests.Load())
	}
}

// The level set must stay the gateway's PSR-3 vocabulary (gateway/validate.go):
// anything else is silently rewritten to "info" on ingest.
func TestLevelHelpers(t *testing.T) {
	got := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var arr []Event
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &arr)
		if len(arr) == 1 {
			got <- arr[0].Level
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := New(srv.URL, "tok", "proj")
	cases := []struct {
		name string
		fn   func(string, map[string]any) error
		want string
	}{
		{"Debug", c.Debug, "debug"},
		{"Info", c.Info, "info"},
		{"Notice", c.Notice, "notice"},
		{"Warning", c.Warning, "warning"},
		{"Warn", c.Warn, "warning"}, // kept as an alias so old callers compile
		{"Error", c.Error, "error"},
		{"Critical", c.Critical, "critical"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.fn("m", nil); err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if lvl := <-got; lvl != tc.want {
				t.Fatalf("%s sent level %q, want %q", tc.name, lvl, tc.want)
			}
		})
	}
}

// The sink is application code running on the client's goroutine, where a panic
// has no caller frame to recover it: it would kill the process and take the
// flusher with it.
func TestSinkPanicDoesNotKillTheFlusher(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	var calls atomic.Int64
	c := New(srv.URL, "tok", "proj", WithBatch(1, time.Hour), WithOnError(func(error) {
		calls.Add(1)
		panic("sink blew up")
	}))
	_ = c.Info("a", nil)
	waitFor(t, "the first sink call", func() bool { return calls.Load() >= 1 })
	_ = c.Info("b", nil)
	waitFor(t, "the flusher to survive the panic", func() bool { return calls.Load() >= 2 })
	_ = c.Close()
}

// syncWriter is log.SetOutput-safe: the flusher writes to it while the test
// reads it.
type syncWriter struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncWriter) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// Without WithOnError the failure must still be visible somewhere: a silent
// default is exactly the "every log discarded forever, no signal" failure.
func TestDefaultSinkIsNotSilent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"error":"all_invalid"}`)
	}))
	defer srv.Close()

	var out syncWriter
	old := log.Writer()
	log.SetOutput(&out)
	defer log.SetOutput(old)

	c := New(srv.URL, "tok", "proj", WithBatch(1, time.Hour))
	_ = c.Info("x", nil)
	waitFor(t, "the default sink to report the failure", func() bool {
		return strings.Contains(out.String(), "gateway returned 400: all_invalid")
	})
	_ = c.Close()
}
