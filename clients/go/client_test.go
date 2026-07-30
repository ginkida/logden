package logden

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

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
	_ = bc.Info("c", nil) // reached 3 -> flush
	if total.Load() != 4 {
		t.Fatalf("batch flush: want 4 got %d", total.Load())
	}
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
		if len(arr) > maxBatch {
			oversized.Add(1)
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// Batching is on but the flush trigger is far away, so the buffer grows past
	// the wire cap before Flush runs.
	c := New(srv.URL, "tok", "proj", WithBatch(maxBatch, time.Hour))
	const n = maxBatch*2 + 7
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
	if got := maxBody.Load(); got > maxBodyBytes {
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
