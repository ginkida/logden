package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimiter(t *testing.T) {
	rl := newRateLimiter(1, 2) // 1 rps, burst 2
	first, second := rl.allow(), rl.allow()
	if !first || !second {
		t.Fatal("burst of 2 should allow two immediate requests")
	}
	if rl.allow() {
		t.Fatal("third immediate request should be denied")
	}
}

func TestRateLimitedEndpoint(t *testing.T) {
	cfg := testConfig()
	cfg.rateLimit = 1
	cfg.rateBurst = 1
	s := newServer(cfg)
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"a"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("first: want 204 got %d", rr.Code)
	}
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"b"}`, nil); rr.Code != http.StatusTooManyRequests {
		t.Fatalf("second: want 429 got %d", rr.Code)
	}
}

func TestGzipBomb(t *testing.T) {
	cfg := testConfig()
	cfg.maxBodyBytes = 100 // small limit on the decompressed body
	s := newServer(cfg)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte(`{"project":"p","message":"` + strings.Repeat("x", 500) + `"}`))
	_ = gz.Close()

	req := httptest.NewRequest("POST", "/logs", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Encoding", "gzip")
	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("gzip bomb: want 413 got %d (decompressed must be capped)", rr.Code)
	}
}

func TestReadinessCache(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if int(status.Load()) == http.StatusOK {
			_, _ = io.WriteString(w, "1")
			return
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	rc := newReadinessCache(cfg, newMetrics("", "", ""), 40*time.Millisecond)

	if !rc.check() {
		t.Fatal("should be ready when ClickHouse returns 1")
	}
	status.Store(http.StatusInternalServerError)
	if !rc.check() {
		t.Fatal("should stay cached-ready within TTL")
	}
	time.Sleep(60 * time.Millisecond)
	if rc.check() {
		t.Fatal("should be not-ready after TTL refresh")
	}
}

func TestMetricsHistogramFormat(t *testing.T) {
	m := newMetrics("v1", "abc", "")
	m.insertDur.observe(0.03)
	m.insertDur.observe(2.0)
	out := m.render()
	for _, want := range []string{
		`logden_clickhouse_insert_duration_seconds_bucket{le="+Inf"} 2`,
		"logden_clickhouse_insert_duration_seconds_sum ",
		"logden_clickhouse_insert_duration_seconds_count 2",
		`logden_build_info{version="v1",commit="abc"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q", want)
		}
	}
}

func TestMetricsAuth(t *testing.T) {
	cfg := testConfig()
	cfg.metricsToken = "msecret"
	s := newServer(cfg)

	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token: want 401 got %d", rr.Code)
	}

	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Authorization", "Bearer msecret")
	rr2 := httptest.NewRecorder()
	s.mux().ServeHTTP(rr2, req)
	if rr2.Code != http.StatusOK {
		t.Fatalf("correct token: want 200 got %d", rr2.Code)
	}

	// without METRICS_TOKEN — open
	open := newServer(testConfig())
	rr3 := httptest.NewRecorder()
	open.mux().ServeHTTP(rr3, httptest.NewRequest("GET", "/metrics", nil))
	if rr3.Code != http.StatusOK {
		t.Fatalf("open metrics: want 200 got %d", rr3.Code)
	}
}

func TestEnqueueAfterStop(t *testing.T) {
	s := newServer(testConfig())
	s.ingest.start()
	s.ingest.stop()
	// enqueue after stop() must not panic (send on closed channel) and must
	// drop the event cleanly.
	acc, drop := s.ingest.enqueue([]row{{Project: "p", Message: "x"}}, "1.2.3.4")
	if acc != 0 || drop != 1 {
		t.Fatalf("after stop want accepted=0 dropped=1, got %d/%d", acc, drop)
	}
}

func TestClientIPNoTrustedProxies(t *testing.T) {
	s := newServer(testConfig()) // trustedProxies is empty
	req := httptest.NewRequest("POST", "/logs", nil)
	req.RemoteAddr = "203.0.113.7:9999"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")
	if got := s.clientIP(req); got != "203.0.113.7" {
		t.Fatalf("without trusted proxies must use peer, got %q", got)
	}
}

// stop() closes the buffer channel under the very mutex enqueue sends on. Closing
// it anywhere else is a send-on-closed-channel panic for a handler still inside
// enqueue — which is exactly what an HTTP Shutdown that timed out leaves behind.
// The window is one instruction wide, so it needs many writers and -race to show.
func TestStopDuringConcurrentEnqueueDoesNotPanic(t *testing.T) {
	cfg := testConfig()
	cfg.bufferSize = 2000
	cfg.batchSize = 500
	cfg.flushInterval = 10 * time.Millisecond
	// chBaseURL is empty and no spool dir is set: every insert fails immediately
	// and the batch is dropped, so the worker keeps draining without any network.
	s := newServer(cfg)
	s.ingest.start()

	const writers = 50
	var offered, accepted, dropped atomic.Int64
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("enqueue panicked during stop(): %v", r)
				}
			}()
			rows := []row{{Project: "p", Message: "a"}, {Project: "p", Message: "b"}}
			for {
				select {
				case <-done:
					return
				default:
				}
				acc, drop := s.ingest.enqueue(rows, "192.0.2.1")
				offered.Add(int64(len(rows)))
				accepted.Add(int64(acc))
				dropped.Add(int64(drop))
				// All-or-nothing: a half-accepted batch means the client's retry of
				// the same request duplicates whatever did get through.
				if acc != 0 && acc != len(rows) {
					t.Errorf("enqueue accepted %d of %d rows: the batch was split", acc, len(rows))
					return
				}
			}
		}()
	}

	waitFor(t, 5*time.Second, func() bool { return offered.Load() > 500 })
	s.ingest.stop() // closes the channel while every writer is inside enqueue
	close(done)

	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("writers did not return after stop()")
	}

	if accepted.Load() == 0 {
		t.Fatal("nothing was accepted: the writers never reached the live path")
	}
	if dropped.Load() == 0 {
		t.Fatal("nothing was dropped: the writers never raced the close")
	}
	if got := accepted.Load() + dropped.Load(); got != offered.Load() {
		t.Fatalf("accepted+dropped = %d but %d rows were offered: rows vanished", got, offered.Load())
	}
}

// logden_clickhouse_reachable backs two alert rules, and the background loop is
// the only thing that keeps it current on an idle gateway that nobody probes.
func TestReadinessLoopUpdatesMetric(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !healthy.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = io.WriteString(w, "1")
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	m := newMetrics("", "", "")
	rc := newReadinessCache(cfg, m, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		rc.loop(ctx)
	}()

	// No /readyz request is made anywhere in this test: the loop alone must move
	// the gauge, in both directions.
	waitFor(t, 2*time.Second, func() bool { return m.chReachable.Load() == 1 })
	healthy.Store(false)
	waitFor(t, 2*time.Second, func() bool { return m.chReachable.Load() == 0 })
	if !strings.Contains(m.render(), "logden_clickhouse_reachable 0\n") {
		t.Error("the gauge the alert reads was not rendered as unreachable")
	}

	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("loop did not return when its context was cancelled: shutdown would hang")
	}
}

// While one probe is in flight every other caller gets the LAST known answer
// instead of starting its own query. /readyz is unauthenticated, so without the
// single-flight guard a burst past the TTL becomes one ClickHouse SELECT per
// request on a box sized for none.
func TestReadinessServesLastValueWhileProbing(t *testing.T) {
	var probes atomic.Int64
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if probes.Add(1) > 1 { // the first probe answers at once; the second blocks
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-time.After(5 * time.Second):
				t.Error("probe stub was never released")
			}
		}
		_, _ = io.WriteString(w, "1")
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	// ttl 0: nothing is ever fresh, so every check would probe if it were not for
	// the in-flight guard — and the test needs no sleep to expire a cache.
	rc := newReadinessCache(cfg, newMetrics("", "", ""), 0)
	if !rc.check() {
		t.Fatal("the first probe should report ready")
	}

	probing := make(chan struct{})
	go func() {
		defer close(probing)
		rc.check() // blocks inside the stub until release
	}()
	defer func() {
		close(release)
		select {
		case <-probing:
		case <-time.After(5 * time.Second):
			t.Error("the blocked probe never finished")
		}
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the second probe never reached ClickHouse")
	}

	if !rc.check() {
		t.Fatal("a check made while a probe is in flight must serve the last value, not a fresh failure")
	}
	if got := probes.Load(); got != 2 {
		t.Fatalf("probes = %d, want 2: the third check fired its own query instead of reusing the in-flight one", got)
	}
}
