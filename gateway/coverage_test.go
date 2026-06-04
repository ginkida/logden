package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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
