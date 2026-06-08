package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestByteSemaphore(t *testing.T) {
	bs := newByteSemaphore(100)
	if !bs.tryAcquire(60) {
		t.Fatal("60/100 must fit")
	}
	if bs.tryAcquire(50) {
		t.Fatal("60+50 must not fit in 100")
	}
	if !bs.tryAcquire(40) {
		t.Fatal("60+40 must fit exactly")
	}
	bs.release(60)
	if got := bs.inUse(); got != 40 {
		t.Fatalf("inUse = %d, want 40", got)
	}
	// Zero-cost requests are always admitted (empty bodies fail validation later).
	if !bs.tryAcquire(0) {
		t.Fatal("zero cost must be admitted")
	}
}

func TestByteSemaphoreConcurrent(t *testing.T) {
	bs := newByteSemaphore(1000)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if bs.tryAcquire(10) {
				bs.release(10)
			}
		}()
	}
	wg.Wait()
	if got := bs.inUse(); got != 0 {
		t.Fatalf("inUse = %d after all released, want 0", got)
	}
}

func TestBodyCost(t *testing.T) {
	cfg := testConfig()
	cfg.maxBodyBytes = 1000
	s := newServer(cfg)

	// Identity body with Content-Length: the declared size is the cost.
	req := httptest.NewRequest("POST", "/logs", strings.NewReader("0123456789"))
	if got := s.bodyCost(req); got != 10 {
		t.Fatalf("identity cost = %d, want 10", got)
	}

	// Declared length above the limit is clamped (readBody rejects it anyway).
	req = httptest.NewRequest("POST", "/logs", strings.NewReader(strings.Repeat("x", 2000)))
	if got := s.bodyCost(req); got != 1000 {
		t.Fatalf("oversized cost = %d, want clamp to 1000", got)
	}

	// gzip can expand up to the decompressed limit: reserve the worst case.
	req = httptest.NewRequest("POST", "/logs", strings.NewReader("xx"))
	req.Header.Set("Content-Encoding", "gzip")
	if got := s.bodyCost(req); got != 1000 {
		t.Fatalf("gzip cost = %d, want worst case 1000", got)
	}

	// Unknown length (chunked): worst case too.
	req = httptest.NewRequest("POST", "/logs", strings.NewReader("xx"))
	req.ContentLength = -1
	if got := s.bodyCost(req); got != 1000 {
		t.Fatalf("chunked cost = %d, want worst case 1000", got)
	}
}

func TestInflightOverloaded(t *testing.T) {
	cfg := testConfig()
	cfg.maxInflightBytes = 16 // tiny budget: any real body exceeds it
	s := newServer(cfg)

	body := `{"project":"p","message":"this body is bigger than sixteen bytes"}`
	rr := doLogs(s, "POST", "secret", body, nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 got %d", rr.Code)
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("503 overloaded must carry Retry-After")
	}
	// The budget must be fully released after the rejected request.
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inflight bytes leaked: %d", got)
	}

	// A body within the budget still passes.
	if rr := doLogs(s, "POST", "secret", `{"a":1}`, nil); rr.Code == http.StatusServiceUnavailable {
		t.Fatalf("small body must not be rejected as overloaded, got %d", rr.Code)
	}
}

func TestInflightReleasedAfterSuccess(t *testing.T) {
	cfg := testConfig()
	cfg.maxInflightBytes = 4 << 20
	s := newServer(cfg)
	for i := 0; i < 5; i++ {
		rr := doLogs(s, "POST", "secret", `{"project":"p","message":"m"}`, nil)
		if rr.Code != http.StatusNoContent {
			t.Fatalf("req %d: want 204 got %d", i, rr.Code)
		}
	}
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inflight bytes leaked after sequential requests: %d", got)
	}
}

func TestBufferByteCap(t *testing.T) {
	cfg := testConfig()
	cfg.bufferMaxBytes = 150 // one small row (~90 bytes) fits, two don't
	s := newServer(cfg)      // worker not started — nobody drains the buffer

	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"a"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("first: want 204 got %d", rr.Code)
	}
	if got := s.ingest.depthBytes(); got <= 0 || got > 150 {
		t.Fatalf("depthBytes = %d, want within (0,150]", got)
	}
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"b"}`, nil); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("byte overflow: want 503 got %d", rr.Code)
	}
	if s.m.dropped.Load() == 0 {
		t.Fatal("expected dropped metric > 0")
	}
}

func TestBufferBytesReleasedAfterFlush(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.batchSize = 1
	cfg.flushInterval = 10 * time.Millisecond
	cfg.bufferMaxBytes = 150
	s := newServer(cfg)
	s.ingest.start()
	defer s.ingest.stop()

	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"a"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("first: want 204 got %d", rr.Code)
	}
	// After the insert lands the byte budget must be returned in full.
	waitFor(t, 2*time.Second, func() bool { return s.ingest.depthBytes() == 0 })
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"b"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("after flush: want 204 got %d", rr.Code)
	}
}

func TestSpoolByteCap(t *testing.T) {
	cfg := testConfig()
	cfg.spoolDir = t.TempDir()
	cfg.spoolMaxBytes = 10
	s := newServer(cfg)

	dropped := s.m.dropped.Load()
	s.ingest.spool([]byte("this batch is bigger than ten bytes"), 3)
	if got := s.m.dropped.Load() - dropped; got != 3 {
		t.Fatalf("dropped delta = %d, want 3", got)
	}
	if got := s.m.spoolFiles.Load(); got != 0 {
		t.Fatalf("spoolFiles = %d, want 0 (write must be refused)", got)
	}

	// Raise the cap: the write goes through and the bytes gauge is updated.
	s.ingest.cfg.spoolMaxBytes = 1 << 20
	s.ingest.spool([]byte("ok"), 1)
	if got := s.m.spoolFiles.Load(); got != 1 {
		t.Fatalf("spoolFiles = %d, want 1", got)
	}
	if got := s.m.spoolBytes.Load(); got != 2 {
		t.Fatalf("spoolBytes = %d, want 2", got)
	}
}

func TestSpoolBytesCountBadFiles(t *testing.T) {
	// Quarantined .bad files keep holding disk: they must stay in the bytes
	// gauge (and therefore inside the SPOOL_MAX_BYTES cap) until removed.
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Cannot parse input", http.StatusBadRequest)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.spoolDir = t.TempDir()
	s := newServer(cfg)

	s.ingest.spool([]byte(`{"message":"poison"}`), 1)
	s.ingest.replayOnce() // 400 from the stub => quarantine to .bad

	if got := s.m.spoolFiles.Load(); got != 0 {
		t.Fatalf("spoolFiles = %d, want 0 after quarantine", got)
	}
	if got := s.m.spoolBytes.Load(); got == 0 {
		t.Fatal("spoolBytes must still count the quarantined .bad file")
	}
}

func TestConfigValidateCaps(t *testing.T) {
	base := func() config {
		c := testConfig()
		c.listenAddr = ":8080"
		c.chBaseURL = "http://localhost:8123"
		c.flushInterval = time.Second
		c.replayInterval = time.Second
		c.rateLimit = 0
		return c
	}
	if err := base().validate(); err != nil {
		t.Fatalf("base config must validate: %v", err)
	}

	c := base()
	c.bufferMaxBytes = -1
	if err := c.validate(); err == nil {
		t.Fatal("negative BUFFER_MAX_BYTES must fail")
	}
	// Buffer floor is 2× MAX_BODY_BYTES (rows re-serialize larger than the body).
	c = base()
	c.bufferMaxBytes = c.maxBodyBytes // exactly 1× must now fail
	if err := c.validate(); err == nil {
		t.Fatal("BUFFER_MAX_BYTES == MAX_BODY_BYTES must fail (rows amplify)")
	}
	c = base()
	c.bufferMaxBytes = 2 * c.maxBodyBytes // exactly 2× is the floor and must pass
	if err := c.validate(); err != nil {
		t.Fatalf("BUFFER_MAX_BYTES == 2× MAX_BODY_BYTES must pass: %v", err)
	}
	c = base()
	c.maxInflightBytes = c.maxBodyBytes - 1
	if err := c.validate(); err == nil {
		t.Fatal("MAX_INFLIGHT_BODY_BYTES below MAX_BODY_BYTES must fail")
	}
	c = base()
	c.spoolMaxBytes = -1
	if err := c.validate(); err == nil {
		t.Fatal("negative SPOOL_MAX_BYTES must fail")
	}
	// 0 disables each cap and is always valid.
	c = base()
	c.bufferMaxBytes, c.spoolMaxBytes, c.maxInflightBytes = 0, 0, 0
	if err := c.validate(); err != nil {
		t.Fatalf("zero caps must validate: %v", err)
	}
}

func TestMetricsRenderCaps(t *testing.T) {
	cfg := testConfig()
	cfg.maxInflightBytes = 4 << 20
	cfg.spoolMaxBytes = 256 << 20
	s := newServer(cfg)
	out := s.m.render()
	for _, want := range []string{
		"logden_buffer_bytes",
		"logden_inflight_body_bytes",
		"logden_spool_bytes",
		"logden_spool_capacity_bytes 268435456",
		"logden_process_start_time_seconds",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
