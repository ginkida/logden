package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

// The budget must hold under concurrency: at most max/cost holders at any moment,
// some requests admitted, and nothing leaked once they all return. (Asserting
// only "inUse() == 0 at the end" would also pass if tryAcquire always failed or
// if the cap were ignored entirely.)
func TestByteSemaphoreConcurrent(t *testing.T) {
	const total, cost, wantHolders = 1000, 100, 10
	bs := newByteSemaphore(total)

	var holders, maxHolders, granted atomic.Int64
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !bs.tryAcquire(cost) {
				return
			}
			granted.Add(1)
			cur := holders.Add(1)
			for {
				m := maxHolders.Load()
				if cur <= m || maxHolders.CompareAndSwap(m, cur) {
					break
				}
			}
			<-release // hold the reservation until every goroutine has tried
			holders.Add(-1)
			bs.release(cost)
		}()
	}
	waitFor(t, 2*time.Second, func() bool { return bs.inUse() == total })
	close(release)
	wg.Wait()

	// Goroutines that were still starting up when the holders released get their
	// turn, so the total admitted is >= the budget; what must never happen is more
	// than budget/cost of them holding at the same time.
	if got := granted.Load(); got < wantHolders {
		t.Fatalf("only %d requests admitted, want at least %d (%d budget / %d each)", got, wantHolders, total, cost)
	}
	if got := maxHolders.Load(); got != wantHolders {
		t.Fatalf("peak concurrent holders = %d, want exactly %d (%d budget / %d each)", got, wantHolders, total, cost)
	}
	if got := bs.inUse(); got != 0 {
		t.Fatalf("inUse = %d after all released, want 0", got)
	}
}

// The buffer's byte budget is incremented by handlers and decremented by the
// worker; a drift under concurrent handlers wedges the gateway at 503 forever.
func TestEnqueueByteAccountingUnderConcurrency(t *testing.T) {
	cfg := testConfig()
	cfg.bufferSize = 300
	cfg.bufferMaxBytes = 1 << 20
	s := newServer(cfg) // no worker started: nothing drains the buffer

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rows := []row{
				{Project: "p", Message: strings.Repeat("m", i)},
				{Project: "p", Message: "second"},
			}
			if acc, drop := s.ingest.enqueue(rows, "192.0.2.1"); acc != 2 || drop != 0 {
				t.Errorf("enqueue = %d/%d, want 2/0", acc, drop)
			}
		}(i)
	}
	wg.Wait()

	var sum int64
	for n := s.ingest.depth(); n > 0; n-- {
		sum += int64(len(<-s.ingest.ch))
	}
	if got := s.ingest.depthBytes(); got != sum {
		t.Fatalf("depthBytes = %d but the buffer holds %d bytes: accounting drifted", got, sum)
	}
}

func TestReservationChargesOnlyWhatIsRead(t *testing.T) {
	cfg := testConfig()
	cfg.maxInflightBytes = 1 << 20
	s := newServer(cfg)

	res := s.newReservation()
	// Exactly what is read, never rounded up: rounding to a fixed chunk would cap
	// concurrency at budget/chunk requests however small their bodies are.
	if !res.charge(10) {
		t.Fatal("first charge must be admitted")
	}
	if got := s.inflight.inUse(); got != 10 {
		t.Fatalf("inUse = %d, want exactly 10", got)
	}
	if !res.charge(90) {
		t.Fatal("second charge must be admitted")
	}
	if got := s.inflight.inUse(); got != 100 {
		t.Fatalf("inUse = %d, want exactly 100", got)
	}
	// Over the budget the charge is refused and nothing is added.
	if res.charge(cfg.maxInflightBytes) {
		t.Fatal("a charge past the budget must be refused")
	}
	if got := s.inflight.inUse(); got != 100 {
		t.Fatalf("a refused charge changed the budget: inUse = %d, want 100", got)
	}
	res.release()
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inUse = %d after release, want 0", got)
	}
	res.release() // double release must not go negative
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inUse = %d after double release, want 0", got)
	}

	// Disabled admission control: every call is a no-op that admits.
	off := newServer(testConfig())
	if nilRes := off.newReservation(); nilRes != nil || !nilRes.charge(1<<30) {
		t.Fatal("with MAX_INFLIGHT_BODY_BYTES=0 the reservation must be nil and permissive")
	}
}

// The whole point of charging on read: a client that announces a big body and
// then stalls must not hold the budget hostage (it used to reserve
// MAX_BODY_BYTES for the entire ReadTimeout, so four sockets 503'd everyone).
func TestStalledBodyDoesNotHoldBudget(t *testing.T) {
	cfg := testConfig()
	cfg.maxInflightBytes = 4 << 20
	cfg.maxBodyBytes = 4 << 20
	s := newServer(cfg)

	unblock := make(chan struct{})
	stalledPrefix := `{"project":"p","mess`
	stalled := &blockingReader{first: []byte(stalledPrefix), gate: unblock}
	req := httptest.NewRequest("POST", "/logs", stalled)
	req.Header.Set("Authorization", "Bearer secret")
	req.ContentLength = -1 // chunked: the old code reserved the worst case here
	done := make(chan int, 1)
	go func() {
		rr := httptest.NewRecorder()
		s.mux().ServeHTTP(rr, req)
		done <- rr.Code
	}()

	// While it hangs, it must hold only the bytes it actually sent...
	waitFor(t, 2*time.Second, func() bool { return s.inflight.inUse() > 0 })
	if got := s.inflight.inUse(); got > int64(len(stalledPrefix)) {
		t.Fatalf("a stalled request holds %d bytes, want at most the %d it sent", got, len(stalledPrefix))
	}
	// ...and the gateway must keep serving everyone else.
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"m"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("a stalled client blocked a healthy one: got %d", rr.Code)
	}

	close(unblock)
	if code := <-done; code != http.StatusBadRequest {
		t.Fatalf("the truncated body should end as 400, got %d", code)
	}
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inflight bytes leaked after the stalled request: %d", got)
	}
}

// blockingReader yields a prefix, then blocks until gate is closed, then EOFs.
type blockingReader struct {
	first []byte
	gate  chan struct{}
	done  bool
}

func (b *blockingReader) Read(p []byte) (int, error) {
	if len(b.first) > 0 {
		n := copy(p, b.first)
		b.first = b.first[n:]
		return n, nil
	}
	if !b.done {
		<-b.gate
		b.done = true
	}
	return 0, io.EOF
}

// Small gzip/chunked requests must not each cost the worst case: reserving
// MAX_BODY_BYTES upfront capped concurrency at 4 with the shipped defaults, so
// a handful of ordinary compressed clients could 503 everyone else.
func TestManySmallGzipRequestsAreAdmitted(t *testing.T) {
	cfg := testConfig()
	cfg.maxBodyBytes = 4 << 20      // upfront reservation would have been this...
	cfg.maxInflightBytes = 16 << 20 // ...so only 4 could be in flight at once
	s := newServer(cfg)
	h := s.mux()

	var body bytes.Buffer
	gz := gzip.NewWriter(&body)
	if _, err := gz.Write([]byte(`{"project":"p","message":"small compressed event"}`)); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	payload := body.Bytes()

	const conc = 20
	var ok, shed atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/logs", bytes.NewReader(payload))
			req.Header.Set("Authorization", "Bearer secret")
			req.Header.Set("Content-Encoding", "gzip")
			rr := httptest.NewRecorder()
			<-start
			h.ServeHTTP(rr, req)
			switch rr.Code {
			case http.StatusNoContent:
				ok.Add(1)
			case http.StatusServiceUnavailable:
				shed.Add(1)
			default:
				t.Errorf("unexpected status %d: %s", rr.Code, rr.Body.String())
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := ok.Load(); got != conc {
		t.Fatalf("%d/%d small gzip requests accepted (%d shed): the budget is still reserved per worst case",
			got, conc, shed.Load())
	}
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inflight bytes leaked: %d", got)
	}
}

// A flood of tiny requests must not be shed for memory it never used: with a
// per-request minimum charge the budget would cap concurrency at
// MAX_INFLIGHT_BODY_BYTES/minimum requests regardless of their real size.
func TestManyTinyRequestsAreNotShed(t *testing.T) {
	const conc = 200 // 200 x ~30 bytes fits many times over
	cfg := testConfig()
	cfg.maxBodyBytes = 4 << 20
	cfg.maxInflightBytes = 64 << 10 // a single 64KiB "chunk" worth of budget
	cfg.bufferSize = 2 * conc       // so a 503 can only come from admission control
	s := newServer(cfg)
	h := s.mux()
	var ok, shed atomic.Int64
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest("POST", "/logs", strings.NewReader(`{"project":"p","message":"tiny"}`))
			req.Header.Set("Authorization", "Bearer secret")
			rr := httptest.NewRecorder()
			<-start
			h.ServeHTTP(rr, req)
			switch rr.Code {
			case http.StatusNoContent:
				ok.Add(1)
			case http.StatusServiceUnavailable:
				shed.Add(1)
			default:
				t.Errorf("unexpected status %d", rr.Code)
			}
		}()
	}
	close(start)
	wg.Wait()

	if got := shed.Load(); got != 0 {
		t.Fatalf("%d/%d tiny requests were shed: the budget is charged in fixed chunks", got, conc)
	}
	if got := ok.Load(); got != conc {
		t.Fatalf("%d/%d tiny requests accepted", got, conc)
	}
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inflight bytes leaked: %d", got)
	}
}

// config.validate() accepts MAX_INFLIGHT_BODY_BYTES == MAX_BODY_BYTES on the
// promise that "one max-size request must fit". Assert the promise end to end at
// that exact floor — this is what the fixed-chunk accounting used to break.
func TestMaxSizeBodyFitsAtTheInflightFloor(t *testing.T) {
	const limit = 1 << 20
	cfg := testConfig()
	cfg.maxBodyBytes = limit
	cfg.maxInflightBytes = limit // the accepted floor, nothing to spare
	cfg.maxMessageBytes = limit
	cfg.bufferSize = 10 // a too-small buffer would 503 for a different reason
	cfg.bufferMaxBytes = 8 * limit
	cfg.listenAddr, cfg.chBaseURL = ":8080", "http://127.0.0.1:8123"
	cfg.flushInterval, cfg.replayInterval = time.Second, time.Second
	if err := cfg.validate(); err != nil {
		t.Fatalf("equal caps must be a valid config: %v", err)
	}
	s := newServer(cfg)

	// ~98% of the limit, in one event.
	msg := strings.Repeat("m", limit-limit/50)
	body := `{"project":"p","message":"` + msg + `"}`
	if int64(len(body)) > cfg.maxBodyBytes {
		t.Fatalf("test body %d exceeds the limit %d", len(body), cfg.maxBodyBytes)
	}

	rr := doLogs(s, "POST", "secret", body, nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("a max-size request must fit at the floor: got %d (%s); rejections: %s",
			rr.Code, rr.Body.String(), s.m.render())
	}
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inflight bytes leaked: %d", got)
	}

	// One byte over the limit is a 413 (too large), not a 503 (overloaded) — even
	// though the detection byte is what exhausts the budget at equal caps.
	over := `{"project":"p","message":"` + strings.Repeat("m", limit) + `"}`
	rr = doLogs(s, "POST", "secret", over, nil)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body: want 413 got %d", rr.Code)
	}
	if !strings.Contains(s.m.render(), `logden_logs_rejected_total{reason="too_large"} 1`) {
		t.Errorf("oversized body must be counted as too_large, not overloaded:\n%s", s.m.render())
	}
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inflight bytes leaked after the 413: %d", got)
	}
}

// gzip is charged by what it inflates to, not by its wire size: otherwise the
// budget would not bound the memory a bomb actually costs.
func TestGzipChargedByDecompressedSize(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(`{"project":"p","message":"` + strings.Repeat("x", 200<<10) + `"}`)); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	cfg.maxBodyBytes = 1 << 20
	// Above the compressed size, far below the decompressed one.
	cfg.maxInflightBytes = 128 << 10
	if int64(buf.Len()) >= cfg.maxInflightBytes {
		t.Fatalf("test payload compressed to %d bytes, expected well under the budget", buf.Len())
	}
	s := newServer(cfg)

	req := httptest.NewRequest("POST", "/logs", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Encoding", "gzip")
	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 overloaded got %d (%s)", rr.Code, rr.Body.String())
	}
	if rr.Header().Get("Retry-After") == "" {
		t.Fatal("503 overloaded must carry Retry-After")
	}
	if got := s.inflight.inUse(); got != 0 {
		t.Fatalf("inflight bytes leaked: %d", got)
	}
	if !strings.Contains(s.m.render(), `logden_logs_rejected_total{reason="overloaded"} 1`) {
		t.Error("the rejection must be counted as overloaded")
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

// The budget must stay charged for the WHOLE flush, not just while the batch sits
// in the channel: buffer and in-flight batch share BUFFER_MAX_BYTES, so releasing
// the bytes before the insert returns lets a fresh 32MiB of events be admitted
// next to an 8MiB batch that is still in memory — the OOM the byte caps exist to
// prevent. TestBufferBytesReleasedAfterFlush cannot see this: it only checks the
// budget once the insert has already landed.
func TestBufferBytesHeldUntilFlushCompletes(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case entered <- struct{}{}:
		default: // a later insert (the drain) must not block on an unread signal
		}
		select {
		case <-release:
		case <-time.After(5 * time.Second):
			// A regression that never reaches the release must fail the test, not
			// wedge the suite.
			t.Error("insert stub was never released")
		}
		w.WriteHeader(http.StatusOK)
	}))

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.batchSize = 1
	cfg.flushInterval = 10 * time.Millisecond
	// Tight on purpose: one ~90-byte row fits, a second does not while the first
	// is still charged. Widening testConfig's rows would need this raised.
	cfg.bufferMaxBytes = 150
	s := newServer(cfg)
	s.ingest.start()
	// Registered in this order so the LIFO unwind releases the stub BEFORE the
	// drain and before the server is closed; either would otherwise block.
	defer stub.Close()
	defer s.ingest.stop()
	defer close(release)

	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"a"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("first: want 204 got %d", rr.Code)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("the worker never started the insert")
	}

	if got := s.ingest.depthBytes(); got <= 0 {
		t.Fatalf("depthBytes = %d while the insert is in flight: the batch left the budget before the flush finished", got)
	}
	// The user-visible half of the same invariant: the in-flight batch keeps the
	// gateway shedding instead of admitting a second budget's worth of events.
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"b"}`, nil); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("second: want 503 while the batch is in flight, got %d", rr.Code)
	}
}
