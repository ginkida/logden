package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// waitDraining blocks until stop() has published the drain flag, so the test can
// release an insert attempt knowing every later attempt is a post-drain one.
func waitDraining(t *testing.T, ing *ingester) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ing.draining.Load() {
		if time.Now().After(deadline) {
			t.Fatal("stop() never set draining")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// insertWithRetry used to read the drain flag once, before the loop: a batch that
// entered the loop before stop() flipped it kept the full retry schedule, which is
// the opposite of what stop() promises ("one attempt, remainder to spool"). At the
// shipped defaults those extra attempts push the drain past stop_grace_period, and
// SIGKILL mid-drain loses the rest of the buffer instead of spooling it.
//
// Every wait here has a timeout arm: a regression must fail the test, not hang the
// suite until the package timeout.
func TestDrainStopsRetriesOfAnInsertAlreadyInFlight(t *testing.T) {
	var attempts atomic.Int64
	inFlight := make(chan struct{}) // closed once attempt #1 has reached ClickHouse
	release := make(chan struct{})  // closed once stop() has set draining

	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if attempts.Add(1) == 1 {
			close(inFlight)
			select {
			case <-release:
			case <-time.After(30 * time.Second): // never wedge the suite on a failure
			}
		}
		w.WriteHeader(http.StatusInternalServerError) // retryable: the gateway would keep trying
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.batchSize = 1             // flush the event immediately, before stop() runs
	cfg.flushInterval = time.Hour // no ticker flush can race the shutdown flush
	cfg.maxRetries = 3
	cfg.spoolDir = t.TempDir() // the drained batch must land here, not be dropped
	s := newServer(cfg)
	s.ingest.start()

	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"m"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("want 204 got %d", rr.Code)
	}
	select {
	case <-inFlight:
	case <-time.After(10 * time.Second):
		t.Fatal("the first insert attempt never reached the ClickHouse stub")
	}

	stopped := make(chan struct{})
	go func() {
		s.ingest.stop()
		close(stopped)
	}()
	waitDraining(t, s.ingest)
	close(release) // attempt #1 now fails, with draining already set

	select {
	case <-stopped:
	case <-time.After(20 * time.Second):
		t.Fatal("stop() did not return: the drain is still working through retries")
	}

	if got := attempts.Load(); got != 1 {
		t.Fatalf("insert attempts = %d, want 1: an insert in flight when the drain started must not be retried", got)
	}
	if got := s.m.insertRetries.Load(); got != 0 {
		t.Fatalf("insertRetries = %d, want 0", got)
	}
	if got := s.m.spoolFiles.Load(); got != 1 {
		t.Fatalf("spoolFiles = %d, want 1 (the undelivered batch must be durable)", got)
	}
	if got := s.m.dropped.Load(); got != 0 {
		t.Fatalf("dropped = %d, want 0 (the batch is spooled, not lost)", got)
	}
}
