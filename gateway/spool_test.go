package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func spoolNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".ndjson") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

// A restart must not reuse sequence numbers: inside Docker the gateway is always
// PID 1, so an unseeded counter would regenerate the previous run's file names
// and os.Rename would silently overwrite batches that were never delivered.
func TestSpoolSeqResumedAfterRestart(t *testing.T) {
	cfg := testConfig()
	cfg.spoolDir = t.TempDir()

	first := newServer(cfg)
	first.ingest.start()
	first.ingest.spool([]byte(`{"project":"p","message":"old"}`), 1)
	first.ingest.stop()

	before := spoolNames(t, cfg.spoolDir)
	if len(before) != 1 {
		t.Fatalf("first run should have spooled 1 file, got %v", before)
	}

	// Same spool volume, same PID: a second process lifetime.
	second := newServer(cfg)
	second.ingest.start()
	second.ingest.spool([]byte(`{"project":"p","message":"new"}`), 1)
	second.ingest.stop()

	after := spoolNames(t, cfg.spoolDir)
	if len(after) != 2 {
		t.Fatalf("restart overwrote an unreplayed spool file: %v -> %v", before, after)
	}
	body, err := os.ReadFile(filepath.Join(cfg.spoolDir, before[0]))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "old") {
		t.Fatalf("pre-restart batch was clobbered: %q", body)
	}
	// FIFO order must survive the restart: the older batch still sorts first.
	if after[0] != before[0] {
		t.Fatalf("replay order broken after restart: %v", after)
	}
	if got := second.m.spoolFiles.Load(); got != 2 {
		t.Fatalf("spoolFiles = %d, want 2", got)
	}
}

// A quarantined .bad file keeps its sequence number reserved, so the counter must
// resume past it too.
func TestSpoolSeqResumedPastQuarantinedFile(t *testing.T) {
	cfg := testConfig()
	cfg.spoolDir = t.TempDir()
	reserved := spoolFileName(7) + ".bad"
	if err := os.WriteFile(filepath.Join(cfg.spoolDir, reserved), []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}

	s := newServer(cfg)
	s.ingest.start()
	s.ingest.spool([]byte("y"), 1)
	s.ingest.stop()

	names := spoolNames(t, cfg.spoolDir)
	if len(names) != 1 || names[0] != spoolFileName(8) {
		t.Fatalf("spool file = %v, want %s (seq resumed past the .bad file)", names, spoolFileName(8))
	}
}

func TestParseSpoolSeq(t *testing.T) {
	cases := map[string]uint64{
		"1-000000000005.ndjson":     5,
		"1-000000000005.ndjson.bad": 5,
		"1-000000000005.ndjson.tmp": 5,
		"4711-000000000042.ndjson":  42,
	}
	for name, want := range cases {
		got, ok := parseSpoolSeq(name)
		if !ok || got != want {
			t.Errorf("parseSpoolSeq(%q) = %d,%v want %d,true", name, got, ok, want)
		}
	}
	for _, name := range []string{"noseq.ndjson", "1-notanumber.ndjson", "1-5", ""} {
		if _, ok := parseSpoolSeq(name); ok {
			t.Errorf("parseSpoolSeq(%q) should not parse", name)
		}
	}
}

// A failed rename must not leave a .tmp behind: nothing replays it, but it does
// occupy the SPOOL_MAX_BYTES budget forever.
func TestSpoolTmpRemovedOnRenameFailure(t *testing.T) {
	cfg := testConfig()
	cfg.spoolDir = t.TempDir()
	s := newServer(cfg)

	// Occupy the target name with a directory — renaming a file onto it fails.
	blocker := filepath.Join(cfg.spoolDir, spoolFileName(1))
	if err := os.Mkdir(blocker, 0o750); err != nil {
		t.Fatal(err)
	}

	s.ingest.spool([]byte("x"), 3)

	if _, err := os.Stat(blocker + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("orphaned .tmp left after a failed rename: %v", err)
	}
	if got := s.m.dropped.Load(); got != 3 {
		t.Fatalf("dropped = %d, want 3 (the batch was not spooled)", got)
	}
}

// SIGTERM must not lose what is already buffered: the worker drains the channel
// and flushes the remainder to ClickHouse.
func TestShutdownDrainsBufferToClickHouse(t *testing.T) {
	var rows atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rows.Add(int64(bytes.Count(b, []byte("\n")) + 1))
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.batchSize = 500           // never reached
	cfg.flushInterval = time.Hour // only the shutdown drain can flush
	s := newServer(cfg)
	s.ingest.start()

	for i := 0; i < 3; i++ {
		if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"m"}`, nil); rr.Code != http.StatusNoContent {
			t.Fatalf("event %d: want 204 got %d", i, rr.Code)
		}
	}
	if got := s.m.inserted.Load(); got != 0 {
		t.Fatalf("nothing should be inserted before the drain, got %d", got)
	}

	s.ingest.stop()

	if got := rows.Load(); got != 3 {
		t.Fatalf("ClickHouse received %d rows on drain, want 3", got)
	}
	if got := s.m.inserted.Load(); got != 3 {
		t.Fatalf("inserted = %d, want 3", got)
	}
	if got := s.ingest.depthBytes(); got != 0 {
		t.Fatalf("byte budget not released after the drain: %d", got)
	}
}

// If ClickHouse is down during shutdown the remainder must land in the spool
// (durable) instead of being dropped.
func TestShutdownDrainsToSpoolWhenClickHouseIsDown(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.batchSize = 500
	cfg.flushInterval = time.Hour
	cfg.maxRetries = 5 // draining must skip the retries, not wait for them
	cfg.spoolDir = t.TempDir()
	s := newServer(cfg)
	s.ingest.start()

	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"m"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("want 204 got %d", rr.Code)
	}

	start := time.Now()
	s.ingest.stop()
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("drain took %s: retries are not disabled while draining", elapsed)
	}

	if names := spoolNames(t, cfg.spoolDir); len(names) != 1 {
		t.Fatalf("spool files after drain = %v, want 1", names)
	}
	if got := s.m.dropped.Load(); got != 0 {
		t.Fatalf("dropped = %d, want 0 (the batch is spooled, not lost)", got)
	}
}

// ClickHouse answering 400 means the DATA was rejected — retrying re-sends the
// same bytes and only stalls the worker. Transport/5xx errors must still retry.
func TestInsertNoRetryOnBadRequest(t *testing.T) {
	var attempts atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		http.Error(w, "Cannot parse input format", http.StatusBadRequest)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.maxRetries = 3
	s := newServer(cfg)

	if err := s.ingest.insertWithRetry([]byte(`{"project":"p","message":"bad"}`)); err == nil {
		t.Fatal("a 400 must be reported as an error")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1 (rejected data must not be retried)", got)
	}
	if got := s.m.insertRetries.Load(); got != 0 {
		t.Fatalf("insertRetries = %d, want 0", got)
	}
}

func TestInsertRetriesOnServerError(t *testing.T) {
	var attempts atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.maxRetries = 2
	s := newServer(cfg)

	if err := s.ingest.insertWithRetry([]byte(`{"project":"p","message":"x"}`)); err == nil {
		t.Fatal("a 500 must be reported as an error after retries")
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("attempts = %d, want 3 (1 + MAX_RETRIES)", got)
	}
	if got := s.m.insertRetries.Load(); got != 2 {
		t.Fatalf("insertRetries = %d, want 2", got)
	}
}
