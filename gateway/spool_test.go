package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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

// allSpoolEntries lists the spool directory unfiltered, so a test can assert on
// the .bad/.delivered/.tmp files that spoolNames deliberately hides.
func allSpoolEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
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

// A transient failure (transport, 5xx, auth) must abort the sweep and leave the
// files for the next tick. Quarantining them instead would turn an ordinary
// ClickHouse outage — the exact situation the spool exists for — into permanent
// loss of the whole backlog, and every existing replay test only drives the 400
// path, so that regression would ship green.
func TestReplayAbortsSweepOnTransientError(t *testing.T) {
	var requests atomic.Int64
	var down atomic.Bool
	down.Store(true)
	var mu sync.Mutex
	var delivered []string
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		b, _ := io.ReadAll(r.Body)
		if down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		mu.Lock()
		delivered = append(delivered, string(b))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.spoolDir = t.TempDir()
	for name, body := range map[string]string{
		"1-000000000001.ndjson": `{"message":"first"}`,
		"1-000000000002.ndjson": `{"message":"second"}`,
	} {
		if err := os.WriteFile(filepath.Join(cfg.spoolDir, name), []byte(body), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	s := newServer(cfg) // replayOnce is driven by hand: no worker, no replayLoop

	s.ingest.replayOnce()

	if got := requests.Load(); got != 1 {
		t.Fatalf("stub saw %d inserts, want 1 (a 503 must abort the sweep, not walk the backlog)", got)
	}
	if names := spoolNames(t, cfg.spoolDir); len(names) != 2 {
		t.Fatalf("spool files after a failed sweep = %v, want both still queued", names)
	}
	for _, name := range allSpoolEntries(t, cfg.spoolDir) {
		if strings.HasSuffix(name, ".bad") {
			t.Fatalf("%s was quarantined on a transient error: the batch is lost, not retried", name)
		}
	}
	if got := s.m.spoolQuarantined.Load(); got != 0 {
		t.Fatalf("spoolQuarantined = %d, want 0", got)
	}
	if got := s.m.dropped.Load(); got != 0 {
		t.Fatalf("dropped = %d, want 0 (a transient error must not lose events)", got)
	}

	// Recovery: the very same files replay, in sequence order, once ClickHouse is
	// back — the outage/recovery cycle, not just "nothing was destroyed".
	down.Store(false)
	s.ingest.replayOnce()

	if names := spoolNames(t, cfg.spoolDir); len(names) != 0 {
		t.Fatalf("spool files after recovery = %v, want none", names)
	}
	if got := s.m.inserted.Load(); got != 2 {
		t.Fatalf("inserted = %d, want 2", got)
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{`{"message":"first"}`, `{"message":"second"}`}
	if len(delivered) != len(want) || delivered[0] != want[0] || delivered[1] != want[1] {
		t.Fatalf("replay delivered %v, want %v (FIFO by sequence)", delivered, want)
	}
}

// Same classification, but for the branch where there is no HTTP response at all:
// errors.As finds no *chStatusError, so nothing may be quarantined.
func TestReplayAbortsSweepOnTransportError(t *testing.T) {
	cfg := testConfig()
	// Refused instantly and cannot race with a reused port, unlike closing an
	// httptest server and reusing its address.
	cfg.chBaseURL = "http://127.0.0.1:1"
	cfg.spoolDir = t.TempDir()
	name := "1-000000000001.ndjson"
	if err := os.WriteFile(filepath.Join(cfg.spoolDir, name), []byte(`{"message":"x"}`), 0o640); err != nil {
		t.Fatal(err)
	}
	s := newServer(cfg)

	s.ingest.replayOnce()

	if names := spoolNames(t, cfg.spoolDir); len(names) != 1 || names[0] != name {
		t.Fatalf("spool files = %v, want %s still queued for the next tick", names, name)
	}
	if got := s.m.spoolQuarantined.Load(); got != 0 {
		t.Fatalf("spoolQuarantined = %d, want 0 (no response means no verdict on the data)", got)
	}
	if got := s.m.dropped.Load(); got != 0 {
		t.Fatalf("dropped = %d, want 0", got)
	}
}

// Replay is FIFO by parsed sequence, not by file name: the PID prefix has no
// fixed width, so a lexicographic sort lets batches written after a restart under
// a different PID jump an older backlog. The PIDs below are chosen so the two
// orders genuinely disagree (lexicographically ".keep" and "0-corrupt" come
// first, then 10-, 100-, 2-, 30-, 9-), and the foreign names exercise the
// tie-break arms: unparseable files sort last, equal sequences fall back to the
// name.
func TestSpoolFilesListIsFIFOAcrossPIDs(t *testing.T) {
	cfg := testConfig()
	cfg.spoolDir = t.TempDir()
	for _, n := range []string{
		"10-000000000003.ndjson",
		"100-000000000002.ndjson",
		"9-000000000001.ndjson",
		"30-000000000004.ndjson", // same sequence as the file below
		"2-000000000004.ndjson",
		"0-corrupt.ndjson", // foreign: the sequence field does not parse
		".keep.ndjson",     // foreign: no sequence field at all
	} {
		if err := os.WriteFile(filepath.Join(cfg.spoolDir, n), []byte("x"), 0o640); err != nil {
			t.Fatal(err)
		}
	}

	want := []string{
		"9-000000000001.ndjson",
		"100-000000000002.ndjson",
		"10-000000000003.ndjson",
		"2-000000000004.ndjson",
		"30-000000000004.ndjson",
		".keep.ndjson",
		"0-corrupt.ndjson",
	}
	got := newServer(cfg).ingest.spoolFilesList()
	if len(got) != len(want) {
		t.Fatalf("spoolFilesList() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("spoolFilesList() = %v, want %v (replay must be FIFO by sequence)", got, want)
		}
	}
}

// Every path that refuses to spool a batch must count it as dropped: these are
// the only places where the gateway loses events on purpose, and the loss is
// invisible unless logden_logs_dropped_total moves.
func TestSpoolDropPaths(t *testing.T) {
	body := []byte(`{"message":"a"}` + "\n" + `{"message":"b"}`)
	cases := []struct {
		name    string
		prepare func(t *testing.T, cfg *config, s *server)
	}{
		{"spool disabled", func(_ *testing.T, cfg *config, _ *server) {
			cfg.spoolDir = "" // SPOOL_DIR unset: durability is off, batches are dropped
		}},
		{"file count cap", func(t *testing.T, cfg *config, s *server) {
			cfg.spoolMaxFiles = 1
			s.ingest.spool([]byte("first"), 1) // fills the cap and updates the gauge
			if got := s.m.spoolFiles.Load(); got != 1 {
				t.Fatalf("setup: spoolFiles = %d, want 1", got)
			}
		}},
		{"byte cap", func(_ *testing.T, cfg *config, _ *server) {
			cfg.spoolMaxBytes = 4 // smaller than the batch below
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.spoolDir = t.TempDir()
			s := newServer(cfg)
			tc.prepare(t, &s.ingest.cfg, s)

			before := s.m.spoolFiles.Load()
			dropped := s.m.dropped.Load()
			s.ingest.spool(body, 2)

			if got := s.m.dropped.Load() - dropped; got != 2 {
				t.Fatalf("dropped delta = %d, want 2 (the whole batch is lost)", got)
			}
			if s.ingest.cfg.spoolDir == "" {
				return
			}
			s.ingest.updateSpoolGauge()
			if got := s.m.spoolFiles.Load(); got != before {
				t.Fatalf("spoolFiles = %d, want %d (nothing may be written)", got, before)
			}
			for _, name := range allSpoolEntries(t, s.ingest.cfg.spoolDir) {
				if strings.HasSuffix(name, ".tmp") {
					t.Fatalf("orphaned %s left behind: it holds the byte cap forever", name)
				}
			}
		})
	}
}

// A file the sweep cannot read must be skipped, not returned on: one unreadable
// entry would otherwise block every batch queued behind it until an operator
// notices.
func TestReplaySkipsUnreadableFile(t *testing.T) {
	var inserts atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		inserts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.spoolDir = t.TempDir()
	// A dangling symlink is an unreadable spool entry that needs no chmod, so the
	// test behaves the same for an unprivileged user and for root.
	broken := filepath.Join(cfg.spoolDir, "1-000000000001.ndjson")
	if err := os.Symlink(filepath.Join(cfg.spoolDir, "gone"), broken); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	valid := filepath.Join(cfg.spoolDir, "1-000000000002.ndjson")
	if err := os.WriteFile(valid, []byte(`{"message":"ok"}`), 0o640); err != nil {
		t.Fatal(err)
	}

	s := newServer(cfg)
	s.ingest.replayOnce()

	if got := inserts.Load(); got != 1 {
		t.Fatalf("inserts = %d, want 1 (the file behind the unreadable one must still go)", got)
	}
	if _, err := os.Stat(valid); !os.IsNotExist(err) {
		t.Fatalf("delivered file still present: %v", err)
	}
	if got := s.m.spoolQuarantined.Load(); got != 0 {
		t.Fatalf("spoolQuarantined = %d, want 0 (an unreadable file is not a rejected batch)", got)
	}
}

// A batch that WAS delivered but whose file cannot be removed must leave the
// replay set anyway: left in place it is re-inserted on every tick, which is an
// unbounded stream of duplicates in ClickHouse.
func TestDeliveredFileRetiredWhenRemoveFails(t *testing.T) {
	cfg := testConfig()
	cfg.spoolDir = t.TempDir()
	path := filepath.Join(cfg.spoolDir, "1-000000000001.ndjson")

	var inserts atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		inserts.Add(1)
		// The insert lands between the read and the remove, which is the only
		// portable place to make os.Remove fail the way a busy or read-only volume
		// would: a non-empty directory refuses removal but still renames.
		if err := os.Remove(path); err != nil {
			t.Error(err)
		}
		if err := os.Mkdir(path, 0o750); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(path, "child"), []byte("x"), 0o640); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()
	cfg.chBaseURL = stub.URL

	if err := os.WriteFile(path, []byte(`{"message":"ok"}`), 0o640); err != nil {
		t.Fatal(err)
	}
	s := newServer(cfg)

	s.ingest.replayOnce()
	s.ingest.replayOnce() // a second tick must find nothing left to replay

	if got := inserts.Load(); got != 1 {
		t.Fatalf("inserts = %d, want 1 (a delivered batch must never be replayed again)", got)
	}
	if _, err := os.Stat(path + ".delivered"); err != nil {
		t.Fatalf("undeletable file was not retired to .delivered: %v", err)
	}
	if names := spoolNames(t, cfg.spoolDir); len(names) != 0 {
		t.Fatalf("spool still lists %v after delivery", names)
	}
}

// If even the .delivered rename fails the sweep has to stop instead of looping
// over a file it can neither deliver nor retire.
func TestReplayAbortsWhenDeliveredFileCannotBeRetired(t *testing.T) {
	cfg := testConfig()
	cfg.spoolDir = t.TempDir()
	first := filepath.Join(cfg.spoolDir, "1-000000000001.ndjson")
	second := filepath.Join(cfg.spoolDir, "1-000000000002.ndjson")

	var inserts atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		// The file disappears under the sweep (an operator clearing the spool):
		// both the remove and the rename now fail with ENOENT.
		if inserts.Add(1) == 1 {
			if err := os.Remove(first); err != nil {
				t.Error(err)
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()
	cfg.chBaseURL = stub.URL

	for _, p := range []string{first, second} {
		if err := os.WriteFile(p, []byte(`{"message":"ok"}`), 0o640); err != nil {
			t.Fatal(err)
		}
	}

	s := newServer(cfg)
	s.ingest.replayOnce()

	if got := inserts.Load(); got != 1 {
		t.Fatalf("inserts = %d, want 1 (the sweep must abort, not continue blind)", got)
	}
	if names := spoolNames(t, cfg.spoolDir); len(names) != 1 || names[0] != filepath.Base(second) {
		t.Fatalf("spool files = %v, want only %s left for the next tick", names, filepath.Base(second))
	}
}

// The transport branch of retryableInsertErr: without an HTTP response there is
// no verdict on the data, so the attempt must be repeated. Only the 5xx case was
// exercised before, and both share one return.
func TestInsertRetriesOnTransportError(t *testing.T) {
	var attempts atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		// Hijack and drop the connection: the client sees a transport error, not a
		// status code, which is what an unreachable ClickHouse looks like.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = conn.Close()
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.maxRetries = 1 // one retry: the backoff starts at 200ms, keep the suite quick
	s := newServer(cfg)

	err := s.ingest.insertWithRetry([]byte(`{"project":"p","message":"x"}`))
	if err == nil {
		t.Fatal("a dropped connection must be reported as an error")
	}
	var se *chStatusError
	if errors.As(err, &se) {
		t.Fatalf("err = %v, want a transport error (no HTTP status)", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2 (1 + MAX_RETRIES)", got)
	}
	if got := s.m.insertRetries.Load(); got != 1 {
		t.Fatalf("insertRetries = %d, want 1", got)
	}
}

func TestRetryableInsertErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"transport", errors.New("dial tcp: connection refused"), true},
		{"timeout", context.DeadlineExceeded, true},
		{"bad request", &chStatusError{code: http.StatusBadRequest}, false},
		{"wrapped bad request", fmt.Errorf("insert: %w", &chStatusError{code: http.StatusBadRequest}), false},
		{"unauthorized", &chStatusError{code: http.StatusUnauthorized}, true},
		{"server error", &chStatusError{code: http.StatusInternalServerError}, true},
		{"unavailable", &chStatusError{code: http.StatusServiceUnavailable}, true},
	}
	for _, tc := range cases {
		if got := retryableInsertErr(tc.err); got != tc.want {
			t.Errorf("retryableInsertErr(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
}
