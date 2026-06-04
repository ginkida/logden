package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// chStatusError means ClickHouse replied with an error HTTP status (as opposed
// to a transport failure, where there was no response at all).
type chStatusError struct {
	code int
	msg  string
}

func (e *chStatusError) Error() string { return fmt.Sprintf("clickhouse %d: %s", e.code, e.msg) }

// ingester is the ingest pipeline: a bounded buffer -> batch worker -> insert
// into ClickHouse with retries. When retries are exhausted the batch goes to
// the disk spool (if enabled) and is replayed later. This prevents log loss on
// brief outages/deploys and on overflow.
type ingester struct {
	cfg       config
	m         *metrics
	ch        chan []byte
	client    *http.Client
	insertURL string
	wg        sync.WaitGroup
	enqueueMu sync.Mutex
	closed    bool // under enqueueMu: channel closed, enqueue no longer sends

	spoolMu    sync.Mutex
	spoolSeq   uint64
	stopReplay chan struct{}
	draining   atomic.Bool
}

func newIngester(cfg config, m *metrics) *ingester {
	transport := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	}
	insertURL := cfg.chBaseURL + "/?" + url.Values{
		"query": {fmt.Sprintf(
			"INSERT INTO %s.%s (timestamp, project, level, message, context, source_ip) FORMAT JSONEachRow",
			cfg.chDatabase, cfg.chTable)},
		"async_insert":          {"1"},
		"wait_for_async_insert": {"1"}, // wait for the flush to be acked => we learn about errors
	}.Encode()

	return &ingester{
		cfg:        cfg,
		m:          m,
		ch:         make(chan []byte, cfg.bufferSize),
		client:     &http.Client{Timeout: 30 * time.Second, Transport: transport},
		insertURL:  insertURL,
		stopReplay: make(chan struct{}),
	}
}

func (ing *ingester) depth() int    { return len(ing.ch) }
func (ing *ingester) capacity() int { return cap(ing.ch) }

// enqueue serializes the rows, stamps source_ip and non-blockingly puts them
// into the buffer. Returns the number of accepted and dropped (buffer full) rows.
func (ing *ingester) enqueue(rows []row, ip string) (accepted, dropped int) {
	// Serialize the rows outside the lock (CPU work doesn't hold the mutex).
	lines := make([][]byte, 0, len(rows))
	for i := range rows {
		rows[i].SourceIP = ip
		if line, err := json.Marshal(&rows[i]); err == nil {
			lines = append(lines, line)
		} else {
			dropped++
		}
	}
	// All-or-nothing UNDER the mutex: the capacity check and the enqueue are
	// atomic with respect to other handlers (otherwise a batch retry would
	// duplicate already-accepted rows). The worker only reads, so free space
	// can't shrink while we hold the lock.
	ing.enqueueMu.Lock()
	defer ing.enqueueMu.Unlock()
	if ing.closed || len(ing.ch)+len(lines) > cap(ing.ch) {
		return 0, dropped + len(lines)
	}
	for _, line := range lines {
		select {
		case ing.ch <- line:
			accepted++
		default:
			dropped++
		}
	}
	return accepted, dropped
}

func (ing *ingester) start() {
	ing.wg.Add(1)
	go ing.worker()

	if ing.cfg.spoolDir != "" {
		if err := os.MkdirAll(ing.cfg.spoolDir, 0o750); err != nil {
			slog.Error("cannot create spool dir", "dir", ing.cfg.spoolDir, "err", err)
		} else {
			ing.cleanupTmp() // before worker/replay: dead .tmp from a crash between write and rename
			ing.updateSpoolGauge()
			ing.wg.Add(1)
			go ing.replayLoop()
		}
	}
}

// stop is called after the HTTP server has stopped (handlers no longer write):
// close the buffer, the worker drains the remainder and flushes, then we wait
// for completion.
func (ing *ingester) stop() {
	ing.draining.Store(true) // while draining don't get stuck in long retries — spool right away
	close(ing.stopReplay)    // stop replay BEFORE draining (no race on the directory)
	// Close the channel SYNCHRONOUSLY with enqueue: otherwise a send-on-closed-channel
	// panic (Shutdown may have timed out, leaving a slow handler inside enqueue).
	ing.enqueueMu.Lock()
	ing.closed = true
	close(ing.ch)
	ing.enqueueMu.Unlock()
	ing.wg.Wait() // wait for both worker and replayLoop
}

func (ing *ingester) worker() {
	defer ing.wg.Done()
	ticker := time.NewTicker(ing.cfg.flushInterval)
	defer ticker.Stop()

	batch := make([][]byte, 0, ing.cfg.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ing.flush(batch)
		batch = batch[:0]
	}

	for {
		select {
		case line, ok := <-ing.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, line)
			if len(batch) >= ing.cfg.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (ing *ingester) flush(batch [][]byte) {
	body := bytes.Join(batch, []byte("\n"))
	if err := ing.insertWithRetry(body); err != nil {
		ing.m.insertFailed.Add(1)
		slog.Error("insert failed after retries", "events", len(batch), "err", err)
		ing.spool(body, len(batch))
		return
	}
	ing.m.inserted.Add(int64(len(batch)))
}

func (ing *ingester) insertWithRetry(body []byte) error {
	var err error
	retries := ing.cfg.maxRetries
	if ing.draining.Load() {
		retries = 0 // shutdown: one attempt, remainder to spool (durable), don't get stuck
	}
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			ing.m.insertRetries.Add(1)
			time.Sleep(backoff)
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
		if err = ing.insertOnce(body); err == nil {
			return nil
		}
	}
	return err
}

func (ing *ingester) insertOnce(body []byte) error {
	timeout := 15 * time.Second
	if ing.draining.Load() {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ing.insertURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-ClickHouse-User", ing.cfg.chUser)
	if ing.cfg.chKey != "" {
		req.Header.Set("X-ClickHouse-Key", ing.cfg.chKey)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	start := time.Now()
	resp, err := ing.client.Do(req)
	ing.m.insertDur.observe(time.Since(start).Seconds())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &chStatusError{code: resp.StatusCode, msg: string(bytes.TrimSpace(b))}
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// --- disk spool ---

func (ing *ingester) spool(body []byte, n int) {
	if ing.cfg.spoolDir == "" {
		ing.m.dropped.Add(int64(n))
		return
	}
	if int(ing.m.spoolFiles.Load()) >= ing.cfg.spoolMaxFiles {
		ing.m.dropped.Add(int64(n))
		slog.Error("spool full, dropping batch", "events", n)
		return
	}

	ing.spoolMu.Lock()
	ing.spoolSeq++
	seq := ing.spoolSeq
	ing.spoolMu.Unlock()

	name := filepath.Join(ing.cfg.spoolDir, fmt.Sprintf("%d-%012d.ndjson", os.Getpid(), seq))
	tmp := name + ".tmp"
	if err := os.WriteFile(tmp, body, 0o640); err != nil {
		slog.Error("spool write failed", "err", err)
		ing.m.dropped.Add(int64(n))
		return
	}
	if err := os.Rename(tmp, name); err != nil {
		slog.Error("spool rename failed", "err", err)
		ing.m.dropped.Add(int64(n))
		return
	}
	ing.updateSpoolGauge()
}

func (ing *ingester) replayLoop() {
	defer ing.wg.Done()
	interval := ing.cfg.replayInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ing.stopReplay:
			return
		case <-ticker.C:
			ing.replayOnce()
		}
	}
}

func (ing *ingester) replayOnce() {
	names := ing.spoolFilesList()
	for _, name := range names {
		select {
		case <-ing.stopReplay: // shutdown: don't get stuck in a full spool sweep
			return
		default:
		}
		path := filepath.Join(ing.cfg.spoolDir, name)
		body, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if err := ing.insertOnce(body); err != nil {
			// 400 = ClickHouse parsed the request and rejected the data itself
			// (corrupt file, incompatibility after a migration). Retries are
			// useless, and an indefinite return would block the whole spool
			// behind this file — quarantine it as .bad and move on. Other errors
			// (transport, 5xx, auth) are transient/global: abort the sweep until
			// the next tick, as before.
			var se *chStatusError
			if errors.As(err, &se) && se.code == http.StatusBadRequest {
				ing.quarantine(path, name, body, err)
				continue
			}
			slog.Warn("spool replay failed, will retry later", "file", name, "err", err)
			return // ClickHouse still unavailable — wait for the next tick
		}
		n := bytes.Count(body, []byte("\n")) + 1
		ing.m.inserted.Add(int64(n))
		_ = os.Remove(path)
		ing.updateSpoolGauge()
	}
}

// quarantine removes a file rejected by ClickHouse from the replay queue by
// renaming it to *.bad (the spool's suffix filter no longer sees it). The file
// stays on disk for manual inspection — see RUNBOOK.
func (ing *ingester) quarantine(path, name string, body []byte, cause error) {
	bad := path + ".bad"
	if err := os.Rename(path, bad); err != nil {
		slog.Error("spool quarantine failed", "file", name, "err", err)
		return
	}
	n := bytes.Count(body, []byte("\n")) + 1
	ing.m.dropped.Add(int64(n)) // delivery didn't happen — this is loss, the alert should fire
	ing.m.spoolQuarantined.Add(1)
	slog.Error("spool batch rejected by clickhouse, quarantined", "file", name+".bad", "events", n, "err", cause)
	ing.updateSpoolGauge()
}

// cleanupTmp removes orphaned *.tmp files (crash between WriteFile and Rename):
// nobody replays them, and the SPOOL_MAX_FILES limit doesn't count them.
func (ing *ingester) cleanupTmp() {
	entries, err := os.ReadDir(ing.cfg.spoolDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tmp") {
			_ = os.Remove(filepath.Join(ing.cfg.spoolDir, e.Name()))
		}
	}
}

func (ing *ingester) spoolFilesList() []string {
	entries, err := os.ReadDir(ing.cfg.spoolDir)
	if err != nil {
		return nil
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

func (ing *ingester) updateSpoolGauge() {
	ing.m.spoolFiles.Store(int64(len(ing.spoolFilesList())))
}
