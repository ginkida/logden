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
	"strconv"
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

// maxBatchBytes flushes a batch early by SIZE (not only by count): it caps the
// bytes.Join copy and the INSERT body even when individual rows are large.
const maxBatchBytes = 8 << 20

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
	// bufBytes = bytes admitted into the buffer and not yet flushed (the
	// worker's in-flight batch included). Incremented under enqueueMu,
	// decremented by the worker AFTER a flush completes — so buffer + batch
	// together stay under BUFFER_MAX_BYTES.
	bufBytes atomic.Int64

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

func (ing *ingester) depth() int        { return len(ing.ch) }
func (ing *ingester) capacity() int     { return cap(ing.ch) }
func (ing *ingester) depthBytes() int64 { return ing.bufBytes.Load() }

// enqueue serializes the rows, stamps source_ip and non-blockingly puts them
// into the buffer. Returns the number of accepted and dropped (buffer full) rows.
func (ing *ingester) enqueue(rows []row, ip string) (accepted, dropped int) {
	// Serialize the rows outside the lock (CPU work doesn't hold the mutex).
	// SetEscapeHTML(false): the default encoder rewrites every < > & as a 6-byte
	// \u00xx escape, so a URL-heavy message ("?a=1&b=2") would inflate the row —
	// and the byte budget it consumes — far past the raw request it came from.
	// ClickHouse decodes both spellings to the same string.
	lines := make([][]byte, 0, len(rows))
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	var total int64
	for i := range rows {
		rows[i].SourceIP = ip
		buf.Reset()
		if err := enc.Encode(&rows[i]); err != nil {
			dropped++
			continue
		}
		b := buf.Bytes()
		b = b[:len(b)-1] // Encode appends a newline; JSONEachRow lines are joined below
		line := make([]byte, len(b))
		copy(line, b) // buf is reused on the next iteration
		lines = append(lines, line)
		total += int64(len(line))
	}
	// All-or-nothing UNDER the mutex: the capacity check and the enqueue are
	// atomic with respect to other handlers (otherwise a batch retry would
	// duplicate already-accepted rows). The worker only reads, so free space
	// can't shrink while we hold the lock. The byte budget bounds buffer
	// memory even when events are near MAX_MESSAGE/CONTEXT_BYTES (the event
	// count alone would admit BUFFER_SIZE × ~130KB).
	ing.enqueueMu.Lock()
	defer ing.enqueueMu.Unlock()
	if ing.closed || len(ing.ch)+len(lines) > cap(ing.ch) ||
		(ing.cfg.bufferMaxBytes > 0 && ing.bufBytes.Load()+total > ing.cfg.bufferMaxBytes) {
		return 0, dropped + len(lines)
	}
	// Count only the bytes actually admitted: the worker decrements what it
	// dequeues, so charging the budget for a line the channel refused would leak
	// it forever. (The capacity check above makes the default branch unreachable —
	// this keeps the invariant true even if that ever changes.)
	var admitted int64
	for _, line := range lines {
		select {
		case ing.ch <- line:
			accepted++
			admitted += int64(len(line))
		default:
			dropped++
		}
	}
	ing.bufBytes.Add(admitted)
	return accepted, dropped
}

func (ing *ingester) start() {
	// The spool directory is prepared BEFORE the worker starts: the worker may
	// spool its first batch immediately, and it must never do that with an
	// unseeded sequence number (see resumeSpoolSeq).
	spoolReady := false
	if ing.cfg.spoolDir != "" {
		if err := os.MkdirAll(ing.cfg.spoolDir, 0o750); err != nil {
			slog.Error("cannot create spool dir", "dir", ing.cfg.spoolDir, "err", err)
		} else {
			ing.cleanupTmp() // dead .tmp from a crash between write and rename
			ing.resumeSpoolSeq()
			ing.updateSpoolGauge()
			spoolReady = true
		}
	}

	ing.wg.Add(1)
	go ing.worker()

	if spoolReady {
		ing.wg.Add(1)
		go ing.replayLoop()
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
	var batchBytes int64
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ing.flush(batch)
		// The batch has left memory (inserted, spooled or dropped) — only now
		// release its bytes from the budget, so the buffer and the in-flight
		// batch stay under BUFFER_MAX_BYTES together.
		ing.bufBytes.Add(-batchBytes)
		batch, batchBytes = batch[:0], 0
	}

	for {
		select {
		case line, ok := <-ing.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, line)
			batchBytes += int64(len(line))
			if len(batch) >= ing.cfg.batchSize || batchBytes >= maxBatchBytes {
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
			// Re-read the flag instead of trusting the snapshot above: stop() flips
			// it while this batch may already be inside the loop, and that batch
			// would then keep the full retry schedule (~10s at the defaults) even
			// though the drain promises a single attempt. Straight to the spool —
			// the remainder has to be durable before stop_grace_period runs out,
			// otherwise SIGKILL lands mid-drain and the events are lost outright.
			if ing.draining.Load() {
				break
			}
			ing.m.insertRetries.Add(1)
			time.Sleep(backoff)
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
		if err = ing.insertOnce(body); err == nil {
			return nil
		}
		if !retryableInsertErr(err) {
			return err
		}
	}
	return err
}

// retryableInsertErr reports whether another attempt could plausibly succeed.
// HTTP 400 means ClickHouse parsed the request and rejected the DATA itself (a
// bad row, a schema mismatch): the retry re-sends identical bytes and fails
// identically, while the backoff stalls the worker (~1.4s at the default
// MAX_RETRIES) and lets the buffer fill with events that would then be dropped.
// Straight to the spool instead, where replayOnce quarantines it as .bad.
func retryableInsertErr(err error) bool {
	var se *chStatusError
	if errors.As(err, &se) {
		return se.code != http.StatusBadRequest
	}
	return true // transport error, timeout, 5xx, auth — may recover
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
	// Byte cap on top of the file-count cap: quarantined .bad files also hold
	// disk space and are counted, so a poisoned spool can't fill the volume.
	if ing.cfg.spoolMaxBytes > 0 && ing.m.spoolBytes.Load()+int64(len(body)) > ing.cfg.spoolMaxBytes {
		ing.m.dropped.Add(int64(n))
		slog.Error("spool byte cap reached, dropping batch",
			"events", n, "spool_bytes", ing.m.spoolBytes.Load(), "cap", ing.cfg.spoolMaxBytes)
		return
	}

	ing.spoolMu.Lock()
	ing.spoolSeq++
	seq := ing.spoolSeq
	ing.spoolMu.Unlock()

	name := filepath.Join(ing.cfg.spoolDir, spoolFileName(seq))
	tmp := name + ".tmp"
	if err := writeFileSync(tmp, body); err != nil {
		slog.Error("spool write failed", "err", err)
		_ = os.Remove(tmp)
		ing.m.dropped.Add(int64(n))
		return
	}
	if err := os.Rename(tmp, name); err != nil {
		slog.Error("spool rename failed", "err", err)
		_ = os.Remove(tmp) // nobody replays a .tmp — don't leak it into the byte cap
		ing.m.dropped.Add(int64(n))
		return
	}
	syncDir(ing.cfg.spoolDir) // the rename itself must be durable, not just the bytes
	ing.updateSpoolGauge()
}

// syncDir fsyncs a directory so a rename into it survives a power loss. Best
// effort: not every filesystem supports it, and a spooled batch is still better
// off written than refused because the sync failed.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

// spoolFileName builds "<pid>-<seq>.ndjson". Replay order comes from the parsed
// sequence, not from the name (the PID prefix has no fixed width and would
// dominate a lexicographic sort after a restart under a different PID).
func spoolFileName(seq uint64) string {
	return fmt.Sprintf("%d-%012d.ndjson", os.Getpid(), seq)
}

// writeFileSync writes the batch and fsyncs it before the rename. The spool is
// the durability path (it only fills when ClickHouse is down), so the data must
// survive a power loss, not just a process crash: without the sync the rename
// can be visible while the contents are still an empty or partial file.
func writeFileSync(path string, body []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(body); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// resumeSpoolSeq continues the sequence from what is already on disk. File names
// carry the PID, but in Docker the gateway is always PID 1: after a restart with
// an unreplayed spool, a fresh counter would regenerate the very same names and
// os.Rename would silently overwrite batches that were never delivered.
func (ing *ingester) resumeSpoolSeq() {
	entries, err := os.ReadDir(ing.cfg.spoolDir)
	if err != nil {
		// Unreadable spool dir: the counter stays at 0 and the next spooled batch
		// could reuse a name. Loud, because this is silent data loss otherwise.
		slog.Error("cannot read spool dir, sequence not resumed", "dir", ing.cfg.spoolDir, "err", err)
		return
	}
	var maxSeq uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if seq, ok := parseSpoolSeq(e.Name()); ok && seq > maxSeq {
			maxSeq = seq
		}
	}
	ing.spoolMu.Lock()
	if maxSeq > ing.spoolSeq {
		ing.spoolSeq = maxSeq
	}
	ing.spoolMu.Unlock()
	if maxSeq > 0 {
		slog.Info("spool sequence resumed", "dir", ing.cfg.spoolDir, "seq", maxSeq)
	}
}

// parseSpoolSeq extracts <seq> from "<pid>-<seq>.ndjson" and from its .bad/.tmp
// variants — a quarantined file still owns its number.
func parseSpoolSeq(name string) (uint64, bool) {
	_, rest, ok := strings.Cut(name, "-")
	if !ok {
		return 0, false
	}
	digits, _, ok := strings.Cut(rest, ".")
	if !ok {
		return 0, false
	}
	seq, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
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
	// One rescan per sweep, not per file: the gauge does a ReadDir plus a stat per
	// entry, and draining a 1000-file backlog would otherwise cost a million
	// syscalls on a box whose whole point is being small.
	defer ing.updateSpoolGauge()

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
			// Undeliverable and invisible otherwise: nothing else reports a spool
			// file that cannot be read.
			slog.Error("spool file unreadable, skipping", "file", name, "err", err)
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
		if err := os.Remove(path); err != nil {
			// The batch IS delivered. Leaving the file in place would re-insert it
			// on every tick forever (unbounded duplicates), so get it out of the
			// replay set; if even that fails, stop the sweep instead of looping.
			slog.Error("spool file delivered but not removed", "file", name, "err", err)
			if err := os.Rename(path, path+".delivered"); err != nil {
				slog.Error("spool file cannot be retired, aborting sweep", "file", name, "err", err)
				return
			}
		}
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
	// The gauge is refreshed once per sweep by replayOnce, the only caller.
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
	// FIFO by sequence number, which resumeSpoolSeq keeps monotonic across
	// restarts. Sorting the names as strings would order by the variable-width
	// PID prefix first, so after a restart fresh batches could jump the backlog.
	sort.Slice(names, func(i, j int) bool {
		si, oki := parseSpoolSeq(names[i])
		sj, okj := parseSpoolSeq(names[j])
		if oki && okj && si != sj {
			return si < sj
		}
		if oki != okj {
			return oki // unparseable names (foreign files) go last
		}
		return names[i] < names[j]
	})
	return names
}

// updateSpoolGauge refreshes both spool gauges: the replayable file count
// (*.ndjson only, as before) and the total bytes of ALL files in the spool dir
// (.ndjson + .bad + .tmp) — the byte cap must see everything that holds disk.
func (ing *ingester) updateSpoolGauge() {
	entries, err := os.ReadDir(ing.cfg.spoolDir)
	if err != nil {
		return
	}
	var files, bytesTotal int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".ndjson") {
			files++
		}
		if info, err := e.Info(); err == nil {
			bytesTotal += info.Size()
		}
	}
	ing.m.spoolFiles.Store(files)
	ing.m.spoolBytes.Store(bytesTotal)
}
