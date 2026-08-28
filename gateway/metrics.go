package main

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics in Prometheus text exposition format — stdlib only, no dependencies.
// Counters are atomic; the histogram and labeled counters are guarded by a mutex.

var insertBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type metrics struct {
	received         atomic.Int64
	inserted         atomic.Int64
	dropped          atomic.Int64
	insertFailed     atomic.Int64
	insertRetries    atomic.Int64
	spoolFiles       atomic.Int64
	spoolBytes       atomic.Int64
	spoolQuarantined atomic.Int64
	chReachable      atomic.Int64

	rejected  *labeledCounter  // logden_logs_rejected_total{reason}
	truncated *labeledCounter  // logden_logs_truncated_total{field}
	restamped *labeledCounter  // logden_logs_restamped_total{reason}
	httpReqs  *labeledCounter  // logden_http_requests_total{path,code}
	insertDur *histogram       // logden_clickhouse_insert_duration_seconds
	projects  *projectCounters // logden_project_logs_{received,dropped}_total{project}

	bufferDepth      func() int
	bufferCap        func() int
	bufferBytes      func() int64
	inflightBytes    func() int64
	bufferCapBytes   int64
	inflightCapBytes int64
	spoolCapBytes    int64
	startTime        int64 // unix seconds; restart-loop alert watches changes()

	version, commit string
}

func newMetrics(version, commit, _ string) *metrics {
	return &metrics{
		rejected:      newLabeledCounter(),
		truncated:     newLabeledCounter(),
		restamped:     newLabeledCounter(),
		httpReqs:      newLabeledCounter(),
		projects:      newProjectCounters(),
		insertDur:     newHistogram(insertBuckets),
		bufferDepth:   func() int { return 0 },
		bufferCap:     func() int { return 0 },
		bufferBytes:   func() int64 { return 0 },
		inflightBytes: func() int64 { return 0 },
		startTime:     time.Now().Unix(),
		version:       version,
		commit:        commit,
	}
}

func (m *metrics) render() string {
	var b strings.Builder
	counter := func(name, help string, v int64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	gauge := func(name, help string, v int64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, v)
	}

	counter("logden_logs_received_total", "Valid log events accepted into the buffer.", m.received.Load())
	counter("logden_logs_inserted_total", "Log events successfully inserted into ClickHouse.", m.inserted.Load())
	counter("logden_logs_dropped_total", "Log events dropped (buffer or spool full).", m.dropped.Load())
	counter("logden_clickhouse_insert_failed_total", "Batch inserts that failed after all retries.", m.insertFailed.Load())
	counter("logden_clickhouse_insert_retries_total", "Batch insert retry attempts.", m.insertRetries.Load())
	gauge("logden_clickhouse_reachable", "ClickHouse passed the last readiness probe (1/0).", m.chReachable.Load())
	counter("logden_spool_quarantined_total", "Spool batches rejected by ClickHouse and quarantined as .bad files.", m.spoolQuarantined.Load())
	gauge("logden_spool_files", "Batches currently spooled to disk awaiting replay.", m.spoolFiles.Load())
	gauge("logden_spool_bytes", "Bytes on disk in the spool directory (including .bad and .tmp files).", m.spoolBytes.Load())
	gauge("logden_spool_capacity_bytes", "Configured SPOOL_MAX_BYTES (0 = unlimited).", m.spoolCapBytes)
	gauge("logden_buffer_events", "Events currently waiting in the in-memory buffer.", int64(m.bufferDepth()))
	gauge("logden_buffer_capacity", "Configured in-memory buffer capacity.", int64(m.bufferCap()))
	gauge("logden_buffer_bytes", "Bytes held by the in-memory buffer and the in-flight batch.", m.bufferBytes())
	gauge("logden_buffer_capacity_bytes", "Configured BUFFER_MAX_BYTES (0 = unlimited).", m.bufferCapBytes)
	gauge("logden_inflight_body_bytes", "Estimated bytes of request bodies being processed right now.", m.inflightBytes())
	gauge("logden_inflight_body_capacity_bytes", "Configured MAX_INFLIGHT_BODY_BYTES (0 = unlimited).", m.inflightCapBytes)
	gauge("logden_process_start_time_seconds", "Unix time the gateway process started.", m.startTime)

	b.WriteString("# HELP logden_logs_rejected_total Requests/events rejected before buffering.\n")
	b.WriteString("# TYPE logden_logs_rejected_total counter\n")
	m.rejected.render(&b, "logden_logs_rejected_total")

	b.WriteString("# HELP logden_logs_truncated_total Events whose message or context exceeded its size cap.\n")
	b.WriteString("# TYPE logden_logs_truncated_total counter\n")
	m.truncated.render(&b, "logden_logs_truncated_total")

	b.WriteString("# HELP logden_logs_restamped_total Events whose client timestamp was out of range and replaced with the ingest time.\n")
	b.WriteString("# TYPE logden_logs_restamped_total counter\n")
	m.restamped.render(&b, "logden_logs_restamped_total")

	m.projects.render(&b)

	b.WriteString("# HELP logden_http_requests_total HTTP responses by path and status code.\n")
	b.WriteString("# TYPE logden_http_requests_total counter\n")
	m.httpReqs.render(&b, "logden_http_requests_total")

	m.insertDur.render(&b, "logden_clickhouse_insert_duration_seconds")

	fmt.Fprintf(&b, "# HELP logden_build_info Build metadata.\n# TYPE logden_build_info gauge\n")
	fmt.Fprintf(&b, "logden_build_info{version=%q,commit=%q,go=%q} 1\n", m.version, m.commit, runtime.Version())
	return b.String()
}

type labeledCounter struct {
	mu   sync.Mutex
	vals map[string]int64
}

func newLabeledCounter() *labeledCounter { return &labeledCounter{vals: map[string]int64{}} }

// inc increments the counter for a ready-made label string of the form `name="value"`.
// Values must be safe literals (no " \ or newlines) — do not pass unescaped
// user input here.
func (c *labeledCounter) inc(labels string) {
	c.mu.Lock()
	c.vals[labels]++
	c.mu.Unlock()
}

func (c *labeledCounter) render(b *strings.Builder, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.vals))
	for k := range c.vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(b, "%s{%s} %d\n", name, k, c.vals[k])
	}
}

// maxProjectLabels caps how many distinct project labels the per-project
// counters may hold. 64 is far above what a single ~1GB VPS ever hosts, and the
// map costs 64 cloned names of at most 64 bytes each (see labelLocked: the clone
// is what keeps that true), but the bound has to exist: `project` comes off the
// wire, so a sender looping over generated names (a UUID per event, a typo in a
// template) would otherwise grow /metrics without limit and turn every scrape
// into an OOM vector. Everything past the cap folds into overflowProject; the
// logden_project_labels_tracked/_capacity gauges make that saturation visible.
const maxProjectLabels = 64

// overflowProject is the bucket for every project beyond the cap. It cannot
// collide with a real one: projectRe (validate.go) admits only [A-Za-z0-9._-],
// so no sender can claim this name and hide its volume inside the bucket.
const overflowProject = "<overflow>"

// projectCounters answers the two questions /metrics could not answer before
// ("who is flooding me?", "who went silent?") without a SQL round trip, at a
// bounded label cost.
//
// Label safety: keys are row.Project values, which reached a row only by
// matching projectRe in buildRow — that validation is the ONLY thing standing
// between this map and label injection, because render (like labeledCounter)
// does not escape label values. Never feed this an unvalidated string.
type projectCounters struct {
	mu       sync.Mutex
	received map[string]int64
	dropped  map[string]int64
	// tracked maps an admitted project to its canonical key; len <= maxProjectLabels.
	// The value is the string every map above is keyed by — see labelLocked.
	tracked map[string]string
}

func newProjectCounters() *projectCounters {
	return &projectCounters{
		received: map[string]int64{},
		dropped:  map[string]int64{},
		tracked:  map[string]string{},
	}
}

// observe records one request's outcome: the first `accepted` rows made it into
// the buffer, the rest were dropped. It takes the lock ONCE per request (not per
// row) — the same order of cost as the existing per-request httpReqs counter.
func (p *projectCounters) observe(rows []row, accepted int) {
	if len(rows) == 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range rows {
		label := p.labelLocked(rows[i].Project)
		if i < accepted {
			p.received[label]++
		} else {
			p.dropped[label]++
		}
	}
}

// labelLocked returns the label a project counts under, admitting it to the
// tracked set while there is room. Admission is sticky: a project that already
// owns a label keeps it even after the set fills, so a long-lived sender does
// not lose its series to a burst of garbage names.
//
// The admitted project is CLONED, and the clone is what every map is keyed by
// and what callers get back. row.Project is a strings.TrimSpace substring of the
// whole `project` field json.Unmarshal decoded, so it shares that field's
// backing array — bounded by MAX_BODY_BYTES (4 MiB), not by projectRe's 64
// characters. Storing it directly would pin megabytes per label for the life of
// the process, invisibly: these counters have no eviction, and no byte gauge
// covers them. The cap bounds the number of labels; only the clone bounds their
// bytes.
func (p *projectCounters) labelLocked(project string) string {
	if key, ok := p.tracked[project]; ok {
		return key // the canonical key, never the caller's string
	}
	if len(p.tracked) >= maxProjectLabels {
		return overflowProject
	}
	key := strings.Clone(project)
	p.tracked[key] = key
	return key
}

func (p *projectCounters) render(b *strings.Builder) {
	p.mu.Lock()
	defer p.mu.Unlock()
	renderProjectMap(b, "logden_project_logs_received_total",
		"Log events accepted into the buffer, by project (bounded label set).", p.received)
	renderProjectMap(b, "logden_project_logs_dropped_total",
		"Log events dropped instead of buffered, by project (bounded label set).", p.dropped)
	fmt.Fprintf(b, "# HELP logden_project_labels_tracked Distinct project labels held by the per-project counters.\n")
	fmt.Fprintf(b, "# TYPE logden_project_labels_tracked gauge\nlogden_project_labels_tracked %d\n", len(p.tracked))
	fmt.Fprintf(b, "# HELP logden_project_labels_capacity Label cap; past it projects fold into project=%q.\n", overflowProject)
	fmt.Fprintf(b, "# TYPE logden_project_labels_capacity gauge\nlogden_project_labels_capacity %d\n", maxProjectLabels)
}

func renderProjectMap(b *strings.Builder, name, help string, vals map[string]int64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(b, "%s{project=\"%s\"} %d\n", name, k, vals[k])
	}
}

type histogram struct {
	mu      sync.Mutex
	buckets []float64
	counts  []int64 // counts[i] = number of observations <= buckets[i] (cumulative)
	sum     float64
	count   int64
}

func newHistogram(buckets []float64) *histogram {
	return &histogram{buckets: buckets, counts: make([]int64, len(buckets))}
}

func (h *histogram) observe(v float64) {
	h.mu.Lock()
	h.sum += v
	h.count++
	for i, ub := range h.buckets {
		if v <= ub {
			h.counts[i]++
		}
	}
	h.mu.Unlock()
}

func (h *histogram) render(b *strings.Builder, name string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fmt.Fprintf(b, "# HELP %s ClickHouse batch insert duration in seconds.\n", name)
	fmt.Fprintf(b, "# TYPE %s histogram\n", name)
	for i, ub := range h.buckets {
		fmt.Fprintf(b, "%s_bucket{le=\"%s\"} %d\n", name, formatFloat(ub), h.counts[i])
	}
	fmt.Fprintf(b, "%s_bucket{le=\"+Inf\"} %d\n", name, h.count)
	fmt.Fprintf(b, "%s_sum %s\n", name, formatFloat(h.sum))
	fmt.Fprintf(b, "%s_count %d\n", name, h.count)
}

func formatFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
