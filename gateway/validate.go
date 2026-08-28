package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// chTimeLayout is ClickHouse's DateTime64(3) text format.
const chTimeLayout = "2006-01-02 15:04:05.000"

// row is one row in the column format of the logs.logs table (JSONEachRow).
// Context is stored as a JSON string; Timestamp is the client's time when it
// sent a usable one and the gateway's ingest time otherwise (omitempty is kept
// as a safety net — an empty column falls back to ClickHouse DEFAULT now64(3)).
type row struct {
	Timestamp string `json:"timestamp,omitempty"`
	Project   string `json:"project"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Context   string `json:"context"`
	SourceIP  string `json:"source_ip"`
}

type inEvent struct {
	Project   string          `json:"project"`
	Level     string          `json:"level"`
	Message   string          `json:"message"`
	Context   json.RawMessage `json:"context"`
	Timestamp json.RawMessage `json:"timestamp"`
}

var projectRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

var levelAliases = map[string]string{
	"warn": "warning", "err": "error", "fatal": "critical", "panic": "emergency", "trace": "debug",
}

// PSR-3 / syslog severities — matches the levels in queries.sql.
var allowedLevels = map[string]bool{
	"debug": true, "info": true, "notice": true, "warning": true,
	"error": true, "critical": true, "alert": true, "emergency": true,
}

// readBody decompresses gzip (with gzip-bomb protection), caps the size and
// charges the bytes it yields to the inflight budget (res may be nil).
func (s *server) readBody(r *http.Request, res *reservation) ([]byte, int, string) {
	limit := s.cfg.maxBodyBytes
	raw := &io.LimitedReader{R: r.Body, N: limit + 1} // cap the wire bytes
	var src io.Reader = raw
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(raw)
		if err != nil {
			return nil, http.StatusBadRequest, "bad_gzip"
		}
		defer gz.Close()
		src = io.LimitReader(gz, limit+1) // cap the DECOMPRESSED size
	}
	metered := res.meter(src)
	data, err := io.ReadAll(metered)
	if err != nil {
		if errors.Is(err, errOverloaded) {
			if metered.attempted > limit {
				// The body itself is over MAX_BODY_BYTES — the budget merely
				// refused the byte that proved it. Say what is actually wrong
				// (413) instead of blaming load (503).
				return nil, http.StatusRequestEntityTooLarge, "too_large"
			}
			return nil, http.StatusServiceUnavailable, reasonOverloaded
		}
		if raw.N == 0 {
			// The COMPRESSED stream ran into the cap, so the decompressor saw a
			// truncated member. That is the same user error as an oversized
			// identity body — answer 413, not a misleading 400.
			return nil, http.StatusRequestEntityTooLarge, "too_large"
		}
		return nil, http.StatusBadRequest, "read_error"
	}
	if int64(len(data)) > limit {
		return nil, http.StatusRequestEntityTooLarge, "too_large"
	}
	return data, 0, ""
}

// parseBatch accepts a single object, a JSON array or NDJSON and returns rows.
func (s *server) parseBatch(r *http.Request, res *reservation) ([]row, int, string) {
	data, code, reason := s.readBody(r, res)
	if code != 0 {
		return nil, code, reason
	}
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, http.StatusBadRequest, "empty"
	}

	var raws []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(trimmed))
	if trimmed[0] == '[' {
		// Stream the array element by element. json.Unmarshal over the whole body
		// would materialize EVERY element before the MAX_BATCH_EVENTS check below,
		// so a MAX_BODY_BYTES body of tiny elements ("[1,1,1,...]") allocates tens
		// of megabytes inside an 80MiB GOMEMLIMIT — memory the inflight semaphore
		// never accounted for, because it only charges the raw body size.
		if _, err := dec.Token(); err != nil { // opening '['
			return nil, http.StatusBadRequest, "bad_json"
		}
		for dec.More() {
			var raw json.RawMessage
			if err := dec.Decode(&raw); err != nil {
				return nil, http.StatusBadRequest, "bad_json"
			}
			raws = append(raws, raw)
			if len(raws) > s.cfg.maxBatchEvents {
				return nil, http.StatusRequestEntityTooLarge, "too_many_events"
			}
		}
		if _, err := dec.Token(); err != nil { // closing ']' — a truncated array is a 400
			return nil, http.StatusBadRequest, "bad_json"
		}
		// Nothing but whitespace may follow the array, as json.Unmarshal used to
		// enforce. dec.More() is NOT enough here: it answers false for a trailing
		// ']' or '}', so "[...]]" would slip through as valid.
		if _, err := dec.Token(); err != io.EOF {
			return nil, http.StatusBadRequest, "bad_json"
		}
	} else {
		// Single object or NDJSON: the decoder reads a sequence of JSON values
		// separated by spaces/newlines.
		for {
			var raw json.RawMessage
			err := dec.Decode(&raw)
			if err == io.EOF {
				break
			}
			if err != nil {
				// Syntactically broken input => 400 (observable), not a silent
				// loss of the tail after break. Semantically invalid (but valid
				// JSON) elements are skipped per-element below.
				return nil, http.StatusBadRequest, "bad_json"
			}
			raws = append(raws, raw)
			if len(raws) > s.cfg.maxBatchEvents {
				return nil, http.StatusRequestEntityTooLarge, "too_many_events"
			}
		}
	}

	if len(raws) == 0 {
		return nil, http.StatusBadRequest, "empty"
	}

	// Partial accept: one broken element must not drop the whole batch (otherwise
	// a single bad line loses thousands of good ones, and the retry repeats the
	// same error).
	rows := make([]row, 0, len(raws))
	for _, raw := range raws {
		if !utf8.Valid(raw) {
			// Sanitize run-wise BEFORE the decoder sees the element: json.Unmarshal
			// turns EVERY invalid byte inside a string into a 3-byte U+FFFD, so a
			// 4KB run of binary garbage in `message` decodes — and re-serializes —
			// as 12KB, three times the bytes the inflight budget was charged for
			// and past the "rows <= 2x body" budget the BUFFER_MAX_BYTES floor is
			// built on. bytes.ToValidUTF8 collapses a whole run into a single
			// replacement instead. Invalid bytes can only sit inside string
			// literals, so the element stays valid JSON, and the scan only costs a
			// copy on the rare body that is actually malformed. The bytes variant
			// is deliberate: the strings one costs three full-size copies of the
			// element (to string, into the Builder, back to RawMessage) on a path
			// whose whole point is bounding what a 4 MiB body can allocate.
			raw = bytes.ToValidUTF8(raw, []byte("�"))
		}
		var e inEvent
		if err := json.Unmarshal(raw, &e); err != nil {
			s.m.rejected.inc(`reason="invalid_event"`)
			continue
		}
		rw, ok := s.buildRow(e)
		if !ok {
			s.m.rejected.inc(`reason="invalid_event"`)
			continue
		}
		rows = append(rows, rw)
	}
	if len(rows) == 0 {
		// A distinct request-level reason: "invalid_event" is already counted once
		// per bad event above, and reusing it here would count the same request
		// twice under one label.
		return nil, http.StatusBadRequest, "all_invalid"
	}
	return rows, 0, ""
}

func (s *server) buildRow(e inEvent) (row, bool) {
	project := strings.TrimSpace(e.Project)
	if !projectRe.MatchString(project) {
		return row{}, false
	}
	if strings.TrimSpace(e.Message) == "" {
		return row{}, false
	}
	msg := e.Message
	if len(msg) > s.cfg.maxMessageBytes {
		s.m.truncated.inc(`field="message"`)
		const suffix = "…[truncated]"
		if s.cfg.maxMessageBytes <= len(suffix) {
			msg = strings.ToValidUTF8(msg[:s.cfg.maxMessageBytes], "")
		} else {
			msg = strings.ToValidUTF8(msg[:s.cfg.maxMessageBytes-len(suffix)], "") + suffix
		}
		// the result is always <= maxMessageBytes
	}
	ts, skew := normalizeEventTime(e.Timestamp, s.cfg.retention)
	if ts == "" {
		// Stamp at INGEST time instead of leaving the column to ClickHouse's
		// DEFAULT now64(3), which is applied at INSERT time: a batch that sits in
		// the spool through a ClickHouse outage would otherwise be stored with the
		// replay time, hours after the event actually happened.
		ts = time.Now().UTC().Format(chTimeLayout)
		// Overwriting a timestamp the sender DID provide is the last silent
		// data mutation in the pipeline: a fleet with a skewed clock or a
		// backfill older than RETENTION lands entirely under the ingest time and
		// still looks perfectly healthy. The switch keeps the hot path free of
		// allocation (both labels are compile-time literals, like every other
		// value handed to labeledCounter) and counts only a discarded client
		// time — an event that simply carried no timestamp is normal traffic.
		switch skew {
		case skewFuture:
			s.m.restamped.inc(`reason="future"`)
		case skewTooOld:
			s.m.restamped.inc(`reason="too_old"`)
		}
	}
	ctx, ctxTruncated := normalizeContext(e.Context, s.cfg.maxContextBytes)
	if ctxTruncated {
		s.m.truncated.inc(`field="context"`)
	}
	return row{
		Timestamp: ts,
		Project:   project,
		Level:     normalizeLevel(e.Level),
		Message:   msg,
		Context:   ctx,
	}, true
}

func normalizeLevel(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "info"
	}
	if a, ok := levelAliases[s]; ok {
		s = a
	}
	if allowedLevels[s] {
		return s
	}
	return "info"
}

// normalizeContext returns the context to store and whether it was dropped for
// exceeding MAX_CONTEXT_BYTES (the caller counts that — silent truncation is
// invisible to an operator otherwise).
func normalizeContext(raw json.RawMessage, max int) (string, bool) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || string(t) == "null" {
		return "{}", false
	}
	if !json.Valid(t) {
		return `{"_invalid_json":true}`, false
	}
	if !utf8.Valid(t) {
		// json.Valid tolerates raw invalid UTF-8 inside strings, but the row
		// encoder rewrites every such byte as the 6-byte � escape — a binary
		// context would serialize ~6x its input size and break the
		// "rows <= 2x body" budget the BUFFER_MAX_BYTES floor is built on.
		// parseBatch already sanitizes whole elements, which makes this branch
		// unreachable from /logs — it stays because normalizeContext and buildRow
		// are also called directly, and losing it there loses that 6x bound.
		t = []byte(strings.ToValidUTF8(string(t), "�"))
	}
	if len(t) > max {
		return `{"_truncated":true,"_orig_bytes":` + strconv.Itoa(len(t)) + `}`, true
	}
	return string(t), false
}

// Skew classes for a client timestamp that parsed but failed the range check.
// They exist so buildRow can count what it silently overwrites; skewNone covers
// both "nothing to report" and "nothing parseable was sent".
const (
	skewNone   = ""
	skewFuture = "future"
	skewTooOld = "too_old"
)

// normalizeTimestamp is normalizeEventTime without the skew class, for callers
// that only want the stored value.
func normalizeTimestamp(raw json.RawMessage, retention time.Duration) string {
	ts, _ := normalizeEventTime(raw, retention)
	return ts
}

// normalizeEventTime accepts RFC3339 or unix (sec/ms). Returns the time in
// ClickHouse format, or "" when the client sent nothing usable (buildRow then
// stamps the ingest time), plus the reason a well-formed client time was thrown
// away. Junk protection: anything more than +5min in the future or older than
// the retention is dropped.
func normalizeEventTime(raw json.RawMessage, retention time.Duration) (string, string) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return "", skewNone
	}
	var t time.Time
	if raw[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return "", skewNone
		}
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(str))
		if err != nil {
			return "", skewNone
		}
		t = parsed
	} else {
		f, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return "", skewNone
		}
		if f > 1e12 {
			t = time.UnixMilli(int64(f))
		} else {
			sec := int64(f)
			t = time.Unix(sec, int64((f-float64(sec))*1e9)) // keep fractional seconds
		}
	}
	t = t.UTC()
	now := time.Now().UTC()
	if t.After(now.Add(5 * time.Minute)) {
		return "", skewFuture
	}
	if t.Before(now.Add(-retention)) {
		return "", skewTooOld
	}
	return t.Format(chTimeLayout), skewNone
}
