package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// row is one row in the column format of the logs.logs table (JSONEachRow).
// Context is stored as a JSON string; Timestamp is omitted when the client
// didn't send a valid time — then ClickHouse fills in DEFAULT now64(3).
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

// readBody decompresses gzip (with gzip-bomb protection) and caps the size.
func (s *server) readBody(r *http.Request) ([]byte, int, string) {
	limit := s.cfg.maxBodyBytes
	var src io.Reader = io.LimitReader(r.Body, limit+1)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(io.LimitReader(r.Body, limit+1))
		if err != nil {
			return nil, http.StatusBadRequest, "bad_gzip"
		}
		defer gz.Close()
		src = io.LimitReader(gz, limit+1) // cap the DECOMPRESSED size
	}
	data, err := io.ReadAll(src)
	if err != nil {
		return nil, http.StatusBadRequest, "read_error"
	}
	if int64(len(data)) > limit {
		return nil, http.StatusRequestEntityTooLarge, "too_large"
	}
	return data, 0, ""
}

// parseBatch accepts a single object, a JSON array or NDJSON and returns rows.
func (s *server) parseBatch(r *http.Request) ([]row, int, string) {
	data, code, reason := s.readBody(r)
	if code != 0 {
		return nil, code, reason
	}
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, http.StatusBadRequest, "empty"
	}

	var raws []json.RawMessage
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			return nil, http.StatusBadRequest, "bad_json"
		}
	} else {
		// Single object or NDJSON: the decoder reads a sequence of JSON values
		// separated by spaces/newlines.
		dec := json.NewDecoder(bytes.NewReader(trimmed))
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
	if len(raws) > s.cfg.maxBatchEvents {
		return nil, http.StatusRequestEntityTooLarge, "too_many_events"
	}

	// Partial accept: one broken element must not drop the whole batch (otherwise
	// a single bad line loses thousands of good ones, and the retry repeats the
	// same error).
	rows := make([]row, 0, len(raws))
	for _, raw := range raws {
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
		return nil, http.StatusBadRequest, "invalid_event"
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
		const suffix = "…[truncated]"
		if s.cfg.maxMessageBytes <= len(suffix) {
			msg = strings.ToValidUTF8(msg[:s.cfg.maxMessageBytes], "")
		} else {
			msg = strings.ToValidUTF8(msg[:s.cfg.maxMessageBytes-len(suffix)], "") + suffix
		}
		// the result is always <= maxMessageBytes
	}
	return row{
		Timestamp: normalizeTimestamp(e.Timestamp, s.cfg.retention),
		Project:   project,
		Level:     normalizeLevel(e.Level),
		Message:   msg,
		Context:   normalizeContext(e.Context, s.cfg.maxContextBytes),
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

func normalizeContext(raw json.RawMessage, max int) string {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || string(t) == "null" {
		return "{}"
	}
	if !json.Valid(t) {
		return `{"_invalid_json":true}`
	}
	if len(t) > max {
		return `{"_truncated":true,"_orig_bytes":` + strconv.Itoa(len(t)) + `}`
	}
	return string(t)
}

// normalizeTimestamp accepts RFC3339 or unix (sec/ms). Returns the time in
// ClickHouse format, or "" (then CH stamps the insert time). Junk protection:
// anything more than +5min in the future or older than the retention is dropped.
func normalizeTimestamp(raw json.RawMessage, retention time.Duration) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var t time.Time
	if raw[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return ""
		}
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(str))
		if err != nil {
			return ""
		}
		t = parsed
	} else {
		f, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return ""
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
	if t.After(now.Add(5*time.Minute)) || t.Before(now.Add(-retention)) {
		return ""
	}
	return t.Format("2006-01-02 15:04:05.000")
}
