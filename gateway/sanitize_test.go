package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// A run of invalid UTF-8 in `message` must collapse to a single U+FFFD before
// the decoder sees the element. json.Unmarshal rewrites EVERY bad byte as a
// 3-byte U+FFFD, so an unsanitized run decodes — and re-serializes — at 3x the
// bytes the inflight budget was charged for, which is how a request the caps
// admit used to push the gateway past its mem_limit.
func TestInvalidUTF8MessageDoesNotExpand(t *testing.T) {
	cases := []struct{ name, payload string }{
		{"one long run", strings.Repeat("\xff", 4000)},
		{"runs between valid text", strings.Repeat("ok\xff\xfe\xfd\xfc", 700)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := newServer(testConfig())
			// Hand-built so the bad bytes arrive RAW: going through json.Marshal
			// would sanitize exactly what is under test.
			body := `{"project":"p","message":"` + c.payload + `","context":{"k":"` + c.payload + `"}}`
			rows, code, reason := s.parseBatch(newLogsRequest(body, nil), nil)
			if code != 0 {
				t.Fatalf("parse failed: %d %s", code, reason)
			}
			if acc, drop := s.ingest.enqueue(rows, "192.0.2.1"); acc != 1 || drop != 0 {
				t.Fatalf("enqueue = %d/%d, want 1/0", acc, drop)
			}
			line := <-s.ingest.ch
			if !utf8.Valid(line) {
				t.Fatalf("serialized row is not valid UTF-8: %q", line)
			}
			// The only growth allowed is the fixed per-row overhead (timestamp,
			// level, source_ip, field names); an expansion would instead be a
			// multiple of the payload.
			if limit := len(body) + 256; len(line) > limit {
				t.Fatalf("row is %d bytes for a %d-byte request (limit %d): the invalid run expanded",
					len(line), len(body), limit)
			}
			var r row
			if err := json.Unmarshal(line, &r); err != nil {
				t.Fatalf("row is not valid JSON: %v", err)
			}
			if !utf8.ValidString(r.Message) || !utf8.ValidString(r.Context) {
				t.Fatalf("row still carries invalid UTF-8: %q / %q", r.Message, r.Context)
			}
		})
	}
}

// The same amplification at batch scale: a body of binary messages must still
// buffer within the "rows <= 2x body" budget that the BUFFER_MAX_BYTES floor —
// and with it the 128m mem_limit — is sized on.
func TestBinaryBatchStaysWithinRowBudget(t *testing.T) {
	s := newServer(testConfig())
	const events = 50
	elem := `{"project":"p","message":"` + strings.Repeat("\xff", 4100) + `"}`
	body := "[" + strings.Repeat(elem+",", events-1) + elem + "]"

	rows, code, reason := s.parseBatch(newLogsRequest(body, nil), nil)
	if code != 0 {
		t.Fatalf("parse failed: %d %s", code, reason)
	}
	if acc, drop := s.ingest.enqueue(rows, "192.0.2.1"); acc != events || drop != 0 {
		t.Fatalf("enqueue = %d/%d, want %d/0", acc, drop, events)
	}
	if got, limit := s.ingest.depthBytes(), int64(2*len(body)); got > limit {
		t.Fatalf("%d binary events buffered %d bytes for a %d-byte body (over 2x, limit %d)",
			events, got, len(body), limit)
	}
}

// The insert and readiness URLs are built as chBaseURL + "/?query=...", so any
// URL part after the host breaks the concatenation. A fragment is the worst of
// them: it swallows the whole query string, ClickHouse answers 400, and every
// batch is quarantined as .bad — so these shapes must fail at startup.
func TestClickHouseURLShapes(t *testing.T) {
	cases := []struct {
		name, url string
		wantErr   bool
	}{
		{"host and port", "http://127.0.0.1:8123", false},
		{"https host", "https://clickhouse.internal:8443", false},
		{"trailing slash", "http://127.0.0.1:8123/", false},
		{"query", "http://127.0.0.1:8123?x=1", true},
		{"bare question mark", "http://127.0.0.1:8123?", true},
		{"fragment", "http://127.0.0.1:8123#frag", true},
		{"credentials", "http://writer:pw@127.0.0.1:8123", true},
		{"user without password", "http://writer@127.0.0.1:8123", true},
		{"path", "http://127.0.0.1:8123/query", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.replayInterval = 30 * time.Second // unset in testConfig; unrelated to the URL
			cfg.chBaseURL = c.url
			err := cfg.validate()
			if c.wantErr && err == nil {
				t.Fatalf("CLICKHOUSE_URL %q was accepted", c.url)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("CLICKHOUSE_URL %q was rejected: %v", c.url, err)
			}
		})
	}
}
