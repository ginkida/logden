package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// The array branch must enforce MAX_BATCH_EVENTS while decoding, not after: a
// json.Unmarshal of the whole body materializes every element first, which turns
// a MAX_BODY_BYTES request of tiny elements into tens of megabytes of heap that
// the inflight semaphore never charged for.
func TestJSONArrayIsStreamedUnderBatchCap(t *testing.T) {
	cfg := testConfig()
	cfg.maxBatchEvents = 1000
	cfg.maxBodyBytes = 4 << 20
	s := newServer(cfg)

	// ~1MiB of tiny elements => ~500k events, 500x over the cap.
	body := "[" + strings.Repeat("1,", 500_000) + "1]"

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	rr := doLogs(s, "POST", "secret", body, nil)
	runtime.ReadMemStats(&after)

	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413 got %d", rr.Code)
	}
	// Streaming allocates the body plus at most maxBatchEvents+1 elements; the
	// old whole-body Unmarshal allocated ~40MiB for this input. The bound is
	// deliberately loose — it only has to separate those two orders of magnitude.
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 12<<20 {
		t.Fatalf("parsing a %d-byte array allocated %d bytes: the array path is not streaming",
			len(body), grew)
	}
}

func TestJSONArrayEdgeCases(t *testing.T) {
	cfg := testConfig()
	cfg.maxBatchEvents = 3
	s := newServer(cfg)
	cases := []struct {
		name, body string
		want       int
	}{
		{"empty array", `[]`, http.StatusBadRequest},
		{"truncated array", `[{"project":"p","message":"a"}`, http.StatusBadRequest},
		{"trailing junk after array", `[{"project":"p","message":"a"}] {"project":"p"}`, http.StatusBadRequest},
		{"trailing bracket after array", `[{"project":"p","message":"a"}]]`, http.StatusBadRequest},
		{"trailing brace after array", `[{"project":"p","message":"a"}]}`, http.StatusBadRequest},
		{"trailing comma after array", `[{"project":"p","message":"a"}],`, http.StatusBadRequest},
		{"trailing whitespace is fine", "[{\"project\":\"p\",\"message\":\"a\"}]\n ", http.StatusNoContent},
		{"nested arrays are invalid events", `[[1,2],[3]]`, http.StatusBadRequest},
		{"at the cap", `[{"project":"p","message":"a"},{"project":"p","message":"b"},{"project":"p","message":"c"}]`, http.StatusNoContent},
		{"over the cap", `[{"project":"p","message":"a"},{"project":"p","message":"b"},{"project":"p","message":"c"},{"project":"p","message":"d"}]`, http.StatusRequestEntityTooLarge},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if rr := doLogs(s, "POST", "secret", c.body, nil); rr.Code != c.want {
				t.Fatalf("want %d got %d (%s)", c.want, rr.Code, rr.Body.String())
			}
		})
	}
}

// json.Valid accepts raw invalid UTF-8 inside strings, but the row encoder turns
// every such byte into a 6-byte escape — so the context must be sanitized before
// it is measured against MAX_CONTEXT_BYTES.
func TestNormalizeContextSanitizesUTF8(t *testing.T) {
	raw := json.RawMessage("{\"a\":\"\xff\xfe\xfd\"}")
	got, _ := normalizeContext(raw, 1000)
	if !utf8.ValidString(got) {
		t.Fatalf("context still contains invalid UTF-8: %q", got)
	}
	if !json.Valid([]byte(got)) {
		t.Fatalf("sanitized context is not valid JSON: %q", got)
	}
}

func TestRowSerializationStaysWithinBudget(t *testing.T) {
	// The BUFFER_MAX_BYTES floor assumes a serialized row stays within a small
	// multiple of the raw request. The bodies below are hand-built so the bytes
	// reach the gateway UNESCAPED — going through json.Marshal would pre-escape
	// exactly what is being measured and make the assertion unfalsifiable.
	cases := []struct {
		name    string
		payload string
		maxMult int
	}{
		// Raw < > & : with HTML escaping on, each becomes a 6-byte \u00xx.
		{"html characters", strings.Repeat("<&>", 200), 2},
		{"url with ampersands", strings.Repeat("?a=1&b=2 ", 100), 2},
		// Invalid UTF-8: parseBatch now sanitizes the whole element run-wise
		// before the decoder sees it, so both `message` and `context` collapse a
		// run to one U+FFFD and the row obeys the same 2x budget as the rest.
		// Unsanitized, the decoder alone re-encoded `message` at 3 bytes per bad
		// byte and the context encoder at 6.
		{"invalid utf-8", strings.Repeat("\xff\xfe", 100), 2},
	}
	s := newServer(testConfig())
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := `{"project":"p","message":"` + c.payload + `","context":{"k":"` + c.payload + `"}}`
			rows, code, reason := s.parseBatch(newLogsRequest(body, nil), nil)
			if code != 0 {
				t.Fatalf("parse failed: %d %s", code, reason)
			}
			if acc, drop := s.ingest.enqueue(rows, "192.0.2.1"); acc != 1 || drop != 0 {
				t.Fatalf("enqueue = %d/%d, want 1/0", acc, drop)
			}
			line := <-s.ingest.ch
			if limit := c.maxMult*len(body) + 128; len(line) > limit {
				t.Fatalf("row is %d bytes for a %d-byte request (over %dx): %s",
					len(line), len(body), c.maxMult, line)
			}
		})
	}
}

func TestRowEncodingKeepsHTMLCharacters(t *testing.T) {
	s := newServer(testConfig())
	const msg = "GET /x?a=1&b=2 <script>"
	rows, code, _ := s.parseBatch(newLogsRequest(`{"project":"p","message":"`+
		strings.ReplaceAll(msg, `"`, `\"`)+`"}`, nil), nil)
	if code != 0 {
		t.Fatalf("parse failed: %d", code)
	}
	if acc, _ := s.ingest.enqueue(rows, "192.0.2.1"); acc != 1 {
		t.Fatal("enqueue failed")
	}
	line := <-s.ingest.ch
	if !bytes.Contains(line, []byte(msg)) {
		t.Fatalf("HTML characters were escaped on the wire: %s", line)
	}
}

// Truncation is data loss inside an accepted event: it must be countable, not
// only visible to whoever reads the stored row.
func TestTruncationIsCounted(t *testing.T) {
	cfg := testConfig()
	cfg.maxMessageBytes = 32
	cfg.maxContextBytes = 32
	s := newServer(cfg)

	body := `{"project":"p","message":"` + strings.Repeat("m", 200) +
		`","context":{"k":"` + strings.Repeat("c", 200) + `"}}`
	if rr := doLogs(s, "POST", "secret", body, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("want 204 got %d", rr.Code)
	}
	out := s.m.render()
	for _, want := range []string{
		`logden_logs_truncated_total{field="message"} 1`,
		`logden_logs_truncated_total{field="context"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q\n%s", want, out)
		}
	}
}

// The timestamps the shipped clients emit must all be accepted — each library
// formats "now" differently.
func TestClientTimestampFormatsAreAccepted(t *testing.T) {
	s := newServer(testConfig())
	now := time.Now().UTC()
	cases := map[string]string{
		"go RFC3339Nano":      now.Format(time.RFC3339Nano),
		"node toISOString":    now.Format("2006-01-02T15:04:05.000Z"),
		"python isoformat":    now.Format("2006-01-02T15:04:05.000000+00:00"),
		"php RFC3339_EXT":     now.Format("2006-01-02T15:04:05.000-07:00"),
		"unix seconds":        strconv.FormatInt(now.Unix(), 10),
		"unix milliseconds":   strconv.FormatInt(now.UnixMilli(), 10),
		"unix seconds w/frac": strconv.FormatFloat(float64(now.UnixNano())/1e9, 'f', 3, 64),
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			quoted := raw
			if _, err := strconv.ParseFloat(raw, 64); err != nil {
				quoted = `"` + raw + `"`
			}
			rw, ok := s.buildRow(inEvent{Project: "p", Message: "m", Timestamp: json.RawMessage(quoted)})
			if !ok {
				t.Fatalf("event rejected for timestamp %s", quoted)
			}
			got, err := time.Parse(chTimeLayout, rw.Timestamp)
			if err != nil {
				t.Fatalf("stored timestamp %q is malformed: %v", rw.Timestamp, err)
			}
			if d := got.Sub(now); d > time.Second || d < -time.Second {
				t.Fatalf("timestamp %s stored as %q (%s off) — the client's time was not preserved",
					quoted, rw.Timestamp, d)
			}
		})
	}
}

// Every row must carry a timestamp: relying on ClickHouse's DEFAULT now64(3)
// would stamp spool-replayed batches with the replay time.
func TestBuildRowAlwaysStampsTimestamp(t *testing.T) {
	s := newServer(testConfig())

	rw, ok := s.buildRow(inEvent{Project: "p", Message: "no client time"})
	if !ok {
		t.Fatal("buildRow rejected a valid event")
	}
	stamped, err := time.Parse(chTimeLayout, rw.Timestamp)
	if err != nil {
		t.Fatalf("ingest stamp %q not in ClickHouse format: %v", rw.Timestamp, err)
	}
	if d := time.Since(stamped); d < -time.Minute || d > time.Minute {
		t.Fatalf("ingest stamp %q is %s away from now", rw.Timestamp, d)
	}

	// A usable client timestamp still wins over the ingest stamp.
	clientTime := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	rw2, ok := s.buildRow(inEvent{
		Project:   "p",
		Message:   "client time",
		Timestamp: json.RawMessage(`"` + clientTime.Format(time.RFC3339Nano) + `"`),
	})
	if !ok {
		t.Fatal("buildRow rejected an event with a client timestamp")
	}
	if want := clientTime.Format(chTimeLayout); rw2.Timestamp != want {
		t.Fatalf("client timestamp = %q, want %q", rw2.Timestamp, want)
	}

	// An unusable one (older than RETENTION) falls back to the ingest stamp.
	rw3, ok := s.buildRow(inEvent{
		Project:   "p",
		Message:   "ancient",
		Timestamp: json.RawMessage(`"2000-01-01T00:00:00Z"`),
	})
	if !ok || rw3.Timestamp == "" {
		t.Fatalf("out-of-range client timestamp must fall back to the ingest stamp, got %q", rw3.Timestamp)
	}
	if stamped, err := time.Parse(chTimeLayout, rw3.Timestamp); err != nil || time.Since(stamped) > time.Minute {
		t.Fatalf("fallback stamp %q is not the ingest time", rw3.Timestamp)
	}
}
