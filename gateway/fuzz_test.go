package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// Fuzz targets over the untrusted-input surface. Every /logs request reaches
// readBody + parseBatch + buildRow as attacker-controlled bytes, so these
// targets assert INVARIANTS (what has to hold for any input at all) instead of
// outputs, which a mutator is free to change.
//
// Cost matters here: `go test` runs the seed corpus of every target on every
// run. So the targets keep no state between iterations, touch no network, and
// write nothing to disk (SPOOL_DIR stays empty in fuzzConfig).

// truncSuffix mirrors the constant inside buildRow. MAX_MESSAGE_BYTES >= 16
// (config.go) exists so it always fits.
const truncSuffix = "…[truncated]"

// The sentinels normalizeContext substitutes for a value it will not store
// verbatim.
const (
	ctxEmpty       = "{}"
	ctxInvalidJSON = `{"_invalid_json":true}`
	ctxTruncPrefix = `{"_truncated":true,"_orig_bytes":`
)

// parseReasons is every rejection reason readBody/parseBatch can return.
// Keeping it as an explicit allowlist is the point of the assertion below:
// reject() hands the reason straight to labeledCounter.inc, which does NOT
// escape label values, so the day a reason starts carrying request-controlled
// text /metrics breaks. A fuzzer that reached an unlisted reason fails here
// first.
var parseReasons = map[string]bool{
	"bad_gzip":        true,
	"read_error":      true,
	"too_large":       true,
	reasonOverloaded:  true,
	"empty":           true,
	"bad_json":        true,
	"too_many_events": true,
	"all_invalid":     true,
}

// fuzzConfig is a deliberately SMALL testConfig: every cap sits low enough that
// a mutator reaches each rejection path (413 too_large, 413 too_many_events,
// message and context truncation) with inputs of a few hundred bytes, which
// keeps one exec cheap. BUFFER_MAX_BYTES stays unlimited so a batch the parser
// accepted is always admissible — checkFraming asserts exactly that, and a byte
// budget would turn it into a flaky size test.
func fuzzConfig() config {
	cfg := testConfig()
	// Filled in so this is a config the gateway would actually boot with;
	// FuzzParseBatch asserts cfg.validate() passes.
	cfg.chBaseURL = "http://127.0.0.1:8123"
	cfg.replayInterval = 30 * time.Second
	cfg.bufferSize = 128 // > maxBatchEvents: every parsed batch fits
	cfg.bufferMaxBytes = 0
	cfg.maxBodyBytes = 4 << 10
	cfg.maxInflightBytes = cfg.maxBodyBytes // admission control on: meteredReader in the path
	cfg.maxMessageBytes = 64
	cfg.maxContextBytes = 256
	cfg.maxBatchEvents = 16
	return cfg
}

// checkRow asserts everything a row must satisfy before it can be handed to
// ClickHouse, whatever the request said.
func checkRow(t *testing.T, cfg config, rw row) {
	t.Helper()
	if !projectRe.MatchString(rw.Project) {
		t.Fatalf("project %q does not match the accepted charset", rw.Project)
	}
	if strings.TrimSpace(rw.Message) == "" {
		// Truncation must never empty a message buildRow already accepted — that
		// is what the MAX_MESSAGE_BYTES >= 16 floor buys.
		t.Fatalf("message is blank after normalization (max=%d)", cfg.maxMessageBytes)
	}
	if !utf8.ValidString(rw.Message) {
		t.Fatalf("message is not valid UTF-8: %q", rw.Message)
	}
	if len(rw.Message) > cfg.maxMessageBytes {
		t.Fatalf("message is %d bytes, over MAX_MESSAGE_BYTES=%d: %q",
			len(rw.Message), cfg.maxMessageBytes, rw.Message)
	}
	if !allowedLevels[rw.Level] {
		t.Fatalf("level %q is outside the PSR-3 set queries.sql knows about", rw.Level)
	}
	if !json.Valid([]byte(rw.Context)) {
		t.Fatalf("context is not valid JSON: %q", rw.Context)
	}
	if !utf8.ValidString(rw.Context) {
		t.Fatalf("context is not valid UTF-8: %q", rw.Context)
	}
	if len(rw.Context) > cfg.maxContextBytes && !strings.HasPrefix(rw.Context, ctxTruncPrefix) {
		t.Fatalf("context is %d bytes, over MAX_CONTEXT_BYTES=%d: %q",
			len(rw.Context), cfg.maxContextBytes, rw.Context)
	}
	// Never left to ClickHouse's DEFAULT now64(3): a spool-replayed batch would
	// then be stored with the replay time.
	if _, err := time.Parse(chTimeLayout, rw.Timestamp); err != nil {
		t.Fatalf("timestamp %q is not in the ClickHouse DateTime64(3) layout: %v", rw.Timestamp, err)
	}
}

// checkFraming pins the JSONEachRow framing invariant, which nothing else in the
// suite pins: ingester.flush joins the serialized rows with "\n" (ingest.go) and
// the spool counts the events back with bytes.Count(body, "\n")+1. ONE raw
// newline inside a row therefore corrupts the framing of the whole batch —
// ClickHouse misparses every following row and the spool's event accounting
// silently drifts. Row content is user input; only the encoder's escaping keeps
// this true.
func checkFraming(t *testing.T, s *server, rows []row) {
	t.Helper()
	accepted, dropped := s.ingest.enqueue(rows, "192.0.2.1")
	if accepted != len(rows) || dropped != 0 {
		t.Fatalf("enqueue = %d accepted / %d dropped for %d rows: the buffer refused a parsed batch",
			accepted, dropped, len(rows))
	}
	lines := make([][]byte, 0, accepted)
	for i := 0; i < accepted; i++ {
		line := <-s.ingest.ch
		if bytes.IndexByte(line, '\n') >= 0 {
			t.Fatalf("serialized row contains a raw newline, which splits the JSONEachRow batch: %q", line)
		}
		if !json.Valid(line) {
			t.Fatalf("serialized row is not valid JSON: %q", line)
		}
		var back row
		if err := json.Unmarshal(line, &back); err != nil {
			t.Fatalf("serialized row does not decode: %v (%q)", err, line)
		}
		if back.Message != rows[i].Message || back.Context != rows[i].Context {
			t.Fatalf("row did not round-trip through the encoder:\n got msg=%q ctx=%q\nwant msg=%q ctx=%q",
				back.Message, back.Context, rows[i].Message, rows[i].Context)
		}
		lines = append(lines, line)
	}
	// The exact body flush() builds, framed back the way the spool counts it.
	body := bytes.Join(lines, []byte("\n"))
	if n := bytes.Count(body, []byte("\n")) + 1; n != len(lines) {
		t.Fatalf("a batch of %d rows frames as %d events", len(lines), n)
	}
	for i, part := range bytes.Split(body, []byte("\n")) {
		if !json.Valid(part) {
			t.Fatalf("batch line %d is not a standalone JSON object: %q", i, part)
		}
	}
}

// fuzzGzip reuses one gzip.Writer across iterations: a fresh writer allocates the
// whole deflate state per call and would dominate the fuzz loop. The mutex is
// free while uncontended and keeps the helper correct if the engine ever runs
// inputs concurrently.
var fuzzGzip gzipper

type gzipper struct {
	mu  sync.Mutex
	buf bytes.Buffer
	w   *gzip.Writer
}

func (g *gzipper) compress(p []byte) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.buf.Reset()
	if g.w == nil {
		g.w = gzip.NewWriter(&g.buf)
	} else {
		g.w.Reset(&g.buf)
	}
	_, _ = g.w.Write(p)
	_ = g.w.Close()
	return g.buf.String()
}

// jsonQuote escapes s into the body of a JSON string WITHOUT touching invalid
// UTF-8. json.Marshal would rewrite every bad byte as U+FFFD and sanitize away
// exactly the input the parse path has to survive — parse_test.go and
// sanitize_test.go hand-build their bodies for the same reason.
func jsonQuote(s string) string {
	const hex = "0123456789abcdef"
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c < 0x20: // the only bytes JSON forbids raw inside a string
			b.WriteString(`\u00`)
			b.WriteByte(hex[c>>4])
			b.WriteByte(hex[c&0xf])
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// FuzzParseBatch drives whole request bodies through readBody + parseBatch +
// buildRow the way a real POST /logs does, then through enqueue so the
// serialized rows are checked too. mode selects the transfer encoding.
func FuzzParseBatch(f *testing.F) {
	cfg := fuzzConfig()
	if err := cfg.validate(); err != nil {
		f.Fatalf("the fuzz config is not one the gateway would boot with: %v", err)
	}
	addParseSeeds(f, cfg)

	f.Fuzz(func(t *testing.T, body []byte, mode uint8) {
		payload := string(body)
		var hdr map[string]string
		switch mode % 3 {
		case 1:
			// A well-formed member: exercises the decompressor and the cap on the
			// DECOMPRESSED size (a gzip bomb costs what it inflates to).
			payload = fuzzGzip.compress(body)
			hdr = map[string]string{"Content-Encoding": "gzip"}
		case 2:
			// Declared gzip over arbitrary bytes: the bad_gzip and truncated-member
			// paths, which a compressor can never produce.
			hdr = map[string]string{"Content-Encoding": "gzip"}
		}

		s := newServer(cfg)
		res := s.newReservation()
		defer res.release()
		rows, code, reason := s.parseBatch(newLogsRequest(payload, hdr), res)

		if code != 0 {
			if len(rows) != 0 {
				t.Fatalf("rejected with %d %s but still returned %d rows", code, reason, len(rows))
			}
			if !parseReasons[reason] {
				t.Fatalf("rejection reason %q is not in the metric label allowlist", reason)
			}
			switch code {
			case http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusServiceUnavailable:
			default:
				t.Fatalf("unexpected rejection status %d (%s)", code, reason)
			}
			return
		}

		if reason != "" {
			t.Fatalf("accepted but carries reason %q", reason)
		}
		if len(rows) == 0 {
			t.Fatal("accepted with zero rows: the handler would answer 204 for nothing")
		}
		if len(rows) > cfg.maxBatchEvents {
			t.Fatalf("%d rows accepted, over MAX_BATCH_EVENTS=%d", len(rows), cfg.maxBatchEvents)
		}
		for _, rw := range rows {
			checkRow(t, cfg, rw)
		}
		checkFraming(t, s, rows)
	})
}

func addParseSeeds(f *testing.F, cfg config) {
	now := time.Now().UTC()
	bodies := []string{
		// --- shapes the existing suite already knows are interesting ---
		`{"project":"p","message":"m"}`,
		`{"project":"billing","level":"error","message":"x","context":{"a":1}}`,
		`[{"project":"p","message":"a"},{"project":"p","message":"b"}]`,
		`[{"project":"p","message":"a"}`,                  // truncated array
		`[{"project":"p","message":"a"}] {"project":"p"}`, // trailing garbage
		`[{"project":"p","message":"a"}]]`,                // trailing bracket
		`[{"project":"p","message":"a"}]}`,                // trailing brace
		`[{"project":"p","message":"a"}],`,                // trailing comma
		"[{\"project\":\"p\",\"message\":\"a\"}]\n ",      // trailing whitespace is legal
		`[]`,
		`[[1,2],[3]]`,
		"{\"project\":\"p\",\"message\":\"a\"}\n{\"project\":\"p\",\"message\":\"b\"}", // NDJSON
		"{\"project\":\"p\",\"message\":\"a\"}\nNOTJSON",
		`{not json`,
		``,
		"   \t\r\n",
		`{"project":"a b","message":"x"}`, // project charset
		`{"project":"p"}`,                 // no message
		`{"message":"x"}`,                 // no project
		`{"project":"p","message":"   "}`, // blank message
		`{"project":"` + strings.Repeat("p", 65) + `","message":"m"}`,
		`null`, `[null]`, `123`, `"str"`, `true`,
		// --- levels: aliases, unknown, wrong type ---
		`{"project":"p","level":"WARN","message":"m"}`,
		`{"project":"p","level":"nonsense","message":"m"}`,
		`{"project":"p","level":123,"message":"m"}`,
		`{"project":"p","level":"  Fatal  ","message":"m"}`,
		// --- invalid UTF-8 runs in message and context (sanitize_test.go) ---
		`{"project":"p","message":"` + strings.Repeat("\xff", 200) + `"}`,
		`{"project":"p","message":"` + strings.Repeat("ok\xff\xfe\xfd\xfc", 40) +
			`","context":{"k":"` + strings.Repeat("\xff\xfe", 40) + `"}}`,
		// --- huge unicode and the escapes the encoder treats specially ---
		`{"project":"p","message":"` + strings.Repeat("\U0001F600", 40) + `"}`,
		// U+2028/U+2029 are escaped by the encoder even with SetEscapeHTML(false),
		// the 2x row growth the BUFFER_MAX_BYTES floor is sized against. They also
		// count as whitespace, so the message needs a non-space character or the
		// event is rejected as blank before the encoder ever sees it.
		`{"project":"p","message":"m` + "\u2028\u2029" + `","context":{"k":"` + "\u2028" + `"}}`,
		`{"project":"p","message":"\ud800 lone surrogate"}`,
		`{"project":"p","message":"<script>&amp; /x?a=1&b=2"}`,
		// A 4-byte rune straddling MAX_MESSAGE_BYTES: the cut must not store half
		// a rune.
		`{"project":"p","message":"` + strings.Repeat("a", cfg.maxMessageBytes-len(truncSuffix)-1) +
			"\U0001F600" + strings.Repeat("b", 40) + `"}`,
		// --- framing: a RAW newline inside the context's JSON is legal input ---
		"{\"project\":\"p\",\"message\":\"m\",\"context\":{\"a\":\n1,\"b\":\"line\\nbreak\"}}",
		"[{\"project\":\"p\",\"message\":\"m\",\"context\":{\n\"a\":1}},\n{\"project\":\"p\",\"message\":\"m2\"}]",
		// --- context shapes, including deep nesting (nothing may recurse on it) ---
		`{"project":"p","message":"m","context":{"a":` + strings.Repeat(`{"b":`, 20) + "1" + strings.Repeat("}", 20) + `}}`,
		`{"project":"p","message":"m","context":` + strings.Repeat("[", 300) + strings.Repeat("]", 300) + `}`,
		`{"project":"p","message":"m","context":"` + strings.Repeat("c", cfg.maxContextBytes+100) + `"}`,
		`{"project":"p","message":"m","context":null}`,
		`{"project":"p","message":"m","context":` + ctxTruncPrefix + `9}}`, // a client faking the sentinel
		// --- timestamps: the non-finite forms JSON does allow, plus both boundaries ---
		`{"project":"p","message":"m","timestamp":1e400}`,
		`{"project":"p","message":"m","timestamp":-1e400}`,
		`{"project":"p","message":"m","timestamp":1e308}`,
		`{"project":"p","message":"m","timestamp":NaN}`, // not JSON at all
		`{"project":"p","message":"m","timestamp":1000000000000}`,
		`{"project":"p","message":"m","timestamp":1000000000001}`, // the sec/ms switch
		`{"project":"p","message":"m","timestamp":"` + now.Format(time.RFC3339Nano) + `"}`,
		`{"project":"p","message":"m","timestamp":"` + now.Add(5*time.Minute-time.Second).Format(time.RFC3339Nano) + `"}`,
		`{"project":"p","message":"m","timestamp":"` + now.Add(5*time.Minute+time.Second).Format(time.RFC3339Nano) + `"}`,
		`{"project":"p","message":"m","timestamp":"` + now.Add(-cfg.retention+time.Minute).Format(time.RFC3339Nano) + `"}`,
		`{"project":"p","message":"m","timestamp":"` + now.Add(-cfg.retention-time.Minute).Format(time.RFC3339Nano) + `"}`,
		`{"project":"p","message":"m","timestamp":` + strconv.FormatInt(now.UnixMilli(), 10) + `}`,
		// --- the caps ---
		`[` + strings.Repeat(`{"project":"p","message":"a"},`, cfg.maxBatchEvents) + `{"project":"p","message":"a"}]`,
		`{"project":"p","message":"` + strings.Repeat("a", int(cfg.maxBodyBytes)+100) + `"}`,
	}
	for _, b := range bodies {
		f.Add([]byte(b), uint8(0))
	}
	// gzip: mode 1 wraps the input in a real member, mode 2 declares gzip over
	// bytes that are not one.
	f.Add([]byte(`[{"project":"p","message":"a"}]`), uint8(1))
	f.Add([]byte(`{"project":"p","message":"`+strings.Repeat("a", int(cfg.maxBodyBytes)+100)+`"}`), uint8(1))
	f.Add([]byte(`{"project":"p","message":"m"}`), uint8(2))
	f.Add([]byte("\x1f\x8b\x08\x00\x00\x00\x00\x00"), uint8(2)) // header only, member truncated
	f.Add([]byte{}, uint8(1))
}

// FuzzNormalizeTimestamp fuzzes the raw JSON token a client puts in
// `timestamp`. The invariant is the accept window, not the value: whatever comes
// back must be empty (buildRow then stamps ingest time) or a ClickHouse-layout
// timestamp inside [now-RETENTION, now+5min]. The overflowing float conversions
// are the interesting part — int64(1e300) is implementation-defined in Go, and
// whatever it yields still has to land outside the window.
func FuzzNormalizeTimestamp(f *testing.F) {
	retention := fuzzConfig().retention
	now := time.Now().UTC()
	// The boundary seeds are relative to now by nature; the assertion is windowed,
	// so an input landing on either side of a boundary passes either way.
	for _, s := range []string{
		`"` + now.Format(time.RFC3339Nano) + `"`,
		`"` + now.Format("2006-01-02T15:04:05.000Z") + `"`,
		`"` + now.Format("2006-01-02T15:04:05.000000+00:00") + `"`,
		`"` + now.Add(5*time.Minute-time.Second).Format(time.RFC3339Nano) + `"`,
		`"` + now.Add(5*time.Minute+time.Second).Format(time.RFC3339Nano) + `"`,
		`"` + now.Add(-retention+time.Minute).Format(time.RFC3339Nano) + `"`,
		`"` + now.Add(-retention-time.Minute).Format(time.RFC3339Nano) + `"`,
		strconv.FormatInt(now.Unix(), 10),
		strconv.FormatInt(now.UnixMilli(), 10),
		strconv.FormatFloat(float64(now.UnixNano())/1e9, 'f', 3, 64),
		"", " ", "null", "0", "-0", "-1", "true", "[1]", `{"a":1}`, `""`, `"   "`,
		"1000000000000", "1000000000001", // the seconds/milliseconds switch
		"1e400", "-1e400", "1e308", "-1e308", "1e12", "9223372036854775807",
		"-9223372036854775808", "0.5", "1e-400", "NaN", "Infinity",
		`"not a time"`, `"2026-13-45T99:99:99Z"`, `"0001-01-01T00:00:00Z"`,
		`"9999-12-31T23:59:59Z"`, `"1970-01-01T00:00:00Z"`,
		`"` + now.Format(time.RFC3339Nano) + ` "`,
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, raw string) {
		before := time.Now().UTC()
		got := normalizeTimestamp(json.RawMessage(raw), retention)
		after := time.Now().UTC()
		if got == "" {
			return // buildRow stamps the ingest time instead
		}
		if len(got) != len(chTimeLayout) {
			// A year outside [1000,9999] would widen the column text, and
			// ClickHouse rejects the whole batch over one unparseable value.
			t.Fatalf("timestamp %q is %d bytes, not the fixed DateTime64(3) width", got, len(got))
		}
		ts, err := time.Parse(chTimeLayout, got)
		if err != nil {
			t.Fatalf("timestamp %q does not parse in the ClickHouse layout: %v", got, err)
		}
		// One second of slack: Format truncates to milliseconds and `now` moved
		// while the call was running.
		if ts.After(after.Add(5*time.Minute + time.Second)) {
			t.Fatalf("timestamp %q is further ahead than the +5min guard allows", got)
		}
		if ts.Before(before.Add(-retention - time.Second)) {
			t.Fatalf("timestamp %q is older than RETENTION: it would land in a partition the TTL already dropped", got)
		}
	})
}

// FuzzNormalizeContext fuzzes the `context` value together with the cap it is
// measured against. Two branches are only reachable from here: /logs hands
// normalizeContext a json.RawMessage the decoder already validated, so the
// invalid-JSON and invalid-UTF-8 sentinels exist for direct callers — which is
// exactly why they need a fuzzer of their own.
func FuzzNormalizeContext(f *testing.F) {
	seeds := []string{
		"", " ", "\t\r\n", "null", "{}", "[]", `{"a":1}`, `[1,2,3]`, `"string"`, "123", "true",
		`{"a":`, `{"a":}`, `{`, `}`, `{"a":1}{"b":2}`, `1 2`, // invalid JSON: the sentinel branch
		"{\"a\":\n1}", "{\n\"a\":\t1\r\n}", // raw newlines between tokens are legal JSON
		"{\"a\":\"line\\nbreak\"}",
		"{\"a\":\"\xff\xfe\xfd\"}",                        // invalid UTF-8 inside a string
		"{\"\xff\":1}",                                    // invalid UTF-8 inside a key
		"{\"a\":\"" + strings.Repeat("\xff", 300) + "\"}", // a long run: sanitizing must not blow up the size
		`{"a":"` + "\u2028\u2029" + `"}`,                  // always escaped by the encoder (2x growth)
		`{"a":"` + strings.Repeat("\U0001F600", 60) + `"}`,
		`{"a":"\ud800"}`,                             // lone surrogate escape
		`{"a":1e400}`, `{"a":-1e400}`, `{"a":1e308}`, // non-finite numbers
		`{"a":` + strings.Repeat("0", 400) + `}`, // absurd number literal
		ctxInvalidJSON, ctxTruncPrefix + `9}`,    // a client faking the sentinels
		strings.Repeat(`{"a":`, 300) + "1" + strings.Repeat("}", 300),
		// Past encoding/json's 10000-frame nesting limit: this has to come back as
		// invalid, never be recursed into.
		strings.Repeat("[", 10001) + strings.Repeat("]", 10001),
		`{"k":"` + strings.Repeat("v", 4096) + `"}`,
	}
	for _, s := range seeds {
		f.Add([]byte(s), 256)
	}
	f.Add([]byte(`{"a":1}`), 1)
	f.Add([]byte(`{"a":1}`), 7)

	f.Fuzz(func(t *testing.T, raw []byte, max int) {
		// MAX_CONTEXT_BYTES > 0 is a config invariant (config.go); the modulo keeps
		// an arbitrary int inside a range a real deployment could have.
		max = 1 + int(uint(max)%4096)

		got, truncated := normalizeContext(json.RawMessage(raw), max)
		if !json.Valid([]byte(got)) {
			t.Fatalf("stored context is not valid JSON: %q", got)
		}
		if !utf8.ValidString(got) {
			t.Fatalf("stored context is not valid UTF-8: %q", got)
		}
		if truncated && !strings.HasPrefix(got, ctxTruncPrefix) {
			t.Fatalf("reported as truncated but stored %q", got)
		}
		if len(got) > max && !truncated && got != ctxEmpty && got != ctxInvalidJSON {
			// The sentinels are the only outputs allowed past the cap (a tiny
			// MAX_CONTEXT_BYTES cannot hold them); a passed-through value is not.
			t.Fatalf("stored context is %d bytes for a %d-byte cap: %q", len(got), max, got)
		}

		// The stored context keeps the client's raw JSON bytes, which may
		// legitimately contain a newline between tokens. The row encoder has to
		// escape it, or the batch that joins rows with "\n" loses its framing.
		s := newServer(fuzzConfig())
		checkFraming(t, s, []row{{
			Timestamp: time.Now().UTC().Format(chTimeLayout),
			Project:   "p",
			Level:     "info",
			Message:   "m",
			Context:   got,
		}})
	})
}

// FuzzMessageTruncation drives arbitrary message bytes through the real parse
// path at an arbitrary MAX_MESSAGE_BYTES. Unlike FuzzParseBatch, whose random
// bodies rarely form a valid event, every iteration here reaches buildRow's
// truncation branch, so the byte-wise cut and its broken-rune cleanup get the
// coverage.
func FuzzMessageTruncation(f *testing.F) {
	long := strings.Repeat("a", 300)
	for _, s := range []string{
		"", " ", "\t\n ", "m", long,
		strings.Repeat("\U0001F600", 100),             // 4-byte runes: the cut lands mid-rune
		strings.Repeat("a", 17) + "\U0001F600" + long, // ...right at the suffix boundary
		strings.Repeat("\xff", 300),                   // one invalid run
		strings.Repeat("ok\xff\xfe\xfd\xfc", 60),      // runs between valid text
		strings.Repeat(" ", 100) + "x",                // truncation must not empty it
		"line\nbreak\ttab\r\n",                        // control characters
		"\u2028\u2029" + long,                         // always escaped by the encoder
		`quotes " and \ backslashes`,
		"<script>&amp; /x?a=1&b=2 " + long,
		truncSuffix + long, // a client sending the marker itself
	} {
		f.Add(s, 64)
		f.Add(s, 16) // the MAX_MESSAGE_BYTES floor: the suffix exactly fits
	}

	f.Fuzz(func(t *testing.T, msg string, limit int) {
		cfg := fuzzConfig()
		// MAX_MESSAGE_BYTES >= 16 is a config invariant: below it the suffix does
		// not fit and truncation could empty an accepted message.
		cfg.maxMessageBytes = 16 + int(uint(limit)%512)
		// The body cap is FuzzParseBatch's subject, not this one's.
		cfg.maxBodyBytes = 1 << 20
		cfg.maxInflightBytes = cfg.maxBodyBytes

		body := `{"project":"p","message":"` + jsonQuote(msg) + `"}`
		if len(body) > int(cfg.maxBodyBytes) {
			return
		}

		// Model what parseBatch hands to buildRow: the element is sanitized
		// run-wise first, then decoded.
		elem := body
		if !utf8.ValidString(elem) {
			elem = strings.ToValidUTF8(elem, "�")
		}
		var e inEvent
		if err := json.Unmarshal([]byte(elem), &e); err != nil {
			t.Fatalf("the test built a body that is not JSON: %v (%q)", err, elem)
		}
		want := e.Message

		s := newServer(cfg)
		rows, code, reason := s.parseBatch(newLogsRequest(body, nil), nil)

		if strings.TrimSpace(want) == "" {
			// A blank message is the one thing buildRow refuses outright, and this
			// batch holds nothing else.
			if code != http.StatusBadRequest || reason != "all_invalid" {
				t.Fatalf("blank message %q gave %d %s, want 400 all_invalid", want, code, reason)
			}
			return
		}
		if code != 0 {
			t.Fatalf("valid event rejected: %d %s (message %d bytes, cap %d)",
				code, reason, len(want), cfg.maxMessageBytes)
		}
		if len(rows) != 1 {
			t.Fatalf("one event produced %d rows", len(rows))
		}
		checkRow(t, cfg, rows[0])

		got := rows[0].Message
		if len(want) <= cfg.maxMessageBytes {
			if got != want {
				t.Fatalf("message was rewritten though it fit in %d bytes:\n got %q\nwant %q",
					cfg.maxMessageBytes, got, want)
			}
		} else {
			if !strings.HasSuffix(got, truncSuffix) {
				t.Fatalf("truncated message %q does not carry the %q marker: the loss is invisible downstream",
					got, truncSuffix)
			}
			if head := strings.TrimSuffix(got, truncSuffix); !strings.HasPrefix(want, head) {
				t.Fatalf("truncated message is not a prefix of the original:\nhead %q\norig %q", head, want)
			}
		}
		checkFraming(t, s, rows)
	})
}
