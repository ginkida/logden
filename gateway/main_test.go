package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig() config {
	return config{
		listenAddr:      ":0",
		tokens:          []string{"secret", "secret2"},
		chDatabase:      "logs",
		chTable:         "logs",
		bufferSize:      100,
		batchSize:       500,
		flushInterval:   time.Hour,
		maxRetries:      0,
		spoolMaxFiles:   100,
		maxBodyBytes:    4 << 20,
		maxMessageBytes: 64 << 10,
		maxContextBytes: 64 << 10,
		maxBatchEvents:  1000,
		retention:       30 * 24 * time.Hour,
		logLevel:        slog.LevelError,
	}
}

func doLogs(s *server, method, token, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/logs", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, req)
	return rr
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func TestAuthAndMethod(t *testing.T) {
	s := newServer(testConfig())
	cases := []struct {
		name, method, token, body string
		hdr                       map[string]string
		want                      int
	}{
		{"GET not allowed", "GET", "secret", "", nil, http.StatusMethodNotAllowed},
		{"no token", "POST", "", `{"project":"p","message":"m"}`, nil, http.StatusUnauthorized},
		{"wrong token", "POST", "nope", `{"project":"p","message":"m"}`, nil, http.StatusUnauthorized},
		{"first token", "POST", "secret", `{"project":"p","message":"m"}`, nil, http.StatusNoContent},
		{"second token (rotation)", "POST", "secret2", `{"project":"p","message":"m"}`, nil, http.StatusNoContent},
		{"X-Log-Token header", "POST", "", `{"project":"p","message":"m"}`, map[string]string{"X-Log-Token": "secret"}, http.StatusNoContent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rr := doLogs(s, c.method, c.token, c.body, c.hdr)
			if rr.Code != c.want {
				t.Fatalf("want %d got %d (%s)", c.want, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestValidation(t *testing.T) {
	cfg := testConfig()
	cfg.maxBatchEvents = 2
	s := newServer(cfg)
	cases := []struct {
		name, body string
		want       int
	}{
		{"valid single", `{"project":"billing","level":"error","message":"x","context":{"a":1}}`, http.StatusNoContent},
		{"missing project", `{"message":"x"}`, http.StatusBadRequest},
		{"missing message", `{"project":"p"}`, http.StatusBadRequest},
		{"bad project chars", `{"project":"a b","message":"x"}`, http.StatusBadRequest},
		{"bad json", `{not json`, http.StatusBadRequest},
		{"json array", `[{"project":"p","message":"a"},{"project":"p","message":"b"}]`, http.StatusNoContent},
		{"too many events", `[{"project":"p","message":"a"},{"project":"p","message":"b"},{"project":"p","message":"c"}]`, http.StatusRequestEntityTooLarge},
		{"ndjson", "{\"project\":\"p\",\"message\":\"a\"}\n{\"project\":\"p\",\"message\":\"b\"}", http.StatusNoContent},
		{"ndjson with garbage line", "{\"project\":\"p\",\"message\":\"a\"}\nNOTJSON", http.StatusBadRequest},
		{"empty body", ``, http.StatusBadRequest},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rr := doLogs(s, "POST", "secret", c.body, nil)
			if rr.Code != c.want {
				t.Fatalf("want %d got %d (%s)", c.want, rr.Code, rr.Body.String())
			}
		})
	}
}

func TestBodyTooLarge(t *testing.T) {
	cfg := testConfig()
	cfg.maxBodyBytes = 16
	s := newServer(cfg)
	rr := doLogs(s, "POST", "secret", `{"project":"p","message":"this body is way over sixteen bytes"}`, nil)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413 got %d", rr.Code)
	}
}

func TestGzip(t *testing.T) {
	s := newServer(testConfig())

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte(`{"project":"p","message":"gzipped"}`))
	_ = gz.Close()

	req := httptest.NewRequest("POST", "/logs", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Encoding", "gzip")
	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, req)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("gzip valid: want 204 got %d", rr.Code)
	}

	// невалидный gzip
	req2 := httptest.NewRequest("POST", "/logs", strings.NewReader("not gzip"))
	req2.Header.Set("Authorization", "Bearer secret")
	req2.Header.Set("Content-Encoding", "gzip")
	rr2 := httptest.NewRecorder()
	s.mux().ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusBadRequest {
		t.Fatalf("gzip invalid: want 400 got %d", rr2.Code)
	}
}

func TestBufferFull(t *testing.T) {
	cfg := testConfig()
	cfg.bufferSize = 1
	s := newServer(cfg) // воркер не запущен — никто не вычитывает буфер
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"a"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("first: want 204 got %d", rr.Code)
	}
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"b"}`, nil); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("overflow: want 503 got %d", rr.Code)
	}
	if s.m.dropped.Load() == 0 {
		t.Fatal("expected dropped metric > 0")
	}
}

func TestInsertPipeline(t *testing.T) {
	received := make(chan []byte, 1)
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		select {
		case received <- b:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.batchSize = 1
	cfg.flushInterval = 10 * time.Millisecond
	s := newServer(cfg)
	s.ingest.start()
	defer s.ingest.stop()

	rr := doLogs(s, "POST", "secret", `{"project":"p","level":"error","message":"hi","context":{"a":1}}`, nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("want 204 got %d", rr.Code)
	}

	select {
	case b := <-received:
		var m map[string]any
		if err := json.Unmarshal(b, &m); err != nil {
			t.Fatalf("bad JSONEachRow: %v (%s)", err, b)
		}
		if m["project"] != "p" || m["level"] != "error" || m["message"] != "hi" {
			t.Fatalf("unexpected row: %s", b)
		}
		if m["context"] != `{"a":1}` {
			t.Fatalf("context should be stored as JSON string, got %v", m["context"])
		}
		if _, hasTS := m["timestamp"]; hasTS {
			t.Fatalf("timestamp should be omitted when client sends none: %s", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ClickHouse stub did not receive insert")
	}
	waitFor(t, time.Second, func() bool { return s.m.inserted.Load() == 1 })
}

func TestRetryAndSpoolReplay(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var inserts atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		inserts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.batchSize = 1
	cfg.flushInterval = 10 * time.Millisecond
	cfg.maxRetries = 1
	cfg.spoolDir = t.TempDir()
	s := newServer(cfg)
	s.ingest.start()
	defer s.ingest.stop()

	doLogs(s, "POST", "secret", `{"project":"p","message":"x"}`, nil)

	// ClickHouse "лежит": после ретраев батч уходит в спул
	waitFor(t, 4*time.Second, func() bool { return s.m.spoolFiles.Load() >= 1 })
	if s.m.insertFailed.Load() == 0 {
		t.Fatal("expected insertFailed > 0")
	}

	// ClickHouse "поднялся": переигрываем спул
	fail.Store(false)
	s.ingest.replayOnce()
	waitFor(t, 4*time.Second, func() bool { return s.m.spoolFiles.Load() == 0 })
	if inserts.Load() == 0 {
		t.Fatal("replay did not insert spooled batch")
	}
}

func TestSpoolQuarantine(t *testing.T) {
	// Стаб ClickHouse: батчи со словом "poison" отвергает 400 (как при
	// несовместимости схемы/битом файле), остальные принимает.
	var inserts atomic.Int64
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if bytes.Contains(b, []byte("poison")) {
			http.Error(w, "Cannot parse input", http.StatusBadRequest)
			return
		}
		inserts.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	cfg.spoolDir = t.TempDir()
	s := newServer(cfg)

	// Ядовитый файл в голове очереди не должен блокировать валидный за ним.
	poison := filepath.Join(cfg.spoolDir, "1-000000000001.ndjson")
	valid := filepath.Join(cfg.spoolDir, "1-000000000002.ndjson")
	if err := os.WriteFile(poison, []byte(`{"message":"poison"}`), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(valid, []byte(`{"message":"ok"}`), 0o640); err != nil {
		t.Fatal(err)
	}

	s.ingest.replayOnce()

	if _, err := os.Stat(poison + ".bad"); err != nil {
		t.Fatalf("poison file not quarantined to .bad: %v", err)
	}
	if got := s.m.spoolQuarantined.Load(); got != 1 {
		t.Fatalf("spoolQuarantined = %d, want 1", got)
	}
	if got := s.m.dropped.Load(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
	if got := inserts.Load(); got != 1 {
		t.Fatalf("valid batch behind poison not replayed: inserts = %d, want 1", got)
	}
	if got := s.m.spoolFiles.Load(); got != 0 {
		t.Fatalf("spoolFiles = %d, want 0", got)
	}
}

func TestSpoolTmpCleanup(t *testing.T) {
	cfg := testConfig()
	cfg.spoolDir = t.TempDir()
	stale := filepath.Join(cfg.spoolDir, "1-000000000001.ndjson.tmp")
	if err := os.WriteFile(stale, []byte("x"), 0o640); err != nil {
		t.Fatal(err)
	}

	s := newServer(cfg)
	s.ingest.start()
	s.ingest.stop()

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale .tmp not cleaned up at start: %v", err)
	}
}

func TestClientIPTrustedProxies(t *testing.T) {
	cfg := testConfig()
	s := newServer(cfg)
	req := httptest.NewRequest("POST", "/logs", nil)
	req.RemoteAddr = "10.0.0.5:1234"
	req.Header.Set("X-Forwarded-For", "1.2.3.4")

	if got := s.clientIP(req); got != "10.0.0.5" {
		t.Fatalf("untrusted XFF must be ignored, got %q", got)
	}

	tp, _ := parseCIDRs("10.0.0.0/8")
	s.cfg.trustedProxies = tp
	if got := s.clientIP(req); got != "1.2.3.4" {
		t.Fatalf("trusted proxy XFF must be used, got %q", got)
	}
}

func TestNormalizeLevel(t *testing.T) {
	cases := map[string]string{
		"":      "info",
		"INFO":  "info",
		"warn":  "warning",
		"err":   "error",
		"fatal": "critical",
		"panic": "emergency",
		"trace": "debug",
		"bogus": "info",
		"error": "error",
	}
	for in, want := range cases {
		if got := normalizeLevel(in); got != want {
			t.Errorf("normalizeLevel(%q)=%q want %q", in, got, want)
		}
	}
}

func TestNormalizeContext(t *testing.T) {
	if got := normalizeContext(nil, 1000); got != "{}" {
		t.Errorf("empty context => %q", got)
	}
	if got := normalizeContext(json.RawMessage(`{"a":1}`), 1000); got != `{"a":1}` {
		t.Errorf("valid context => %q", got)
	}
	if got := normalizeContext(json.RawMessage(`{bad`), 1000); !strings.Contains(got, "_invalid_json") {
		t.Errorf("invalid context => %q", got)
	}
	if got := normalizeContext(json.RawMessage(`{"a":"`+strings.Repeat("x", 100)+`"}`), 10); !strings.Contains(got, "_truncated") {
		t.Errorf("oversized context => %q", got)
	}
}

func TestNormalizeTimestamp(t *testing.T) {
	if got := normalizeTimestamp(nil, time.Hour); got != "" {
		t.Errorf("nil ts => %q", got)
	}
	if got := normalizeTimestamp(json.RawMessage(`"2020-01-01T00:00:00Z"`), 365*24*time.Hour*100); got == "" {
		t.Errorf("valid RFC3339 should parse")
	}
	// слишком старое относительно ретеншна — отбрасываем
	if got := normalizeTimestamp(json.RawMessage(`"2000-01-01T00:00:00Z"`), time.Hour); got != "" {
		t.Errorf("too old ts must be dropped, got %q", got)
	}
}

func TestSecureEqual(t *testing.T) {
	if !secureEqual("abc", "abc") {
		t.Error("equal strings")
	}
	if secureEqual("abc", "abd") || secureEqual("abc", "abcd") {
		t.Error("unequal strings")
	}
}

func TestPartialBatch(t *testing.T) {
	s := newServer(testConfig())
	// валидный, null (битый), валидный — должны пройти 2 валидных
	rr := doLogs(s, "POST", "secret", `[{"project":"p","message":"a"},null,{"project":"p","message":"b"}]`, nil)
	if rr.Code != http.StatusNoContent {
		t.Fatalf("partial batch: want 204 got %d (%s)", rr.Code, rr.Body.String())
	}
	if got := s.m.received.Load(); got != 2 {
		t.Fatalf("want 2 accepted, got %d", got)
	}
	// полностью битый батч -> 400
	if rr2 := doLogs(s, "POST", "secret", `[null,123]`, nil); rr2.Code != http.StatusBadRequest {
		t.Fatalf("all-invalid batch: want 400 got %d", rr2.Code)
	}
}

func TestMessageTruncationBounded(t *testing.T) {
	for _, max := range []int{5, 13, 14, 20, 100} {
		cfg := testConfig()
		cfg.maxMessageBytes = max
		s := newServer(cfg)
		rw, ok := s.buildRow(inEvent{Project: "p", Message: strings.Repeat("x", 200)})
		if !ok {
			t.Fatalf("max=%d: buildRow failed", max)
		}
		if len(rw.Message) > max {
			t.Fatalf("max=%d: truncated len %d exceeds limit", max, len(rw.Message))
		}
	}
}

func TestMetricsRender(t *testing.T) {
	s := newServer(testConfig())
	s.m.received.Add(3)
	out := s.m.render()
	for _, want := range []string{
		"logden_logs_received_total 3",
		"logden_build_info{",
		"logden_clickhouse_insert_duration_seconds_bucket",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q\n%s", want, out)
		}
	}
}

func TestSecurityHeaders(t *testing.T) {
	s := newServer(testConfig())
	req := httptest.NewRequest("GET", "/healthz", nil)
	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, req)
	if rr.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Error("missing nosniff header")
	}
}
