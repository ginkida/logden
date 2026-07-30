package main

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Every response must be counted, not only the ones from /logs.
func TestHTTPRequestCounterCoversAllPaths(t *testing.T) {
	s := newServer(testConfig())
	h := s.mux()
	get := func(path string) {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
	}
	get("/healthz")
	get("/version")
	get("/definitely-not-a-route")
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"m"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("logs: want 204 got %d", rr.Code)
	}
	if rr := doLogs(s, "POST", "wrong", `{"project":"p","message":"m"}`, nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("logs: want 401 got %d", rr.Code)
	}

	out := s.m.render()
	for _, want := range []string{
		`logden_http_requests_total{path="/healthz",code="200"} 1`,
		`logden_http_requests_total{path="/version",code="200"} 1`,
		`logden_http_requests_total{path="other",code="404"} 1`,
		`logden_http_requests_total{path="/logs",code="204"} 1`,
		`logden_http_requests_total{path="/logs",code="401"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q\n%s", want, out)
		}
	}
}

// An unknown path must not become a metric label: labeledCounter does not escape
// label values, and the cardinality would be attacker-controlled.
func TestUnknownPathsShareOneLabel(t *testing.T) {
	s := newServer(testConfig())
	h := s.mux()
	for _, p := range []string{`/a"b`, "/x", "/y", "/z"} {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", p, nil))
	}
	out := s.m.render()
	if !strings.Contains(out, `logden_http_requests_total{path="other",code="404"} 4`) {
		t.Errorf("unknown paths must collapse into one label\n%s", out)
	}
	if strings.Contains(out, `/a"b`) {
		t.Error("a raw request path leaked into a metric label")
	}
}

// An oversized COMPRESSED body is the same user error as an oversized identity
// body and must be answered 413, not 400.
func TestGzipCompressedBodyTooLarge(t *testing.T) {
	cfg := testConfig()
	cfg.maxBodyBytes = 512
	s := newServer(cfg)

	// Incompressible payload: the compressed stream is what overflows the cap.
	payload := make([]byte, 8<<10)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() <= int(cfg.maxBodyBytes) {
		t.Fatalf("test payload compressed to %d bytes, expected more than the %d limit", buf.Len(), cfg.maxBodyBytes)
	}

	req := httptest.NewRequest("POST", "/logs", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Encoding", "gzip")
	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, req)
	if rr.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413 got %d (%s)", rr.Code, rr.Body.String())
	}
}

// The gzip guard must cap the DECOMPRESSED stream, not just return 413: without
// the inner limit the whole bomb is read into a process with GOMEMLIMIT=80MiB.
func TestGzipBombIsNotFullyDecompressed(t *testing.T) {
	cfg := testConfig()
	cfg.maxBodyBytes = 64 << 10
	s := newServer(cfg)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	chunk := bytes.Repeat([]byte("0"), 1<<20)
	for i := 0; i < 64; i++ { // 64MiB of zeros => a few dozen KB compressed
		if _, err := gz.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("POST", "/logs", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Encoding", "gzip")

	// readBody must stop at the cap: it returns at most limit+1 bytes and leaves
	// the rest of the 64MiB unread on the wire.
	data, code, _ := s.readBody(req, nil)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("want 413 got %d", code)
	}
	if len(data) != 0 {
		t.Fatalf("readBody returned %d bytes on rejection", len(data))
	}
	if n, _ := req.Body.Read(make([]byte, 1)); n == 0 {
		t.Fatal("the whole compressed body was consumed: the decompressed stream is not capped")
	}
}
