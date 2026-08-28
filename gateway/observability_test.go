package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

// Tests for the three signals an operator otherwise has to reconstruct from SQL:
// per-project ingest, the reason behind a /logs rejection, and timestamps the
// gateway silently rewrote.

func obsConfig() config {
	return config{
		listenAddr:      ":0",
		tokens:          []string{"secret"},
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

// obsPost sends an authorized POST /logs through the full mux (the middleware
// chain is part of what these tests assert).
func obsPost(s *server, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/logs", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, req)
	return rr
}

func obsWantLine(t *testing.T, out, line string) {
	t.Helper()
	if !strings.Contains(out, line+"\n") {
		t.Errorf("missing metric line %q in:\n%s", line, out)
	}
}

// TestProjectCountersAcceptAndDrop covers the whole point of the per-project
// counters: which project got in, and which one was shed. The buffer is sized so
// the second request cannot fit, and no worker runs (start() is never called),
// so the drop is deterministic.
func TestProjectCountersAcceptAndDrop(t *testing.T) {
	cfg := obsConfig()
	cfg.bufferSize = 2
	s := newServer(cfg)

	if rr := obsPost(s, `{"project":"alpha","message":"in"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("accept: want 204 got %d (%s)", rr.Code, rr.Body.String())
	}
	// Two events, one free slot: enqueue is all-or-nothing, so both are dropped.
	rr := obsPost(s, `[{"project":"beta","message":"a"},{"project":"gamma","message":"b"}]`, nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("drop: want 503 got %d (%s)", rr.Code, rr.Body.String())
	}
	if got := rr.Body.String(); strings.TrimSpace(got) != `{"error":"buffer_full"}` {
		t.Errorf("buffer-full 503 must carry a JSON reason, got %q", got)
	}

	out := s.m.render()
	obsWantLine(t, out, `logden_project_logs_received_total{project="alpha"} 1`)
	obsWantLine(t, out, `logden_project_logs_dropped_total{project="beta"} 1`)
	obsWantLine(t, out, `logden_project_logs_dropped_total{project="gamma"} 1`)
	// A project that only ever gets in must not appear as a drop, and vice versa.
	if strings.Contains(out, `logden_project_logs_dropped_total{project="alpha"}`) {
		t.Errorf("alpha was accepted, it must not be counted as dropped:\n%s", out)
	}
	if strings.Contains(out, `logden_project_logs_received_total{project="beta"}`) {
		t.Errorf("beta was dropped, it must not be counted as received:\n%s", out)
	}
	// The per-project counters must add up to the global ones, otherwise two
	// dashboards disagree about the same traffic.
	obsWantLine(t, out, "logden_logs_received_total 1")
	obsWantLine(t, out, "logden_logs_dropped_total 2")
	obsWantLine(t, out, "logden_project_labels_tracked 3")
	obsWantLine(t, out, fmt.Sprintf("logden_project_labels_capacity %d", maxProjectLabels))
}

// TestProjectLabelCardinalityCap is the guard against a sender that invents a
// project name per event: the label set must stop growing and everything past
// the cap must land in one visibly non-project bucket.
func TestProjectLabelCardinalityCap(t *testing.T) {
	s := newServer(obsConfig())

	rows := make([]row, 0, maxProjectLabels+10)
	for i := 0; i < maxProjectLabels+10; i++ {
		rows = append(rows, row{Project: fmt.Sprintf("p%03d", i), Message: "m"})
	}
	s.m.projects.observe(rows, len(rows))

	out := s.m.render()
	obsWantLine(t, out, fmt.Sprintf("logden_project_labels_tracked %d", maxProjectLabels))
	obsWantLine(t, out, fmt.Sprintf(`logden_project_logs_received_total{project=%q} 10`, overflowProject))

	series := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "logden_project_logs_received_total{") {
			series++
		}
	}
	if series != maxProjectLabels+1 {
		t.Fatalf("label set unbounded: %d series, want cap %d + overflow", series, maxProjectLabels+1)
	}

	// Admission is sticky: an already-tracked project keeps its own series even
	// after the cap is reached, so a real sender is not buried by a name flood.
	s.m.projects.observe([]row{{Project: "p000", Message: "m"}}, 1)
	// ...and a fresh name still folds into the overflow bucket.
	s.m.projects.observe([]row{{Project: "brand-new", Message: "m"}}, 0)
	out = s.m.render()
	obsWantLine(t, out, `logden_project_logs_received_total{project="p000"} 2`)
	obsWantLine(t, out, fmt.Sprintf(`logden_project_logs_dropped_total{project=%q} 1`, overflowProject))
	if strings.Contains(out, `project="brand-new"`) {
		t.Errorf("a project past the cap must not get its own label:\n%s", out)
	}
}

// TestProjectLabelSafety pins the one invariant the per-project labels rest on:
// render does not escape label values, so nothing but a projectRe-validated name
// (or the overflow literal, which projectRe can never produce) may reach them.
func TestProjectLabelSafety(t *testing.T) {
	if projectRe.MatchString(overflowProject) {
		t.Fatalf("%q is a legal project name — a sender could hide inside the overflow bucket", overflowProject)
	}

	s := newServer(obsConfig())
	// A project name crafted to close the label and append a fake series. It must
	// never reach the counters: buildRow rejects it long before a row exists.
	hostile, err := json.Marshal(map[string]string{
		"project": "evil\"} 99\nlogden_injected{x=\"",
		"message": "m",
	})
	if err != nil {
		t.Fatal(err)
	}
	if rr := obsPost(s, string(hostile), nil); rr.Code != http.StatusBadRequest {
		t.Fatalf("hostile project: want 400 got %d (%s)", rr.Code, rr.Body.String())
	}
	if rr := obsPost(s, `{"project":"good","message":"m"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("valid event: want 204 got %d (%s)", rr.Code, rr.Body.String())
	}

	out := s.m.render()
	if strings.Contains(out, "logden_injected") {
		t.Fatalf("label injection reached /metrics:\n%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "logden_project_logs_") || !strings.Contains(line, "{") {
			continue
		}
		value := line[strings.Index(line, `{project="`)+len(`{project="`) : strings.LastIndex(line, `"}`)]
		if value != overflowProject && !projectRe.MatchString(value) {
			t.Errorf("unvalidated project label %q in line %q", value, line)
		}
	}
}

// TestRejectionBodyIsJSON: the reason used to live only inside
// logden_logs_rejected_total, i.e. only the operator could see it. The sender
// needs it too — "your project name is invalid" and "your body is too big" need
// opposite fixes.
func TestRejectionBodyIsJSON(t *testing.T) {
	cases := []struct {
		name, method, body string
		hdr                map[string]string
		tune               func(*config)
		wantCode           int
		wantReason         string
	}{
		{name: "method", method: "GET", wantCode: http.StatusMethodNotAllowed, wantReason: "method"},
		{name: "empty body", method: "POST", body: "   ", wantCode: http.StatusBadRequest, wantReason: "empty"},
		{name: "bad json", method: "POST", body: `{"project":`, wantCode: http.StatusBadRequest, wantReason: "bad_json"},
		{
			name: "all invalid", method: "POST", body: `[{"project":"p"},{"message":"m"}]`,
			wantCode: http.StatusBadRequest, wantReason: "all_invalid",
		},
		{
			name: "bad gzip", method: "POST", body: "not gzip at all",
			hdr:      map[string]string{"Content-Encoding": "gzip"},
			wantCode: http.StatusBadRequest, wantReason: "bad_gzip",
		},
		{
			name: "too large", method: "POST", body: `{"project":"p","message":"` + strings.Repeat("x", 512) + `"}`,
			tune:     func(c *config) { c.maxBodyBytes = 128 },
			wantCode: http.StatusRequestEntityTooLarge, wantReason: "too_large",
		},
		{
			name: "too many events", method: "POST",
			body:     `[{"project":"p","message":"a"},{"project":"p","message":"b"}]`,
			tune:     func(c *config) { c.maxBatchEvents = 1 },
			wantCode: http.StatusRequestEntityTooLarge, wantReason: "too_many_events",
		},
		{
			name: "overloaded", method: "POST", body: `{"project":"p","message":"m"}`,
			tune:     func(c *config) { c.maxInflightBytes = 8 },
			wantCode: http.StatusServiceUnavailable, wantReason: "overloaded",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := obsConfig()
			if c.tune != nil {
				c.tune(&cfg)
			}
			s := newServer(cfg)
			req := httptest.NewRequest(c.method, "/logs", strings.NewReader(c.body))
			req.Header.Set("Authorization", "Bearer secret")
			for k, v := range c.hdr {
				req.Header.Set(k, v)
			}
			rr := httptest.NewRecorder()
			s.mux().ServeHTTP(rr, req)

			if rr.Code != c.wantCode {
				t.Fatalf("want %d got %d (%s)", c.wantCode, rr.Code, rr.Body.String())
			}
			if ct := rr.Header().Get("Content-Type"); ct != "application/json; charset=utf-8" {
				t.Errorf("want a JSON content type, got %q", ct)
			}
			var body struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not JSON (%v): %q", err, rr.Body.String())
			}
			if body.Error != c.wantReason {
				t.Errorf("want error %q got %q", c.wantReason, body.Error)
			}
			// The reason vocabulary is shared with the metric, and alerts key on it.
			obsWantLine(t, s.m.render(), fmt.Sprintf("logden_logs_rejected_total{reason=%q} 1", c.wantReason))
		})
	}
}

// TestRejectionBodyMissingToken keeps the 401 in the same shape as the rest
// without leaking anything about the token.
func TestRejectionBodyMissingToken(t *testing.T) {
	s := newServer(obsConfig())
	req := httptest.NewRequest("POST", "/logs", strings.NewReader(`{"project":"p","message":"m"}`))
	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 got %d", rr.Code)
	}
	if got := strings.TrimSpace(rr.Body.String()); got != `{"error":"auth"}` {
		t.Errorf("want a JSON auth reason, got %q", got)
	}
	if rr.Header().Get("WWW-Authenticate") == "" {
		t.Error("401 must still carry the challenge")
	}
}

// TestProbeBodiesUnchanged: the JSON shape is a /logs contract only. Probes and
// scrapers parse the other endpoints' plain text.
func TestProbeBodiesUnchanged(t *testing.T) {
	s := newServer(obsConfig())
	rr := httptest.NewRecorder()
	s.mux().ServeHTTP(rr, httptest.NewRequest("GET", "/healthz", nil))
	if rr.Body.String() != "ok\n" {
		t.Errorf("/healthz body changed: %q", rr.Body.String())
	}

	cfg := obsConfig()
	cfg.metricsToken = "m"
	rr = httptest.NewRecorder()
	newServer(cfg).mux().ServeHTTP(rr, httptest.NewRequest("GET", "/metrics", nil))
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("/metrics want 401 got %d", rr.Code)
	}
	if strings.Contains(rr.Body.String(), `{"error"`) {
		t.Errorf("/metrics must keep its plain-text error body: %q", rr.Body.String())
	}
}

// TestRestampCounter: an out-of-range client timestamp is overwritten with the
// ingest time, which used to be completely invisible — a fleet with a broken
// clock looked healthy.
func TestRestampCounter(t *testing.T) {
	s := newServer(obsConfig())
	ingestFloor := time.Now().UTC().Add(-time.Minute).Format(chTimeLayout)

	future, err := json.Marshal(time.Now().UTC().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	old, err := json.Marshal(time.Now().UTC().Add(-60 * 24 * time.Hour)) // past RETENTION (30d)
	if err != nil {
		t.Fatal(err)
	}

	for _, ts := range []json.RawMessage{future, old} {
		rw, ok := s.buildRow(inEvent{Project: "p", Message: "m", Timestamp: ts})
		if !ok {
			t.Fatalf("buildRow rejected an event with timestamp %s", ts)
		}
		// The row must carry the ingest time, not the client's junk.
		if rw.Timestamp < ingestFloor {
			t.Errorf("timestamp %s was not replaced with the ingest time: got %s", ts, rw.Timestamp)
		}
	}

	// Events that simply carry no usable timestamp are normal traffic: they are
	// stamped too, but nothing was discarded, so they must not be counted.
	for _, e := range []inEvent{
		{Project: "p", Message: "m"},
		{Project: "p", Message: "m", Timestamp: json.RawMessage(`null`)},
		{Project: "p", Message: "m", Timestamp: json.RawMessage(`"not a time"`)},
	} {
		if _, ok := s.buildRow(e); !ok {
			t.Fatal("buildRow rejected a valid event without a usable timestamp")
		}
	}

	out := s.m.render()
	obsWantLine(t, out, `logden_logs_restamped_total{reason="future"} 1`)
	obsWantLine(t, out, `logden_logs_restamped_total{reason="too_old"} 1`)
}

// TestNormalizeEventTimeSkew pins the classification itself, including the
// boundaries: inside the window nothing is reported.
func TestNormalizeEventTimeSkew(t *testing.T) {
	const retention = 30 * 24 * time.Hour
	now := time.Now().UTC()
	cases := []struct {
		name     string
		raw      string
		wantTS   bool
		wantSkew string
	}{
		{"absent", ``, false, skewNone},
		{"null", `null`, false, skewNone},
		{"unparseable", `"yesterday"`, false, skewNone},
		{"in window", `"` + now.Add(-time.Hour).Format(time.RFC3339) + `"`, true, skewNone},
		{"just inside future window", `"` + now.Add(4*time.Minute).Format(time.RFC3339) + `"`, true, skewNone},
		{"far future", `"` + now.Add(2*time.Hour).Format(time.RFC3339) + `"`, false, skewFuture},
		{"older than retention", `"` + now.Add(-retention-time.Hour).Format(time.RFC3339) + `"`, false, skewTooOld},
		{"unix seconds far future", fmt.Sprintf("%d", now.Add(48*time.Hour).Unix()), false, skewFuture},
		{"unix millis too old", fmt.Sprintf("%d", now.Add(-retention-48*time.Hour).UnixMilli()), false, skewTooOld},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ts, skew := normalizeEventTime(json.RawMessage(c.raw), retention)
			if (ts != "") != c.wantTS {
				t.Errorf("timestamp %q, want non-empty=%v", ts, c.wantTS)
			}
			if skew != c.wantSkew {
				t.Errorf("want skew %q got %q", c.wantSkew, skew)
			}
			// The thin wrapper the rest of the code (and the docs) still use.
			if got := normalizeTimestamp(json.RawMessage(c.raw), retention); got != ts {
				t.Errorf("normalizeTimestamp disagrees: %q vs %q", got, ts)
			}
		})
	}
}

// A project label must never alias the request body. row.Project is a
// strings.TrimSpace substring of the whole decoded `project` field, so a label
// stored as-is pins up to MAX_BODY_BYTES for the life of the process — 64 such
// labels are 256 MiB against a 128m container limit, and nothing reports it:
// the buffer gauges read zero and logden_project_labels_tracked reads a healthy
// 64. Testing for aliasing rather than for heap growth keeps this deterministic.
func TestProjectLabelsDoNotAliasTheRequestBody(t *testing.T) {
	padded := strings.Repeat(" ", 1<<20) + "web"
	project := strings.TrimSpace(padded)
	if !aliasesString(project, padded) {
		t.Fatal("fixture is wrong: TrimSpace no longer returns a substring, rewrite this test")
	}

	p := newProjectCounters()
	label := p.labelLocked(project)
	if label != "web" {
		t.Fatalf("label = %q, want %q", label, "web")
	}
	if aliasesString(label, padded) {
		t.Error("admitted label aliases the request body: clone it in labelLocked")
	}
	// The second lookup must return the stored key, not the caller's string —
	// the path that leaked even for an already-admitted project.
	if aliasesString(p.labelLocked(project), padded) {
		t.Error("repeat lookup returns the caller's string: return the stored key")
	}
	// And the map must be keyed by the clone, not by the substring handed in.
	for k := range p.tracked {
		if aliasesString(k, padded) {
			t.Error("tracked is keyed by the caller's string: key it by the clone")
		}
	}
	runtime.KeepAlive(padded)
}

// aliasesString reports whether s points inside parent's backing array, which is
// what keeps the whole allocation reachable. Comparing the two data pointers is
// not enough: a substring starts at an offset, so its pointer already differs
// while it still pins every byte of the parent.
func aliasesString(s, parent string) bool {
	base := uintptr(unsafe.Pointer(unsafe.StringData(parent)))
	return uintptr(unsafe.Pointer(unsafe.StringData(s))) >= base &&
		uintptr(unsafe.Pointer(unsafe.StringData(s))) < base+uintptr(len(parent))
}
