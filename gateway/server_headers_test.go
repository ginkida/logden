package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// forwardedRequest builds a request from peer with one X-Forwarded-For header
// line per value, so tests can reproduce a client that sends its own line and a
// proxy that adds another (Go never merges them).
func forwardedRequest(peer string, xff ...string) *http.Request {
	req := httptest.NewRequest("POST", "/logs", nil)
	req.RemoteAddr = peer
	for _, v := range xff {
		req.Header.Add("X-Forwarded-For", v)
	}
	return req
}

func TestClientIPForwardedChain(t *testing.T) {
	cfg := testConfig()
	cfg.trustedProxies, _ = parseCIDRs("10.0.0.0/8, 192.0.2.0/24, ::1/128")
	s := newServer(cfg)

	cases := []struct {
		name string
		peer string
		xff  []string
		want string
	}{
		{
			// A client talking to the gateway directly can put anything in the
			// header; the peer is the only thing that is not client-controlled.
			name: "spoof from untrusted peer is ignored",
			peer: "203.0.113.9:4444",
			xff:  []string{"198.51.100.7"},
			want: "203.0.113.9",
		},
		{
			// What nginx/Caddy actually forward: the client's own value first,
			// then the address the proxy observed.
			name: "spoofed leftmost loses to the proxy-appended entry",
			peer: "10.0.0.5:1234",
			xff:  []string{"203.0.113.9, 198.51.100.7"},
			want: "198.51.100.7",
		},
		{
			// The client sends its own header LINE; Get would return that one and
			// miss the line the proxy appended.
			name: "duplicate header lines are one chain",
			peer: "10.0.0.5:1234",
			xff:  []string{"203.0.113.9", "198.51.100.7"},
			want: "198.51.100.7",
		},
		{
			name: "duplicate lines, spoofed line carries its own commas",
			peer: "10.0.0.5:1234",
			xff:  []string{"203.0.113.9, 203.0.113.10", "198.51.100.7"},
			want: "198.51.100.7",
		},
		{
			// Everything in front is a known proxy, so the sender itself is inside
			// TRUSTED_PROXIES: the leftmost entry names it, the peer would not.
			name: "all-trusted chain keeps the furthest hop",
			peer: "10.0.0.5:1234",
			xff:  []string{"10.0.0.9, 192.0.2.7"},
			want: "10.0.0.9",
		},
		{
			name: "single trusted entry keeps that entry",
			peer: "10.0.0.5:1234",
			xff:  []string{"10.0.0.9"},
			want: "10.0.0.9",
		},
		{
			name: "no header falls back to the peer",
			peer: "10.0.0.5:1234",
			want: "10.0.0.5",
		},
		{
			name: "empty header falls back to the peer",
			peer: "10.0.0.5:1234",
			xff:  []string{""},
			want: "10.0.0.5",
		},
		{
			// The malformed hop sits left of the answer, so it is never reached.
			name: "garbage left of a usable entry does not matter",
			peer: "10.0.0.5:1234",
			xff:  []string{"junk, 198.51.100.7"},
			want: "198.51.100.7",
		},
		{
			// Fail closed: an unparseable hop must never become source_ip.
			name: "garbage at the right end falls back to the peer",
			peer: "10.0.0.5:1234",
			xff:  []string{"198.51.100.7, junk"},
			want: "10.0.0.5",
		},
		{
			name: "ip:port entries lose the port",
			peer: "10.0.0.5:1234",
			xff:  []string{"203.0.113.9, 198.51.100.7:51234"},
			want: "198.51.100.7",
		},
		{
			name: "ipv6 peer and entry",
			peer: "[::1]:1234",
			xff:  []string{"2001:db8::1"},
			want: "2001:db8::1",
		},
		{
			name: "ipv6 peer, spoofed leftmost",
			peer: "[::1]:1234",
			xff:  []string{"2001:db8::9, 198.51.100.7"},
			want: "198.51.100.7",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := forwardedRequest(c.peer, c.xff...)
			if got := s.clientIP(req); got != c.want {
				t.Fatalf("clientIP = %q, want %q", got, c.want)
			}
		})
	}
}

// A padded chain must not be walked in full, and must not collapse to the peer
// either: the last maxForwardedHops entries are examined, so the answer is a
// near hop, never the attacker's leftmost value.
func TestClientIPLongChainIsCapped(t *testing.T) {
	cfg := testConfig()
	cfg.trustedProxies, _ = parseCIDRs("10.0.0.0/8")
	s := newServer(cfg)

	chain := "203.0.113.9" + strings.Repeat(", 10.0.0.1", maxForwardedHops+4)
	req := forwardedRequest("10.0.0.5:1234", chain)
	if got := s.clientIP(req); got != "10.0.0.1" {
		t.Fatalf("clientIP = %q, want a near trusted hop", got)
	}
}

func TestBufferFullCarriesRetryAfter(t *testing.T) {
	cfg := testConfig()
	cfg.bufferSize = 1
	s := newServer(cfg) // worker not started — nobody drains the buffer
	if rr := doLogs(s, "POST", "secret", `{"project":"p","message":"a"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("first: want 204 got %d", rr.Code)
	}

	rr := doLogs(s, "POST", "secret", `{"project":"p","message":"b"}`, nil)
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("overflow: want 503 got %d", rr.Code)
	}
	if got := rr.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("buffer-full 503 Retry-After = %q, want %q", got, "1")
	}
	// The drop stays on the dropped counter: deploy/alerts.yml keys a separate
	// rule on the rejected counter, which this path must not touch.
	if s.m.dropped.Load() == 0 {
		t.Fatal("expected dropped metric > 0")
	}
	if strings.Contains(s.m.render(), "logden_logs_rejected_total{") {
		t.Error("buffer-full drops must not be counted as rejections")
	}
}

func TestMethodNotAllowedCarriesAllow(t *testing.T) {
	s := newServer(testConfig())
	rr := doLogs(s, "GET", "secret", "", nil)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405 got %d", rr.Code)
	}
	if got := rr.Header().Get("Allow"); got != http.MethodPost {
		t.Fatalf("Allow = %q, want %q", got, http.MethodPost)
	}
}

func TestUnauthorizedCarriesChallenge(t *testing.T) {
	cfg := testConfig()
	cfg.metricsToken = "msecret"
	s := newServer(cfg)

	rr := doLogs(s, "POST", "", `{"project":"p","message":"m"}`, nil)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("/logs: want 401 got %d", rr.Code)
	}
	if got := rr.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer") {
		t.Fatalf("/logs 401 WWW-Authenticate = %q, want a Bearer challenge", got)
	}

	rrm := httptest.NewRecorder()
	s.mux().ServeHTTP(rrm, httptest.NewRequest("GET", "/metrics", nil))
	if rrm.Code != http.StatusUnauthorized {
		t.Fatalf("/metrics: want 401 got %d", rrm.Code)
	}
	if got := rrm.Header().Get("WWW-Authenticate"); !strings.HasPrefix(got, "Bearer") {
		t.Fatalf("/metrics 401 WWW-Authenticate = %q, want a Bearer challenge", got)
	}
}
