package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
)

type server struct {
	cfg    config
	m      *metrics
	ingest *ingester
	rl     *rateLimiter
	ready  *readinessCache
}

func newServer(cfg config) *server {
	m := newMetrics(version, commit, buildDate)
	ing := newIngester(cfg, m)
	m.bufferDepth = ing.depth
	m.bufferCap = ing.capacity
	s := &server{cfg: cfg, m: m, ingest: ing}
	if cfg.rateLimit > 0 {
		s.rl = newRateLimiter(cfg.rateLimit, cfg.rateBurst)
	}
	s.ready = newReadinessCache(cfg, m, 5*time.Second)
	return s
}

func (s *server) mux() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/logs", s.handleLogs)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/version", s.handleVersion)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return securityHeaders(mux)
}

func (s *server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.reject(w, "/logs", http.StatusMethodNotAllowed, "method")
		return
	}
	if !s.authorized(r) {
		s.reject(w, "/logs", http.StatusUnauthorized, "auth")
		return
	}
	if s.rl != nil && !s.rl.allow() {
		w.Header().Set("Retry-After", "1")
		s.reject(w, "/logs", http.StatusTooManyRequests, "rate_limited")
		return
	}

	rows, code, reason := s.parseBatch(r)
	if code != 0 {
		s.reject(w, "/logs", code, reason)
		return
	}

	accepted, dropped := s.ingest.enqueue(rows, s.clientIP(r))
	s.m.received.Add(int64(accepted))
	if dropped > 0 {
		s.m.dropped.Add(int64(dropped))
		s.m.httpReqs.inc(`path="/logs",code="503"`)
		http.Error(w, "buffer full, retry later", http.StatusServiceUnavailable)
		return
	}
	s.m.httpReqs.inc(`path="/logs",code="204"`)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = io.WriteString(w, "ok\n")
}

func (s *server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if s.ready.check() {
		_, _ = io.WriteString(w, "ready\n")
		return
	}
	http.Error(w, "clickhouse unreachable", http.StatusServiceUnavailable)
}

func (s *server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"version":    version,
		"commit":     commit,
		"build_date": buildDate,
		"go":         runtime.Version(),
	})
}

func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.cfg.metricsToken != "" && !secureEqual(bearer(r), s.cfg.metricsToken) {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, s.m.render())
}

func (s *server) reject(w http.ResponseWriter, path string, code int, reason string) {
	s.m.rejected.inc(`reason="` + reason + `"`)
	s.m.httpReqs.inc(`path="` + path + `",code="` + itoa(code) + `"`)
	http.Error(w, http.StatusText(code), code)
}

// authorized compares the presented token against every valid token in
// constant time and WITHOUT early exit (supports rotation: several tokens at once).
func (s *server) authorized(r *http.Request) bool {
	tok := bearer(r)
	if tok == "" {
		return false
	}
	ok := false
	for _, t := range s.cfg.tokens {
		if secureEqual(tok, t) {
			ok = true
		}
	}
	return ok
}

// clientIP trusts X-Forwarded-For only when the connection comes from a
// trusted proxy (TRUSTED_PROXIES); otherwise it uses the real peer.
func (s *server) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if len(s.cfg.trustedProxies) > 0 && isTrusted(net.ParseIP(host), s.cfg.trustedProxies) {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			cand := strings.TrimSpace(strings.Split(xff, ",")[0])
			if net.ParseIP(cand) != nil {
				return cand
			}
		}
	}
	return host
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// readinessCache caches the ClickHouse probe result so frequent /readyz calls
// don't hammer the database with SELECTs on a small box.
type readinessCache struct {
	cfg    config
	m      *metrics
	ttl    time.Duration
	client *http.Client

	mu   sync.Mutex
	last time.Time
	ok   bool
}

func newReadinessCache(cfg config, m *metrics, ttl time.Duration) *readinessCache {
	return &readinessCache{
		cfg:    cfg,
		m:      m,
		ttl:    ttl,
		client: &http.Client{Timeout: 2 * time.Second},
	}
}

// loop periodically refreshes the cache (and the chReachable metric) so the
// logden_clickhouse_reachable alert keeps working even without external /readyz traffic.
func (rc *readinessCache) loop(ctx context.Context) {
	rc.check()
	ticker := time.NewTicker(rc.ttl)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rc.check()
		}
	}
}

func (rc *readinessCache) check() bool {
	rc.mu.Lock()
	if !rc.last.IsZero() && time.Since(rc.last) < rc.ttl {
		ok := rc.ok
		rc.mu.Unlock()
		return ok
	}
	rc.mu.Unlock()

	ok := rc.probe()

	rc.mu.Lock()
	rc.ok = ok
	rc.last = time.Now()
	rc.mu.Unlock()

	if ok {
		rc.m.chReachable.Store(1)
	} else {
		rc.m.chReachable.Store(0)
	}
	return ok
}

func (rc *readinessCache) probe() bool {
	req, err := http.NewRequest(http.MethodGet, rc.cfg.chBaseURL+"/?query=SELECT%201", nil)
	if err != nil {
		return false
	}
	req.Header.Set("X-ClickHouse-User", rc.cfg.chUser)
	if rc.cfg.chKey != "" {
		req.Header.Set("X-ClickHouse-Key", rc.cfg.chKey)
	}
	resp, err := rc.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 16))
	return resp.StatusCode == http.StatusOK && strings.TrimSpace(string(b)) == "1"
}
