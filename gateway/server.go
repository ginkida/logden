package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
)

type server struct {
	cfg      config
	m        *metrics
	ingest   *ingester
	rl       *rateLimiter
	inflight *byteSemaphore
	ready    *readinessCache
}

func newServer(cfg config) *server {
	m := newMetrics(version, commit, buildDate)
	ing := newIngester(cfg, m)
	m.bufferDepth = ing.depth
	m.bufferCap = ing.capacity
	m.bufferBytes = ing.depthBytes
	m.bufferCapBytes = cfg.bufferMaxBytes
	m.inflightCapBytes = cfg.maxInflightBytes
	m.spoolCapBytes = cfg.spoolMaxBytes
	s := &server{cfg: cfg, m: m, ingest: ing}
	if cfg.rateLimit > 0 {
		s.rl = newRateLimiter(cfg.rateLimit, cfg.rateBurst)
	}
	if cfg.maxInflightBytes > 0 {
		s.inflight = newByteSemaphore(cfg.maxInflightBytes)
		m.inflightBytes = s.inflight.inUse
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
	return securityHeaders(s.countRequests(mux))
}

// statusRecorder remembers the status code for the request counter.
type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (w *statusRecorder) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

// countRequests counts every response, not only /logs: a 404 storm or a rejected
// /metrics scrape used to leave no trace at all. The path label goes through a
// fixed allowlist because labeledCounter does not escape label values, and a
// user-controlled path would also blow up the metric's cardinality.
func (s *server) countRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.m.httpReqs.inc(`path="` + knownPath(r.URL.Path) + `",code="` + itoa(rec.code) + `"`)
	})
}

func knownPath(p string) string {
	switch p {
	case "/logs", "/healthz", "/readyz", "/metrics", "/version":
		return p
	}
	return "other"
}

func (s *server) handleLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		// A 405 without Allow leaves the client guessing which method to use;
		// RFC 9110 makes the header mandatory.
		w.Header().Set("Allow", http.MethodPost)
		s.reject(w, http.StatusMethodNotAllowed, "method")
		return
	}
	if !s.authorized(r) {
		w.Header().Set("WWW-Authenticate", bearerChallenge)
		s.reject(w, http.StatusUnauthorized, "auth")
		return
	}
	if s.rl != nil && !s.rl.allow() {
		w.Header().Set("Retry-After", "1")
		s.reject(w, http.StatusTooManyRequests, "rate_limited")
		return
	}
	// Admission control on body bytes: GOMEMLIMIT is a soft GC target and can't
	// stop allocations, so concurrent large bodies must be bounded explicitly —
	// otherwise a burst of max-size uploads OOM-kills the whole gateway. The
	// budget is charged as the body is actually read (see reservation), not
	// reserved upfront: a client that announces a big body and then stalls holds
	// nothing, and small gzip/chunked requests no longer each reserve the
	// worst case.
	res := s.newReservation()
	defer res.release()

	rows, code, reason := s.parseBatch(r, res)
	if code != 0 {
		if reason == reasonOverloaded {
			w.Header().Set("Retry-After", "1")
		}
		s.reject(w, code, reason)
		return
	}

	accepted, dropped := s.ingest.enqueue(rows, s.clientIP(r))
	s.m.received.Add(int64(accepted))
	// Per-project attribution happens HERE, not inside enqueue(): the accept path
	// runs under enqueueMu, which every /logs request serializes on, and a second
	// map write inside it would add lock hold time to the pipeline's only global
	// mutex. enqueue returns just the totals, but that is enough, because it is
	// all-or-nothing: the batch either entered the buffer or it did not, so the
	// first `accepted` rows are exactly the ones that made it (accepted+dropped
	// always equals len(rows), so the per-project totals add up to the global
	// counters either way).
	s.m.projects.observe(rows, accepted)
	if dropped > 0 {
		s.m.dropped.Add(int64(dropped))
		// Every 503 the gateway sheds carries Retry-After, as README and AGENTS
		// promise; the status line is written after the header, so it has to be
		// set first. Deliberately NOT routed through reject(): a buffer-full drop
		// is counted by logden_logs_dropped_total alone, and deploy/alerts.yml
		// keys a separate rule on the rejected counter that a second increment
		// would fire. It still answers the same JSON shape as every other /logs
		// error, so a client parses one body format; "buffer_full" is a
		// response-only string and is deliberately absent from the
		// logden_logs_rejected_total vocabulary.
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "buffer_full")
		return
	}
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
		w.Header().Set("WWW-Authenticate", bearerChallenge)
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, s.m.render())
}

// reject answers an error and records why. The HTTP status itself is counted by
// countRequests, which sees every response.
func (s *server) reject(w http.ResponseWriter, code int, reason string) {
	s.m.rejected.inc(`reason="` + reason + `"`)
	writeError(w, code, reason)
}

// writeError answers a /logs failure with the reason in a machine-readable body.
// A bare "Bad Request" left the reason visible only inside
// logden_logs_rejected_total, i.e. only to whoever runs the gateway: the sender
// could not tell an invalid project name from an oversized body, and the two
// need opposite fixes. Only /logs uses this — /metrics, /readyz and /healthz keep
// the plain-text bodies scrapers and container probes already expect.
//
// The JSON is concatenated rather than encoded because every reason is a
// compile-time literal from the closed vocabulary that feeds the rejected
// counter (lowercase and underscores only), which is the same invariant
// labeledCounter relies on: no user input reaches this string, so there is
// nothing to escape and nothing to reflect. Never pass a caller-supplied value.
func writeError(w http.ResponseWriter, code int, reason string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, `{"error":"`+reason+`"}`+"\n")
}

// bearerChallenge is the challenge RFC 9110 requires on a 401. Without it a
// client cannot tell a missing-credentials failure from a blanket refusal, and
// generic HTTP clients never offer the token they were configured with.
// /logs and /metrics take different secrets but the same scheme.
const bearerChallenge = `Bearer realm="logden"`

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

// reasonOverloaded is the rejection reason for admission control (503): it also
// selects the Retry-After header among the parse-path rejections, so it must
// match on both paths.
const reasonOverloaded = "overloaded"

// errOverloaded aborts a body read that would exceed MAX_INFLIGHT_BODY_BYTES.
var errOverloaded = errors.New("inflight body budget exhausted")

// reservation is one request's slice of the inflight byte budget. It is charged
// for exactly the bytes read (never reserved upfront, never rounded up) and
// returned in full when the handler ends. Used from a single goroutine — the
// shared state lives in byteSemaphore.
//
// Charging exactly matters in both directions: a stalled client holds only what
// it actually sent, and a flood of small requests is not rejected for a memory
// cost it never incurred (rounding up to a fixed chunk would cap /logs at
// MAX_INFLIGHT_BODY_BYTES/chunk concurrent requests regardless of their size).
type reservation struct {
	sem  *byteSemaphore
	held int64
}

// newReservation returns nil when admission control is disabled; every method is
// nil-safe so the handler needs no branches.
func (s *server) newReservation() *reservation {
	if s.inflight == nil {
		return nil
	}
	return &reservation{sem: s.inflight}
}

// charge accounts n bytes just read. Returns false when the budget is exhausted —
// the caller must give up on the request (the bytes are discarded, so nothing is
// accounted twice). One mutex acquisition per Read is noise next to parsing.
func (r *reservation) charge(n int64) bool {
	if r == nil {
		return true
	}
	if !r.sem.tryAcquire(n) {
		return false
	}
	r.held += n
	return true
}

func (r *reservation) release() {
	if r == nil || r.held == 0 {
		return
	}
	r.sem.release(r.held)
	r.held = 0
}

// meter wraps a body reader so the bytes it yields are charged to the
// reservation. Wrapping the DECOMPRESSED stream is deliberate: the budget bounds
// memory, and a gzip bomb costs what it inflates to, not what it weighs on the wire.
// Safe with a nil reservation (admission control disabled): it then only counts.
func (r *reservation) meter(src io.Reader) *meteredReader {
	return &meteredReader{r: src, res: r}
}

type meteredReader struct {
	r         io.Reader
	res       *reservation
	delivered int64 // charged and handed to the caller
	attempted int64 // delivered + the read the budget refused (0 if none)
}

func (m *meteredReader) Read(p []byte) (int, error) {
	n, err := m.r.Read(p)
	if n > 0 {
		if !m.res.charge(int64(n)) {
			// Remember how far the body actually goes: readBody uses it to tell an
			// oversized body (413) from genuine load (503).
			m.attempted = m.delivered + int64(n)
			return 0, errOverloaded
		}
		m.delivered += int64(n)
	}
	return n, err
}

// clientIP trusts X-Forwarded-For only when the connection comes from a
// trusted proxy (TRUSTED_PROXIES); otherwise it uses the real peer.
//
// The chain is walked RIGHT TO LEFT. The leftmost entry is whatever the client
// typed: every appending proxy — nginx's $proxy_add_x_forwarded_for, Caddy's
// reverse_proxy — forwards "<client-supplied value>, <peer it actually saw>",
// so reading element 0 let a client forge source_ip in exactly the reverse-proxy
// deployment SECURITY.md recommends. The price of the fix is that every hop in
// front of the gateway must be listed in TRUSTED_PROXIES, or source_ip records
// the nearest unlisted hop instead of the end client.
func (s *server) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	if len(s.cfg.trustedProxies) == 0 || !isTrusted(net.ParseIP(host), s.cfg.trustedProxies) {
		return host
	}
	if ip := forwardedFor(r.Header.Values("X-Forwarded-For"), s.cfg.trustedProxies); ip != "" {
		return ip
	}
	return host
}

// maxForwardedHops bounds how far back forwardedFor walks. MaxHeaderBytes lets
// a request carry a 32 KiB header, and real deployments have a handful of hops:
// the cap stops a padded chain from turning every /logs request into thousands
// of parse attempts. It examines the LAST N entries, so a long legitimate chain
// still resolves to a near hop rather than silently collapsing to the peer.
const maxForwardedHops = 16

// forwardedFor returns the rightmost X-Forwarded-For entry that is not itself a
// trusted proxy, or "" when the chain yields nothing usable and the caller must
// keep the real peer.
//
// It takes every header line, not Get's first one: Go does not merge repeated
// headers, so a client that sends its own X-Forwarded-For line keeps that line
// ahead of the one the proxy adds, and scanning Get's value alone would still
// return the forged address. Per RFC 7230 repeated lines are one ordered list,
// so the last line holds the most recent hops.
//
// The scan walks commas from the end with LastIndexByte instead of splitting:
// a 32 KiB header would otherwise allocate a multi-thousand-element slice on the
// hottest path, under an 80 MiB memory budget.
func forwardedFor(values []string, trusted []*net.IPNet) string {
	leftmost := ""
	hops := 0
	for i := len(values) - 1; i >= 0 && hops < maxForwardedHops; i-- {
		rest := values[i]
		for hops < maxForwardedHops {
			entry := rest
			if c := strings.LastIndexByte(rest, ','); c >= 0 {
				entry, rest = rest[c+1:], rest[:c]
			} else {
				rest = ""
			}
			hops++
			ip, text := parseForwardedEntry(strings.TrimSpace(entry))
			if ip == nil {
				// An entry we cannot parse means the rest of the chain no longer
				// says anything reliable about who is upstream: fail closed onto
				// the peer rather than record an attacker-chosen string.
				return ""
			}
			// isTrusted must see the parsed address, not the text, or an
			// IPv4-mapped IPv6 form would slip past the operator's CIDRs.
			if !isTrusted(ip, trusted) {
				return text
			}
			// Everything so far is a known proxy. Keep the furthest one: when the
			// whole chain is trusted the sender itself lives inside TRUSTED_PROXIES
			// (an internal service behind the same proxy), and the leftmost entry
			// identifies it, whereas the peer would collapse every such sender onto
			// the proxy address.
			leftmost = text
			if rest == "" {
				break
			}
		}
	}
	return leftmost
}

// parseForwardedEntry parses one chain entry and returns the address plus its
// textual form. Both are substrings of the header, so the walk stays allocation
// free apart from the parsed IP itself.
func parseForwardedEntry(entry string) (net.IP, string) {
	if ip := net.ParseIP(entry); ip != nil {
		return ip, entry
	}
	// Some proxies (Azure Front Door, HAProxy) append "ip:port" entries; the port
	// has no place in source_ip, so only the host survives.
	if host, _, err := net.SplitHostPort(entry); err == nil {
		if ip := net.ParseIP(host); ip != nil {
			return ip, host
		}
	}
	return nil, ""
}

// byteSemaphore caps the total estimated body bytes processed concurrently.
// Non-blocking by design: over the budget the caller answers 503 immediately
// (backpressure, same contract as a full buffer) instead of queueing goroutines.
type byteSemaphore struct {
	mu  sync.Mutex
	cur int64
	max int64
}

func newByteSemaphore(max int64) *byteSemaphore { return &byteSemaphore{max: max} }

func (bs *byteSemaphore) tryAcquire(n int64) bool {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if bs.cur+n > bs.max {
		return false
	}
	bs.cur += n
	return true
}

func (bs *byteSemaphore) release(n int64) {
	bs.mu.Lock()
	bs.cur -= n
	bs.mu.Unlock()
}

func (bs *byteSemaphore) inUse() int64 {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	return bs.cur
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

	mu      sync.Mutex
	last    time.Time
	ok      bool
	probing bool // single-flight: one probe in flight, others serve the last value
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
	fresh := !rc.last.IsZero() && time.Since(rc.last) < rc.ttl
	// Single-flight: /readyz is unauthenticated, so without this every concurrent
	// request that finds the entry expired would fire its own ClickHouse query —
	// exactly the stampede this cache exists to prevent.
	if fresh || rc.probing {
		ok := rc.ok
		rc.mu.Unlock()
		return ok
	}
	rc.probing = true
	rc.mu.Unlock()

	ok := rc.probe()

	rc.mu.Lock()
	rc.ok = ok
	rc.last = time.Now()
	rc.probing = false
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
