package main

import (
	"crypto/subtle"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envDur(k string, def time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func itoa(n int) string { return strconv.Itoa(n) }

// readSecret supports the *_FILE pattern (docker/compose secrets):
// if NAME_FILE is set, read the secret from the file, otherwise from NAME.
func readSecret(name string) string {
	if path := os.Getenv(name + "_FILE"); path != "" {
		b, err := os.ReadFile(path)
		if err == nil {
			return strings.TrimSpace(string(b))
		}
		slog.Warn("cannot read secret file", "env", name+"_FILE", "err", err)
	}
	return os.Getenv(name)
}

// splitTokens splits LOG_TOKEN on commas and newlines only. Spaces and tabs are
// NOT separators: a passphrase with a space in it would otherwise become several
// independently valid tokens, each shorter and easier to guess than the secret
// the operator thinks they configured.
func splitTokens(s string) []string {
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r'
	})
	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

func bearer(r *http.Request) string {
	// The Authorization scheme is case-insensitive (RFC 7235).
	if h := r.Header.Get("Authorization"); len(h) >= 7 && strings.EqualFold(h[:7], "Bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return strings.TrimSpace(r.Header.Get("X-Log-Token"))
}

func secureEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

func parseCIDRs(s string) ([]*net.IPNet, error) {
	var nets []*net.IPNet
	for _, tok := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' }) {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if !strings.Contains(tok, "/") {
			ip := net.ParseIP(tok)
			if ip == nil {
				return nil, fmt.Errorf("invalid IP %q", tok)
			}
			bits := 32
			if ip.To4() == nil {
				bits = 128
			}
			tok = fmt.Sprintf("%s/%d", tok, bits)
		}
		_, n, err := net.ParseCIDR(tok)
		if err != nil {
			return nil, err
		}
		nets = append(nets, n)
	}
	return nets, nil
}

func isTrusted(ip net.IP, nets []*net.IPNet) bool {
	if ip == nil {
		return false
	}
	for _, n := range nets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// rateLimiter is a simple token bucket on the stdlib (no golang.org/x/time/rate).
type rateLimiter struct {
	mu     sync.Mutex
	tokens float64
	max    float64
	refill float64
	last   time.Time
}

func newRateLimiter(rps, burst float64) *rateLimiter {
	if burst <= 0 {
		burst = rps
	}
	if burst < 1 {
		// allow() spends a whole token, so a bucket that can never hold one would
		// reject every request forever — a fractional RATE_LIMIT_RPS (say 0.5)
		// would silently take the whole ingest path down. The average rate still
		// comes from refill; burst is only the ceiling.
		burst = 1
	}
	return &rateLimiter{tokens: burst, max: burst, refill: rps, last: time.Now()}
}

func (rl *rateLimiter) allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	now := time.Now()
	rl.tokens += now.Sub(rl.last).Seconds() * rl.refill
	if rl.tokens > rl.max {
		rl.tokens = rl.max
	}
	rl.last = now
	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}
