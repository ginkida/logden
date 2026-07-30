package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var metricNameRe = regexp.MustCompile(`logden_[a-z_]+`)

func netParse(t *testing.T, s string) net.IP {
	t.Helper()
	ip := net.ParseIP(s)
	if ip == nil {
		t.Fatalf("bad test IP %q", s)
	}
	return ip
}

func TestSplitTokens(t *testing.T) {
	cases := map[string][]string{
		"a,b,c":                {"a", "b", "c"},
		" a , b ":              {"a", "b"},
		"a\nb\r\nc":            {"a", "b", "c"},
		"":                     nil,
		"  ":                   nil,
		"correct horse":        {"correct horse"}, // a spaced passphrase is ONE token
		"correct horse,second": {"correct horse", "second"},
	}
	for in, want := range cases {
		got := splitTokens(in)
		if len(got) != len(want) {
			t.Errorf("splitTokens(%q) = %q, want %q", in, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("splitTokens(%q) = %q, want %q", in, got, want)
				break
			}
		}
	}
}

// A token containing a space must authenticate as a whole and its fragments must
// not: that is the whole point of not splitting on whitespace.
func TestSpacedTokenIsNotSplitForAuth(t *testing.T) {
	cfg := testConfig()
	cfg.tokens = splitTokens("correct horse battery")
	s := newServer(cfg)

	if rr := doLogs(s, "POST", "correct horse battery", `{"project":"p","message":"m"}`, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("full token: want 204 got %d", rr.Code)
	}
	if rr := doLogs(s, "POST", "correct", `{"project":"p","message":"m"}`, nil); rr.Code != http.StatusUnauthorized {
		t.Fatalf("token fragment must not authenticate, got %d", rr.Code)
	}
}

func TestLoadConfigEnv(t *testing.T) {
	dir := t.TempDir()
	secretFile := filepath.Join(dir, "token")
	if err := os.WriteFile(secretFile, []byte("file_token\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("LOG_TOKEN", "env_token")
	t.Setenv("LOG_TOKEN_FILE", secretFile)
	t.Setenv("CLICKHOUSE_URL", "http://clickhouse:8123/")
	t.Setenv("BATCH_SIZE", "not-a-number") // malformed values fall back to the default
	t.Setenv("FLUSH_INTERVAL", "2s")
	t.Setenv("RATE_LIMIT_RPS", "5")
	t.Setenv("TRUSTED_PROXIES", "10.0.0.0/8, 192.168.1.7")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if len(cfg.tokens) != 1 || cfg.tokens[0] != "file_token" {
		t.Fatalf("LOG_TOKEN_FILE must win over LOG_TOKEN and be trimmed, got %q", cfg.tokens)
	}
	if cfg.chBaseURL != "http://clickhouse:8123" {
		t.Fatalf("trailing slash must be trimmed, got %q", cfg.chBaseURL)
	}
	if cfg.batchSize != 500 {
		t.Fatalf("malformed BATCH_SIZE must fall back to the default, got %d", cfg.batchSize)
	}
	if cfg.flushInterval != 2*time.Second {
		t.Fatalf("FLUSH_INTERVAL = %s", cfg.flushInterval)
	}
	if cfg.rateBurst != 5 {
		t.Fatalf("RATE_BURST must default to RATE_LIMIT_RPS, got %v", cfg.rateBurst)
	}
	if len(cfg.trustedProxies) != 2 {
		t.Fatalf("TRUSTED_PROXIES = %v", cfg.trustedProxies)
	}
	if !isTrusted(netParse(t, "10.1.2.3"), cfg.trustedProxies) || !isTrusted(netParse(t, "192.168.1.7"), cfg.trustedProxies) {
		t.Fatal("trusted proxy CIDRs and bare IPs must both match")
	}
}

func TestLoadConfigRequiresToken(t *testing.T) {
	t.Setenv("LOG_TOKEN", "")
	t.Setenv("LOG_TOKEN_FILE", "")
	if _, err := loadConfig(); err == nil {
		t.Fatal("loadConfig must fail without LOG_TOKEN")
	}
}

// A fractional RATE_LIMIT_RPS used to make allow() return false forever, taking
// the whole ingest path down silently.
func TestRateLimiterFractionalRPS(t *testing.T) {
	rl := newRateLimiter(0.5, 0)
	if !rl.allow() {
		t.Fatal("a fractional rps must still admit the first request")
	}
	if rl.allow() {
		t.Fatal("the bucket holds one token at 0.5 rps, the second request must wait")
	}
}

// /readyz is unauthenticated: concurrent requests past the TTL must collapse into
// a single ClickHouse probe.
func TestReadinessSingleFlight(t *testing.T) {
	var probes atomic.Int64
	release := make(chan struct{})
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		<-release
		_, _ = w.Write([]byte("1"))
	}))
	defer stub.Close()

	cfg := testConfig()
	cfg.chBaseURL = stub.URL
	rc := newReadinessCache(cfg, newMetrics("", "", ""), time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rc.check()
		}()
	}
	waitFor(t, 2*time.Second, func() bool { return probes.Load() >= 1 })
	close(release)
	wg.Wait()

	if got := probes.Load(); got != 1 {
		t.Fatalf("%d concurrent probes, want 1 (single-flight)", got)
	}
	if !rc.check() {
		t.Fatal("cache should be ready after the probe")
	}
}

// Every metric name referenced by the alert rules must actually be rendered:
// renaming a metric otherwise disables its alert silently.
func TestAlertRulesReferenceRealMetrics(t *testing.T) {
	data, err := os.ReadFile("../deploy/alerts.yml")
	if err != nil {
		t.Fatalf("read alerts.yml: %v", err)
	}
	cfg := testConfig()
	cfg.bufferMaxBytes = 32 << 20
	cfg.maxInflightBytes = 16 << 20
	cfg.spoolMaxBytes = 256 << 20
	out := newServer(cfg).m.render()

	seen := map[string]bool{}
	for _, tok := range metricNameRe.FindAllString(string(data), -1) {
		name := strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(tok, "_bucket"), "_sum"), "_count")
		if seen[name] {
			continue
		}
		seen[name] = true
		if !strings.Contains(out, "\n# TYPE "+name+" ") && !strings.HasPrefix(out, "# TYPE "+name+" ") {
			t.Errorf("alerts.yml references %q, which /metrics does not expose", name)
		}
	}
	if len(seen) < 8 {
		t.Fatalf("only %d logden_ metrics found in alerts.yml — the regexp is wrong", len(seen))
	}
}
