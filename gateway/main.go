// logden — production log ingest gateway.
//
// Contract: POST /logs with a shared token in Authorization: Bearer.
// Body: a single object, a JSON array, or NDJSON; fields {project, level,
// message, context, timestamp}. The gateway validates, batches, and inserts
// into ClickHouse with retries and (optionally) a disk spool. Endpoints:
// /healthz (liveness), /readyz (checks ClickHouse), /metrics (Prometheus),
// /version.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

// Populated via -ldflags at build time.
var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

type config struct {
	listenAddr string
	tokens     []string

	chBaseURL  string
	chUser     string
	chKey      string
	chDatabase string
	chTable    string

	bufferSize     int
	bufferMaxBytes int64
	batchSize      int
	flushInterval  time.Duration
	maxRetries     int
	spoolDir       string
	spoolMaxFiles  int
	spoolMaxBytes  int64
	replayInterval time.Duration

	rateLimit    float64
	rateBurst    float64
	metricsToken string

	maxBodyBytes     int64
	maxInflightBytes int64
	maxMessageBytes  int
	maxContextBytes  int
	maxBatchEvents   int
	retention        time.Duration

	trustedProxies []*net.IPNet
	logLevel       slog.Level
}

func main() {
	// Healthcheck mode for docker (distroless has no shell/curl).
	if len(os.Args) > 1 && os.Args[1] == "-healthcheck" {
		os.Exit(healthcheckProbe())
	}

	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config error:", err)
		os.Exit(2)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.logLevel})))
	if cfg.metricsToken == "" {
		slog.Warn("METRICS_TOKEN is not set — /metrics is publicly accessible")
	}
	if cfg.maxBatchEvents > cfg.bufferSize {
		// Not fatal (small batches keep working), but a full-size batch can then
		// never be enqueued, not even on an idle gateway: every one is a 503.
		slog.Warn("MAX_BATCH_EVENTS exceeds BUFFER_SIZE — a full-size client batch can never be accepted",
			"max_batch_events", cfg.maxBatchEvents, "buffer_size", cfg.bufferSize)
	}

	srv := newServer(cfg)
	srv.ingest.start()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	// Background ClickHouse probe: the logden_clickhouse_reachable metric is
	// updated independently of external /readyz calls (otherwise the alert on it
	// is dead without probes from outside).
	go srv.ready.loop(ctx)

	httpServer := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           srv.mux(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10, // a token + proxy headers fit easily; the Go default (1MB) is a needless OOM vector
	}

	go func() {
		slog.Info("logden listening",
			"addr", cfg.listenAddr, "clickhouse", cfg.chBaseURL, "version", version,
			"batch_size", cfg.batchSize, "buffer", cfg.bufferSize, "spool", cfg.spoolDir != "")
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutdown signal received, draining")

	// The shutdown budget must fit within docker stop_grace_period (45s):
	// 15s to drain HTTP connections + buffer drain
	// (ceil(BUFFER_SIZE/BATCH_SIZE) × insertOnce 3s while draining).
	shCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shCtx); err != nil {
		slog.Error("http shutdown error", "err", err)
	}
	srv.ingest.stop() // drain the buffer and flush the remainder
	slog.Info("drained, exiting")
}

func loadConfig() (config, error) {
	cfg := config{
		listenAddr:       envOr("LISTEN_ADDR", ":8080"),
		chBaseURL:        envOr("CLICKHOUSE_URL", "http://127.0.0.1:8123"),
		chUser:           envOr("CLICKHOUSE_USER", "writer"),
		chDatabase:       envOr("CLICKHOUSE_DB", "logs"),
		chTable:          envOr("CLICKHOUSE_TABLE", "logs"),
		bufferSize:       envInt("BUFFER_SIZE", 2000),
		bufferMaxBytes:   int64(envInt("BUFFER_MAX_BYTES", 32<<20)),
		batchSize:        envInt("BATCH_SIZE", 500),
		flushInterval:    envDur("FLUSH_INTERVAL", time.Second),
		maxRetries:       envInt("MAX_RETRIES", 3),
		spoolDir:         os.Getenv("SPOOL_DIR"),
		spoolMaxFiles:    envInt("SPOOL_MAX_FILES", 1000),
		spoolMaxBytes:    int64(envInt("SPOOL_MAX_BYTES", 256<<20)),
		replayInterval:   envDur("REPLAY_INTERVAL", 30*time.Second),
		rateLimit:        envFloat("RATE_LIMIT_RPS", 0),
		rateBurst:        envFloat("RATE_BURST", 0),
		maxBodyBytes:     int64(envInt("MAX_BODY_BYTES", 4<<20)),
		maxInflightBytes: int64(envInt("MAX_INFLIGHT_BODY_BYTES", 16<<20)),
		maxMessageBytes:  envInt("MAX_MESSAGE_BYTES", 64<<10),
		maxContextBytes:  envInt("MAX_CONTEXT_BYTES", 64<<10),
		maxBatchEvents:   envInt("MAX_BATCH_EVENTS", 1000),
		retention:        envDur("RETENTION", 30*24*time.Hour),
		logLevel:         parseLevel(envOr("LOG_LEVEL", "info")),
	}
	// Both the insert URL and the readiness probe are built by concatenating
	// chBaseURL + "/?query=..."; a trailing slash would produce "//" and every
	// request would miss ClickHouse's query handler.
	cfg.chBaseURL = strings.TrimRight(cfg.chBaseURL, "/")
	cfg.chKey = readSecret("CLICKHOUSE_PASSWORD")
	cfg.metricsToken = readSecret("METRICS_TOKEN")
	cfg.tokens = splitTokens(readSecret("LOG_TOKEN"))
	if len(cfg.tokens) == 0 {
		return cfg, errors.New("LOG_TOKEN (or LOG_TOKEN_FILE) is required")
	}
	tp, err := parseCIDRs(os.Getenv("TRUSTED_PROXIES"))
	if err != nil {
		return cfg, fmt.Errorf("TRUSTED_PROXIES: %w", err)
	}
	cfg.trustedProxies = tp
	if cfg.rateBurst == 0 {
		cfg.rateBurst = cfg.rateLimit
	}
	if err := cfg.validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func healthcheckProbe() int {
	host, port, err := net.SplitHostPort(envOr("LISTEN_ADDR", ":8080"))
	if err != nil {
		host, port = "127.0.0.1", "8080"
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode == http.StatusOK {
		return 0
	}
	return 1
}
