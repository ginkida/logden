// logden — production-шлюз приёма логов.
//
// Контракт: POST /logs с общим токеном в Authorization: Bearer.
// Тело: один объект, JSON-массив или NDJSON; поля {project, level, message,
// context, timestamp}. Шлюз валидирует, батчит, вставляет в ClickHouse с
// ретраями и (опционально) дисковым спулом. Эндпоинты: /healthz (liveness),
// /readyz (проверяет ClickHouse), /metrics (Prometheus), /version.
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
	"syscall"
	"time"
)

// Заполняется через -ldflags при сборке.
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
	batchSize      int
	flushInterval  time.Duration
	maxRetries     int
	spoolDir       string
	spoolMaxFiles  int
	replayInterval time.Duration

	rateLimit    float64
	rateBurst    float64
	metricsToken string

	maxBodyBytes    int64
	maxMessageBytes int
	maxContextBytes int
	maxBatchEvents  int
	retention       time.Duration

	trustedProxies []*net.IPNet
	logLevel       slog.Level
}

func main() {
	// Режим healthcheck для docker (distroless без shell/curl).
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

	srv := newServer(cfg)
	srv.ingest.start()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	// Фоновая проба ClickHouse: метрика logden_clickhouse_reachable обновляется
	// независимо от внешних /readyz (иначе алерт на неё мёртв без проб извне).
	go srv.ready.loop(ctx)

	httpServer := &http.Server{
		Addr:              cfg.listenAddr,
		Handler:           srv.mux(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
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

	// Бюджет shutdown должен укладываться в docker stop_grace_period (45s):
	// 15s на дренаж HTTP-соединений + дренаж буфера
	// (ceil(BUFFER_SIZE/BATCH_SIZE) × insertOnce 3s при draining).
	shCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shCtx); err != nil {
		slog.Error("http shutdown error", "err", err)
	}
	srv.ingest.stop() // дренируем буфер и флашим остаток
	slog.Info("drained, exiting")
}

func loadConfig() (config, error) {
	cfg := config{
		listenAddr:      envOr("LISTEN_ADDR", ":8080"),
		chBaseURL:       envOr("CLICKHOUSE_URL", "http://127.0.0.1:8123"),
		chUser:          envOr("CLICKHOUSE_USER", "writer"),
		chDatabase:      envOr("CLICKHOUSE_DB", "logs"),
		chTable:         envOr("CLICKHOUSE_TABLE", "logs"),
		bufferSize:      envInt("BUFFER_SIZE", 2000),
		batchSize:       envInt("BATCH_SIZE", 500),
		flushInterval:   envDur("FLUSH_INTERVAL", time.Second),
		maxRetries:      envInt("MAX_RETRIES", 3),
		spoolDir:        os.Getenv("SPOOL_DIR"),
		spoolMaxFiles:   envInt("SPOOL_MAX_FILES", 1000),
		replayInterval:  envDur("REPLAY_INTERVAL", 30*time.Second),
		rateLimit:       envFloat("RATE_LIMIT_RPS", 0),
		rateBurst:       envFloat("RATE_BURST", 0),
		maxBodyBytes:    int64(envInt("MAX_BODY_BYTES", 4<<20)),
		maxMessageBytes: envInt("MAX_MESSAGE_BYTES", 64<<10),
		maxContextBytes: envInt("MAX_CONTEXT_BYTES", 64<<10),
		maxBatchEvents:  envInt("MAX_BATCH_EVENTS", 1000),
		retention:       envDur("RETENTION", 30*24*time.Hour),
		logLevel:        parseLevel(envOr("LOG_LEVEL", "info")),
	}
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
