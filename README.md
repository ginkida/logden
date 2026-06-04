# logden

A compact centralized logging system for multiple projects.
Any service ships a log with a single HTTP POST → a tiny Go gateway batches and
writes to ClickHouse → analyze with plain SQL. Built for a ~1 GB RAM VPS, with
minimal dependencies (the gateway uses only the Go stdlib).

[![CI](https://github.com/ginkida/logden/actions/workflows/ci.yml/badge.svg)](https://github.com/ginkida/logden/actions/workflows/ci.yml)

> **AI agents:** see [AGENTS.md](AGENTS.md) — a step-by-step operating guide
> with verifiable commands for deploying, sending logs, and querying.

## Features

- **Simple universal API** — `POST /logs` with a shared token; a single object,
  a JSON array, NDJSON, or gzip. Any client can send: Laravel, Node, Python, cron, bash.
- **Reliability (on the gateway side)** — batching, retries with backoff, a disk spool,
  and automatic replay: logs survive a brief outage and restart of
  ClickHouse. Clients, meanwhile, are deliberately thin and do not retry (see `clients/`).
- **Observability** — Prometheus `/metrics`, readiness `/readyz`, `/version`,
  structured logging (slog/JSON).
- **Compactness** — the gateway uses ~10-15 MB RAM, ClickHouse is tuned for 768 MB; logs
  compress 10-30×, with automatic partition-based retention.
- **Security** — shared token (with rotation), rate limiting, input validation,
  read-only/cap-drop container, ClickHouse not exposed externally.

## Architecture

```
any service (Laravel / Node / Python / cron / bash)
        │  POST /logs   { project, level, message, context, timestamp? }
        │  Authorization: Bearer <shared token>
        ▼
┌──────────────────────────────────────┐
│   logden (Go, ~10-15 MB RAM)          │
│   token → validation → buffer → batch │
│   retries + backoff → disk spool      │
│   /metrics /readyz /version           │
└──────────────────┬───────────────────┘
                   │ batch INSERT (JSONEachRow, wait_for_async_insert)
                   ▼
            ┌──────────────┐
            │  ClickHouse  │  table logs.logs, TTL 30 days, cap 768 MB
            └──────┬───────┘
                   │ SQL, read-only (reader)
                   ▼
                agent / analysis
```

ClickHouse is not published externally; only the gateway port is reachable from outside.

## API contract

```
POST /logs
Authorization: Bearer <LOG_TOKEN>
Content-Type: application/json
Content-Encoding: gzip            (optional)

{
  "project":   "billing-api",     // required: [A-Za-z0-9._-], up to 64 chars
  "level":     "error",           // opt.; normalized (warn→warning), defaults to info
  "message":   "Payment timeout",  // required; truncated if it exceeds the limit
  "context":   { "order_id": 123 },// opt.; any JSON
  "timestamp": "2026-06-02T10:00:00Z" // opt.; RFC3339 or unix (sec/ms); otherwise insertion time
}
```

- **Batch:** the body may be a JSON array `[ {...}, {...} ]` or NDJSON (one object per line).
- **Partial accept:** invalid batch elements are skipped (the
  `logden_logs_rejected_total{reason="invalid_event"}` metric), valid ones are accepted; the whole
  batch is rejected (`400`) only if none are valid.
- Response: `204 No Content`. Errors: `400` (validation), `401` (token), `413` (size),
  `429` (rate limit), `503` (buffer full).
- The token can be sent in `Authorization: Bearer <…>` or in the `X-Log-Token: <…>` header.
- `source_ip` is set by the gateway (see `TRUSTED_PROXIES`).

### Service endpoints

| Path        | Purpose                                                 |
|-------------|---------------------------------------------------------|
| `/healthz`  | liveness (always 200, does not touch ClickHouse)        |
| `/readyz`   | readiness (200/503 — checks ClickHouse availability)    |
| `/metrics`  | Prometheus metrics                                      |
| `/version`  | version/commit/build date                               |

## Quick start (Docker)

```bash
cp .env.example .env
# set LOG_TOKEN and passwords: openssl rand -hex 32
docker compose up -d --build
```

Without building from source — use the prebuilt image from ghcr (published on release tag `v*`):
```bash
# in .env: GATEWAY_IMAGE=ghcr.io/ginkida/logden:0.2.0
docker compose pull gateway && docker compose up -d
```

Check:
```bash
set -a; . ./.env; set +a
curl -fsS -X POST http://localhost:8080/logs \
  -H "Authorization: Bearer $LOG_TOKEN" \
  -d '{"project":"demo","level":"error","message":"hello"}'

docker compose exec clickhouse clickhouse-client \
  --user reader --password "$CH_READER_PASSWORD" \
  -q "SELECT * FROM logs.logs ORDER BY timestamp DESC LIMIT 5"
```

The gateway port `:8080` listens on the host loopback only by default (`GATEWAY_BIND=127.0.0.1`) —
expose it externally through a TLS reverse proxy (see [SECURITY.md](SECURITY.md)) or
deliberately set `GATEWAY_BIND=0.0.0.0`. ClickHouse is not published externally at all. The
`ch-data` volume holds ClickHouse data, the `gw-spool` volume — the gateway buffer for outages.

## Installation without Docker (bare metal)

1. **ClickHouse** — copy `clickhouse/config.d/*` and `clickhouse/users.d/*`
   into `/etc/clickhouse-server/`, plus `docker/clickhouse-access.xml` →
   `/etc/clickhouse-server/users.d/` (grants `default` the right to create
   users for `users.sql` and locks it to loopback). Restart, then:
   ```bash
   clickhouse-client --multiquery < clickhouse/schema.sql
   # change the passwords in users.sql:
   clickhouse-client --multiquery < clickhouse/users.sql
   ```
2. **Gateway** — `make build`, put the binary in `/usr/local/bin/`, configure
   `deploy/logden.env.example` → `/etc/logden.env`, install
   `deploy/logden.service` and `systemctl enable --now logden`.

## Clients

Ready-made modules with batching — `clients/` (Go package, Python, Node).
**bash** — `examples/curl.sh`. **Laravel** — `examples/LoggerGatewayHandler.php`.

Clients are deliberately thin: they **do not retry and do not spool**; if the gateway is
unavailable the event (in batch mode — the whole batch, silently) is lost. All delivery
reliability lives on the gateway → ClickHouse leg.

**Node.js**
```js
await fetch(`${process.env.LOG_GATEWAY_URL}/logs`, {
  method: 'POST',
  headers: { Authorization: `Bearer ${process.env.LOG_TOKEN}`, 'Content-Type': 'application/json' },
  body: JSON.stringify({ project: 'web', level: 'error', message: 'boom', context: { path: '/checkout' } }),
});
```

**Python**
```python
import os, requests
requests.post(f"{os.environ['LOG_GATEWAY_URL']}/logs",
    headers={"Authorization": f"Bearer {os.environ['LOG_TOKEN']}"},
    json={"project": "worker", "level": "warning", "message": "retry", "context": {"attempt": 3}},
    timeout=2)
```

## Gateway configuration (env)

| Variable             | Default                  | Description                                     |
|----------------------|--------------------------|-------------------------------------------------|
| `LOG_TOKEN`          | —                        | shared token(s), comma-separated; required       |
| `LISTEN_ADDR`        | `:8080`                  | listen address                                  |
| `CLICKHOUSE_URL`     | `http://127.0.0.1:8123`  | ClickHouse address                              |
| `CLICKHOUSE_USER`    | `writer`                 | user for inserts                                |
| `CLICKHOUSE_PASSWORD`| —                        | password (or `*_FILE`)                          |
| `BATCH_SIZE`         | `500`                    | batch size                                      |
| `BUFFER_SIZE`        | `2000`                   | in-memory buffer capacity                       |
| `FLUSH_INTERVAL`     | `1s`                     | flush interval                                  |
| `MAX_RETRIES`        | `3`                      | insert retries before spooling                  |
| `SPOOL_DIR`          | (empty)                  | disk spool directory; empty = disabled          |
| `REPLAY_INTERVAL`    | `30s`                    | spool replay interval                           |
| `RATE_LIMIT_RPS`     | `0`                      | request rate limit per second (0 = off)          |
| `RATE_BURST`         | `0` (=`RATE_LIMIT_RPS`)  | token bucket burst size                          |
| `TRUSTED_PROXIES`    | (empty)                  | CIDR of trusted proxies for `X-Forwarded-For`   |
| `METRICS_TOKEN`      | (empty)                  | token for `/metrics`; empty = open              |
| `LOG_LEVEL`          | `info`                   | gateway log level                               |
| `MAX_MESSAGE_BYTES`  | `65536`                  | message size cap                                |
| `MAX_CONTEXT_BYTES`  | `65536`                  | context size cap                                |
| `MAX_BODY_BYTES`     | `4194304`                | whole request body cap (source of 413)          |
| `MAX_BATCH_EVENTS`   | `1000`                   | maximum events per request                      |
| `SPOOL_MAX_FILES`    | `1000`                   | cap on the number of batches in the spool        |
| `RETENTION`          | `720h`                   | reject client timestamps older than (≈ TTL)     |
| `CLICKHOUSE_DB` / `_TABLE` | `logs` / `logs`    | database/table name                             |

The source of truth for config is `loadConfig` in `gateway/main.go`.
Secrets can be supplied via file: `LOG_TOKEN_FILE`, `CLICKHOUSE_PASSWORD_FILE`.

## Analysis

Ready-made queries — `clickhouse/queries.sql` (analytics + monitoring). Connect with the
read-only `reader` user. Full-text search — via `hasToken(message, …)`
(uses a skip index). Connecting an agent/MCP to the read-only `reader` —
[docs/agent-access.md](docs/agent-access.md).

## Observability

`/metrics` exposes (Prometheus): `logden_logs_received_total`,
`logden_logs_inserted_total`, `logden_logs_dropped_total`,
`logden_clickhouse_insert_failed_total`, `logden_clickhouse_insert_retries_total`,
`logden_clickhouse_reachable`, `logden_spool_files`, `logden_buffer_events`,
`logden_clickhouse_insert_duration_seconds` (histogram), `logden_build_info`.
ClickHouse has its built-in prometheus endpoint enabled on `:9363` (not published externally).
Ready-made alert rules — `deploy/alerts.yml` (drops, CH unavailability, spool growth,
buffer fill, insert latency, ClickHouse memory).

## Production

- **TLS** — the token and logs go over HTTP; put a reverse proxy (caddy/nginx) with
  TLS in front of the gateway, and have the gateway itself listen on loopback. See [SECURITY.md](SECURITY.md).
- **Memory limits** — `docker-compose.yml` sets `mem_limit` (a hard
  cgroup cap on top of the soft `max_server_memory_usage`). Check it for your box.
- **Operations** — [RUNBOOK.md](RUNBOOK.md): what to do when ClickHouse goes down,
  token rotation, changing retention, backup, scaling.
- **Image versions** are pinned by digest for reproducibility.
- DO NOT publish the ClickHouse ports (8123/9000/9363).

## RAM budget (1 GB box)

| Component        | RAM     |
|------------------|---------|
| ClickHouse (cap) | ~768 MB (cgroup `mem_limit` 850m) |
| logden           | ~10-15 MB (`mem_limit` 96m, `GOMEMLIMIT` 80MiB) |
| OS + overhead    | ~150 MB |

Measured at idle: gateway ~2 MB, ClickHouse ~190 MB. Gateway memory under load ≈
`BUFFER_SIZE` × average row size; if you raise `BUFFER_SIZE`, raise `mem_limit`/`GOMEMLIMIT`.
On 1 GB it is comfortable at defaults; for headroom 2 GB is better.

## Reliability: what is guaranteed

The gateway responds `204` after enqueuing into the buffer, then writes to ClickHouse in a batch with
acknowledgment (`wait_for_async_insert=1`) and retries. When ClickHouse is unavailable, the batch
goes to the disk spool and is replayed after recovery —
logs are not lost on a brief outage/database restart. The disk spool is enabled via
`SPOOL_DIR` (set by default in Docker); without it, durability during an outage/shutdown
is not guaranteed. Loss is possible only when both the buffer and the spool are full (then
`logden_logs_dropped_total` grows and `503` is returned).

## License

[MIT](LICENSE).
