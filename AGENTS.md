# AGENTS.md — operating guide for AI agents

logden is a self-hosted centralized logging system: applications POST JSON log
events to a tiny Go gateway; the gateway batches and inserts them into
ClickHouse; logs are queried with plain SQL as a read-only user.

This file is written for LLM agents. Every command is copy-paste runnable from
the repository root (skip the `git clone … && cd` line if you already have a
checkout), and the expected result is stated so each step can be verified.
Humans: start with [README.md](README.md).

## Facts

| Fact | Value |
|---|---|
| Repository | `https://github.com/ginkida/logden` |
| Prebuilt image | `ghcr.io/ginkida/logden` (linux/amd64 + arm64, published on `v*` tags) |
| Write path | `POST /logs` on the gateway, port 8080 (binds to `127.0.0.1` by default) |
| Storage | ClickHouse table `logs.logs`, 30-day TTL, not exposed outside the Docker network |
| Read path | SQL as ClickHouse user `reader` (SELECT only, resource-limited) |
| Secrets (3) | `LOG_TOKEN` (apps → gateway), `CH_WRITER_PASSWORD` (gateway → ClickHouse), `CH_READER_PASSWORD` (read-only SQL) |
| Resource budget | gateway ~10–15 MB RAM, ClickHouse capped at 768 MB; fits a ~1 GB VPS |

## Stand up the full stack (Docker)

Prerequisites: `docker` with compose v2, `openssl`, `curl`. No other tools needed.

```bash
git clone https://github.com/ginkida/logden && cd logden
cp .env.example .env
# Generate the three secrets in place (idempotent, portable sed):
for var in LOG_TOKEN CH_WRITER_PASSWORD CH_READER_PASSWORD; do
  sed -i.bak "s/^$var=.*/$var=$(openssl rand -hex 32)/" .env
done && rm -f .env.bak
```

Then either build from source (leave `GATEWAY_IMAGE=` empty in `.env` — compose
builds and tags `logden:latest` itself):

```bash
docker compose up -d --build
```

or use the prebuilt image (no Go toolchain, no build; check
<https://github.com/ginkida/logden/releases> for the latest tag — `0.3.0` below
is an example):

```bash
sed -i.bak "s|^GATEWAY_IMAGE=.*|GATEWAY_IMAGE=ghcr.io/ginkida/logden:0.3.0|" .env && rm -f .env.bak
docker compose pull gateway && docker compose up -d
```

### Verify the deployment (run all four; stop on first failure)

| # | Command | Expected |
|---|---------|----------|
| 1 | `docker compose ps` | both containers `healthy` (ClickHouse needs ~15 s on first start) |
| 2 | `curl -fsS localhost:8080/healthz` | `ok` |
| 3 | `curl -fsS localhost:8080/readyz` | `ready` (503 means ClickHouse is not up yet — wait and retry) |
| 4 | see below | `204`, then one row returned |

```bash
set -a; . ./.env; set +a   # note: exports every variable from .env into this shell
curl -fsS -o /dev/null -w '%{http_code}\n' -X POST localhost:8080/logs \
  -H "Authorization: Bearer $LOG_TOKEN" \
  -d '{"project":"setup-check","level":"info","message":"hello from agent"}'
sleep 3   # wait for the batch flush (FLUSH_INTERVAL=1s) and async insert
docker compose exec clickhouse clickhouse-client \
  --user reader --password "$CH_READER_PASSWORD" \
  -q "SELECT level, message FROM logs.logs WHERE project='setup-check'"
```

If step 4 prints `info	hello from agent`, the system works end to end. If the
SELECT returns no rows, the async insert may not have flushed yet on a slow
machine — wait a few seconds and retry the SELECT before concluding failure.

## Send logs from an application

The entire write API is one endpoint:

```
POST /logs
Authorization: Bearer <LOG_TOKEN>      (or header: X-Log-Token: <LOG_TOKEN>)
Content-Type: application/json
Content-Encoding: gzip                 (optional)
```

Body: a single JSON object, a JSON array `[{...},{...}]`, or NDJSON (one object
per line). Fields per event:

| Field | Required | Rules |
|---|---|---|
| `project` | yes | your application name; `[A-Za-z0-9._-]`, max 64 chars. New projects need no registration — just send. |
| `message` | yes | non-empty; truncated beyond 64 KiB |
| `level` | no | normalized (`warn`→`warning`, `err`→`error`, `fatal`→`critical`); unknown → `info` |
| `context` | no | any JSON object; beyond 64 KiB it is DISCARDED and replaced by `{"_truncated":true,"_orig_bytes":N}` |
| `timestamp` | no | RFC3339 or unix sec/ms; >5 min in the future or older than retention → the gateway's ingest time is used |

Responses: `204` accepted; `400` invalid; `401` bad token; `405` wrong method;
`413` too large (body > 4 MiB **or** more than 1000 events); `429` rate-limited;
`503` buffer full **or** admission control shedding (`Retry-After: 1` — retry later).
Batches are accepted partially: invalid events are skipped, `400` only if every
event is invalid. Max 1000 events per request.

Zero-dependency client libraries with optional batching: `clients/go`,
`clients/python/logden_client.py`, `clients/node/logden.mjs`; Laravel/Monolog
example in `examples/LoggerGatewayHandler.php`. Clients do **not** retry —
delivery reliability (retries, disk spool, replay) lives between the gateway
and ClickHouse.

## Query logs (read-only SQL)

From the host, through the compose service:

```bash
docker compose exec clickhouse clickhouse-client \
  --user reader --password "$CH_READER_PASSWORD" \
  -q "SELECT timestamp, project, level, message FROM logs.logs
      WHERE project='billing-api' AND level='error'
        AND timestamp > now() - INTERVAL 1 HOUR
      ORDER BY timestamp DESC LIMIT 100"
```

- Ready-made queries (errors by project, frequency spikes, storage usage):
  `clickhouse/queries.sql`.
- Full-text search: `hasToken(message, 'word')` — uses the token index.
  `LIKE '%word%'` works but scans everything.
- `context` is stored as a JSON string: `JSONExtractString(context, 'key')`.
- To reach ClickHouse over HTTP from the host (e.g. for an MCP server), copy
  `docker-compose.override.yml.example` → `docker-compose.override.yml`
  (publishes `8123` on `127.0.0.1` only) and restart. Details:
  `docs/agent-access.md`.

## Operate

Gateway endpoints: `/healthz` (liveness), `/readyz` (readiness, checks
ClickHouse), `/metrics` (Prometheus), `/version`.

Metric signals (prefix `logden_`):

| Signal | Meaning | Action |
|---|---|---|
| `logden_logs_dropped_total` > 0 | logs are being **lost** (buffer and spool full, or quarantine) | see RUNBOOK |
| `logden_spool_files` growing | ClickHouse is down; gateway spools to disk and will replay automatically | usually self-heals |
| `logden_clickhouse_reachable` = 0 | ClickHouse unreachable | `docker compose logs clickhouse` |
| `logden_spool_quarantined_total` > 0 | ClickHouse rejected spooled batches (schema mismatch); files kept as `*.ndjson.bad` | RUNBOOK “Spool: .bad quarantine” |

Operational procedures (token rotation without downtime, retention change,
disk-full recovery, backup/restore): [RUNBOOK.md](RUNBOOK.md). Prometheus alert
rules: `deploy/alerts.yml`.

## Production exposure rules

- The gateway listens on `127.0.0.1` by default (`GATEWAY_BIND` in `.env`).
  For internet exposure put a TLS reverse proxy in front ([SECURITY.md](SECURITY.md))
  — or set `GATEWAY_BIND=0.0.0.0` deliberately (plain HTTP, token in cleartext).
- Behind a proxy, set `TRUSTED_PROXIES` to the proxy CIDR, otherwise
  `source_ip` records the proxy address.
- Set `METRICS_TOKEN`, or `/metrics` is public (the gateway warns at startup).
- Never publish ClickHouse ports (8123/9000/9363) externally.

All gateway configuration is environment variables only — no config files.
The full variable list with defaults: `.env.example` (compose) or
`deploy/logden.env.example` (bare metal / systemd). Source of truth:
`loadConfig` in `gateway/main.go`.

## Develop in this repository

Three independent Go modules (no go.work): `gateway/`, `clients/go/`,
`tools/loadtest/`. Go ≥ 1.25. Run Go commands from the module directory.

```bash
make test          # unit tests with -race; no ClickHouse needed (stubbed via httptest)
make lint          # go vet + staticcheck (staticcheck is fetched from the network)
make build         # static gateway binary → gateway/logden
cd gateway && go test -race -run TestInsertPipeline .   # single test
# integration tests (need a real ClickHouse; silently SKIPPED without CLICKHOUSE_URL):
cd gateway && CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_USER=default \
  CLICKHOUSE_PASSWORD=ci go test -tags=integration ./...
```

Hard rules (enforced in review and CI):

1. The gateway uses **Go stdlib only** — no external dependencies in `gateway/go.mod`.
2. Memory budget: the target host is ~1 GB RAM; do not grow the gateway or ClickHouse footprint.
3. Changing the live table schema → `ALTER` in `clickhouse/migrations.sql`;
   `schema.sql` only applies to fresh installs.
4. Any change to the `/logs` contract or the ClickHouse schema → entry in `CHANGELOG.md`.
5. Repository language is English (docs, comments, commit messages).

Invariants that are easy to break (read the surrounding comments first):
the INSERT column list in `gateway/ingest.go` must match the `row` struct in
`gateway/validate.go` and the table schema; `wait_for_async_insert=1` is set in
the insert URL (the server profile deliberately has it off); the shutdown order
in `ingester.stop()` and the enqueue mutex discipline must not change; the
ClickHouse profile name `readonly` is load-bearing (renaming silently removes
the reader's read-only protection); metric names are referenced by
`deploy/alerts.yml`.
