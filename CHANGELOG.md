# Changelog

Format — [Keep a Changelog](https://keepachangelog.com/), versioning — [SemVer](https://semver.org/).

## [Unreleased]

### Added
- `deploy/prometheus.yml.example` — example scrape config wired to `deploy/alerts.yml`
  (job names `logden`/`clickhouse`).
- Dependabot now also tracks the pinned ClickHouse image in `docker-compose.yml`
  (`docker-compose` ecosystem).

### Fixed
- Release images now embed the actual build time in `BUILD_DATE`
  (was the repository-metadata timestamp).
- Clients cap the batch size at the gateway's `MAX_BATCH_EVENTS` (1000) instead of
  letting an oversized batch be rejected with 413.
- The Laravel/Monolog example sends the event as an array, matching the client contract.
- CI/Release: pinned actions bumped (checkout v6, setup-go v6, metadata-action v6,
  buildx v4, hadolint v3.3) — Node 20 runner deprecation; builder image golang 1.26-alpine.

## [0.2.0] — 2026-06-04

### Added
- Ingest pipeline in the gateway: bounded buffer, batching, retries with backoff,
  disk spool and automatic replay (durability while ClickHouse is down).
- Batch ingest: JSON array, NDJSON, gzip body; accepts a client-supplied `timestamp`.
- Validation: `level` whitelist/normalization, `project` check, limits on
  message/context/body/event count.
- Observability: Prometheus `/metrics`, readiness `/readyz`, `/version`,
  structured logging (slog/JSON). Built-in ClickHouse prometheus endpoint (:9363).
- Security: multiple tokens (rotation), rate limiting, `TRUSTED_PROXIES`
  for X-Forwarded-For, secrets from files (`*_FILE`), container hardening
  (read_only, cap_drop, no-new-privileges), default ClickHouse restricted to loopback.
- ClickHouse prod config: `ttl_only_drop_parts`, tokenbf skip indexes,
  TTL for system logs, async_insert tuning, profile/quotas for `reader`,
  native BACKUP disk.
- Operations: graceful shutdown, healthcheck mode for the binary, Makefile,
  RUNBOOK/SECURITY/CONTRIBUTING, release workflow (GHCR multi-arch + SBOM/provenance),
  images pinned by digest, resource limits and log rotation in compose.
- Fail-fast validation of the gateway config at startup (clear errors instead of a panic under load).
- Prometheus alert rules (`deploy/alerts.yml`) and the `logden_buffer_capacity` metric (for a buffer-fill alert).
- CI hardening: hadolint, shellcheck, yamllint (`--strict`), promtool check rules; Dependabot (gomod/actions/docker).
- Reusable batching clients in `clients/` (Go package + test, Python, Node) — no external dependencies.
- Optional `/metrics` authorization via `METRICS_TOKEN`.
- Docs on agent/MCP access to ClickHouse (`docs/agent-access.md`) + an example loopback compose override.
- Load generator `tools/loadtest` (+ `make loadtest`) for throughput testing.
- Tests: unit (auth, validation, batch, spool/replay, clientIP, metrics, config) + ClickHouse integration.
- Spool quarantine: a batch rejected by ClickHouse (HTTP 400 — e.g. after an incompatible
  migration) is renamed to `*.ndjson.bad` and does not block replay of the rest of the queue;
  new `logden_spool_quarantined_total` metric (events are also counted in `dropped`).
- Background ClickHouse probe: `logden_clickhouse_reachable` updates even without external
  `/readyz` requests — the `LogdenClickHouseUnreachable` alert works out of the box.
- Startup warning when `METRICS_TOKEN` is empty (open `/metrics`).
- `GATEWAY_IMAGE` in compose: run a prebuilt image from ghcr without a local build.
- Safe defaults: the gateway port in compose binds to 127.0.0.1 (`GATEWAY_BIND`
  to override); CI workflow with minimal `permissions: contents: read`.

### Fixed
- Shutdown: fixed the `send on closed channel` panic (the channel is closed synchronously with enqueue) and unbounded drain (spool replay is interrupted by the stop signal).
- NDJSON with a syntactically broken line now returns `400` (observable) instead of silently dropping the valid tail.
- Clients: Node fire-and-forget no longer causes unhandledRejection; Go `Close()` is idempotent; Python `close()` joins the background flusher.
- `Authorization: Bearer` is accepted case-insensitively (RFC 7235).
- Fixed staticcheck SA4000 in the rate limiter test.
- Spool: orphaned `*.tmp` files (a crash between write and rename) are removed at startup.
- Python client: the background flusher survives send errors (previously the first network
  error killed the thread for good); batch mode is fire-and-forget, like Node.
- CI/Release: all GitHub Actions are pinned by commit SHA, `prom/prometheus` — by digest.
- Docs: bare-metal install requires `clickhouse-access.xml` (access_management) before
  `users.sql`; a working RESTORE procedure (via a temp table + EXCHANGE);
  an explicit note that clients do not retry.

## [0.1.0]
- Initial version: thin gateway (`POST /logs`, shared token, single insert),
  ClickHouse schema with 30-day TTL, docker-compose, init schema/users.
