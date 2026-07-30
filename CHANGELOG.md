# Changelog

Format — [Keep a Changelog](https://keepachangelog.com/), versioning — [SemVer](https://semver.org/).

## [Unreleased]

### Fixed
- **`logs.logs.timestamp` is pinned to `DateTime64(3, 'UTC')`.** The gateway writes
  a naive UTC string, and a column without a timezone is parsed in the *server's*
  timezone: on a host set to `Asia/Almaty` every row landed 5 hours off (measured),
  taking the partition day and the TTL with it. Existing installations: apply the
  `MODIFY COLUMN` from `clickhouse/migrations.sql` (metadata-only, instant); rows
  already written on a non-UTC server keep their shift. The integration test now
  fails if the pin is removed.
- **Admission control charges request bodies as they are read** instead of
  reserving the worst case upfront. Two consequences: an authenticated client
  that announced a chunked or gzip body and then stalled used to hold a full
  `MAX_BODY_BYTES` reservation for the whole `ReadTimeout` (four sockets were
  enough to answer `503` to everyone else for 30s), and every gzip or chunked
  request — however small — cost `MAX_BODY_BYTES`, capping concurrency at 4 with
  the shipped defaults. The budget is charged on the decompressed stream, so a
  gzip bomb still costs what it inflates to; the measured worst-case RSS is
  unchanged (~85 MB).
- **Spool no longer overwrites undelivered batches after a restart.** File names
  are `<pid>-<seq>.ndjson` and the gateway is always PID 1 in Docker, so a fresh
  process regenerated the previous run's names and `rename` clobbered batches
  that were never inserted. The sequence is now resumed from the spool directory
  (quarantined `.bad` files keep their number reserved), and replay orders files
  by that sequence instead of lexicographically by name — the unpadded PID prefix
  used to dominate the sort, letting fresh batches jump the backlog.
- **A `4 MiB` JSON array could allocate ~65 MiB of heap.** The array branch
  unmarshalled every element before checking `MAX_BATCH_EVENTS`; it now streams
  and enforces the cap while decoding, so a request holds at most
  `MAX_BATCH_EVENTS + 1` elements — memory the inflight-byte semaphore never
  accounted for. Anything but whitespace after the array is still a `400` —
  including a stray `]` or `}`, which a `Decoder.More()` check would have let in.
- **Every row now carries a timestamp stamped at ingest** instead of relying on
  ClickHouse `DEFAULT now64(3)` at insert time: batches replayed from the spool
  after an outage were stored with the replay time, hours off. A usable client
  `timestamp` still wins.
- Inserts are no longer retried when ClickHouse answers `400` (the data itself is
  rejected): the retries could not succeed and their backoff stalled the worker
  while the buffer filled. Such a batch goes straight to the spool, where replay
  quarantines it as `.bad`.
- A fractional `RATE_LIMIT_RPS` (e.g. `0.5`) rejected **every** request with
  `429` forever — the bucket could never hold the one token `allow()` spends.
  The burst is now floored at 1 token.
- `CLICKHOUSE_URL` with a trailing slash produced `//?query=...` and broke every
  insert; it is now trimmed, and a URL with a path prefix is refused at startup.
- `LOG_TOKEN` is no longer split on spaces and tabs, only on commas and newlines:
  a passphrase containing a space became several independently valid tokens.
- `/readyz` is unauthenticated and every concurrent request past the cache TTL
  fired its own ClickHouse probe; the readiness cache is now single-flight.
- Spool robustness: batches are `fsync`ed and the spool directory is `fsync`ed
  after the rename (the durability path must survive a power loss, not just a
  crash); a leftover `.tmp` from a failed rename is removed instead of holding
  disk inside `SPOOL_MAX_BYTES`; an unreadable spool file is logged instead of
  skipped silently; and a delivered file that cannot be removed is retired to
  `.delivered` rather than being re-inserted on every replay tick forever.
- `docker/initdb/20-users.sh` hashes the ClickHouse passwords (`sha256_hash`)
  instead of interpolating them into SQL literals: a password containing a quote
  or a backslash used to abort the first-time init or create a user whose
  password differed from the gateway's, and a failing statement could echo the
  secret into the container log.
- Non-UTF-8 bytes in `context` are replaced before the `MAX_CONTEXT_BYTES` check,
  and rows are serialized without HTML escaping — `<`, `>`, `&` and invalid bytes
  used to expand up to 6x on the wire and break the "row <= 2x body" budget the
  `BUFFER_MAX_BYTES` floor is built on.
- An oversized **compressed** body is answered `413` (`too_large`) instead of a
  misleading `400` (`read_error`). An oversized body is `413` even when the
  inflight budget is what refused its last byte (possible when
  `MAX_INFLIGHT_BODY_BYTES == MAX_BODY_BYTES`, which config validation allows),
  instead of blaming load with a `503`.
- `.env.example` / `deploy/logden.env.example` claimed the byte caps only need to
  be `>= MAX_BODY_BYTES`, while startup validation requires `BUFFER_MAX_BYTES >=
  2x MAX_BODY_BYTES` — following the docs produced an exit-2 crash loop.
- Clients: `Flush()` splits into requests bounded by both `MAX_BATCH_EVENTS` and
  `MAX_BODY_BYTES`. Concurrent `Log()` calls could push a batch over the event
  cap between flushes, and large contexts over the body cap, and the gateway
  rejects such a request as a whole — losing every event in it. Both halves of a
  byte-split (and every remaining chunk of a flush) are always attempted, with
  the first error surfaced at the end: stopping at the first failure would drop
  the sibling events, which the buffer no longer holds.
- `docker-compose.yml` asked for `cpus: "1.5"` on ClickHouse. The Docker daemon
  refuses a value above the host's CPU count outright, so on the 1-vCPU VPS this
  profile targets the container was never created and — through
  `depends_on: service_healthy` — the whole stack failed to start. Now `1.0`,
  with a note about raising it on a bigger box.
- Node client: the batch timer is `unref`'d, so a CLI or worker can exit without
  `close()`; a new `onError` option routes non-batch send failures to a sink
  instead of an unawaited rejection that crashes the process.
- `examples/LoggerGatewayHandler.php` reads its settings through `config()`
  instead of `env()`, which returns `null` after `php artisan config:cache` —
  the standard production step — and silently sent every record to `null/logs`.
- `clickhouse/queries.sql`: the context-search example had no time bound, so it
  scanned every partition and hit the reader profile's 30s `max_execution_time`;
  it now filters by time and narrows the scan with `hasToken` first. The memory
  query asked for `MemoryTracking` in `system.asynchronous_metrics`, where it does
  not exist (it is a `CurrentMetric`, i.e. `system.metrics`) and returned nothing.
- Docs corrected against the code: `413` also means "too many events", `503` also
  means admission control, `405` existed but was undocumented; an oversized
  `context` is discarded, not truncated; log loss is not "only when buffer and
  spool are both full" (quarantine and shed traffic lose logs too, and clients
  never retry); `.delivered` files count toward `SPOOL_MAX_BYTES`; `METRICS_TOKEN_FILE`
  and the `CLICKHOUSE_URL` path rule were missing from the config table.
- `deploy/backup.sh` refuses to run when free space is below the table size plus
  `MIN_FREE_BYTES` (2 GB) instead of filling the volume the database lives on,
  rotates before backing up, keeps `RETAIN_DAYS` (7, was 30 — every backup is a
  FULL copy of the table) and reports a failed rotation instead of hiding it.
  It is also executable now: the documented cron line `./deploy/backup.sh` could
  not have worked with mode 644.
- `deploy/logden.service` sets `TimeoutStopSec=45` explicitly (matching compose's
  `stop_grace_period`): on a host with a low `DefaultTimeoutStopSec` systemd would
  SIGKILL the gateway mid-drain and the buffered events would be lost.

### Added
- Metrics: `logden_buffer_capacity_bytes` and
  `logden_inflight_body_capacity_bytes`, so byte-based saturation is visible
  (with large events the byte budget fills long before the event count does).
- `logden_http_requests_total` now covers every endpoint, not only `/logs`:
  unmatched paths are counted under `path="other"` (a fixed allowlist — request
  paths must never reach a metric label), so 404 storms and rejected `/metrics`
  scrapes are visible.
- Metric `logden_logs_truncated_total{field}` — messages and contexts cut for
  exceeding their size cap were only visible inside the stored row.
- Alerts: `LogdenBufferBytesNearFull` and `LogdenTrafficShed` (traffic rejected
  by admission control or the rate limiter is log loss — clients don't retry).
- Clients stamp `timestamp` themselves (Go/Python/Node and the PHP example), so
  the event keeps its own time end to end.
- Opt-in memory probe: `LOGDEN_MEM_PROBE=1 go test -run WorstCaseHeap` drives the
  worst case the byte caps admit and fails if the peak RSS leaves `mem_limit`.
  Measured ~85 MB peak RSS with the shipped defaults (README).
- CI now also builds/vets `tools/loadtest` and syntax-checks the Node, Python and
  PHP clients — three modules and three files that nothing verified before. The
  client checks live in the existing `lint-extra` job on purpose: a brand-new job
  would not be in the required-checks list and could fail without blocking a merge.
- The integration test pins the timestamp contract against a real ClickHouse: the
  ingest stamp must parse as `DateTime64(3)` and a client timestamp must survive
  to the millisecond (a stub server accepts any string, so only a real server
  catches a bad layout).

### Changed
- A request whose events are **all** invalid is now counted as
  `logden_logs_rejected_total{reason="all_invalid"}`; `reason="invalid_event"`
  keeps its per-event meaning (the same label used to be incremented twice).
- Startup warns when `MAX_BATCH_EVENTS > BUFFER_SIZE` — a full-size client batch
  can then never be accepted, not even on an idle gateway.

## [0.2.2] — 2026-06-06

### Added
- Memory safety caps in the gateway (all configurable, 0 = off):
  `MAX_INFLIGHT_BODY_BYTES` (default 16 MiB) bounds concurrent request bodies —
  a burst of large uploads now degrades to `503` + `Retry-After` (reason
  `overloaded`) instead of risking an OOM kill; `BUFFER_MAX_BYTES` (default
  32 MiB) bounds the buffer and the in-flight batch by bytes on top of the
  event-count cap; batches also flush early at 8 MiB.
- `SPOOL_MAX_BYTES` (default 256 MiB) caps the spool directory on disk;
  quarantined `*.bad` files count toward the cap.
- New metrics: `logden_buffer_bytes`, `logden_inflight_body_bytes`,
  `logden_spool_bytes`, `logden_spool_capacity_bytes`,
  `logden_process_start_time_seconds`.
- New alerts (`deploy/alerts.yml`): `ClickHouseDiskLow` (<2 GB free on the data
  path), `LogdenSpoolBytesNearCap` (>80% of `SPOOL_MAX_BYTES`),
  `LogdenRestartLoop` (>2 restarts in 15m — OOM-kill loop detector).
- README: honest worst-case RAM budget and a swap setup snippet for 1 GB boxes.

### Changed
- Gateway HTTP server sets `MaxHeaderBytes` 32 KiB (Go default is 1 MB).
- `deploy/logden.service` now sets `GOMEMLIMIT=80MiB` (previously the GC had no
  ceiling on bare metal) and raises `MemoryMax` 64M→96M / `MemoryHigh` 48M→88M
  to match the docker profile.
- Compose: gateway `mem_limit` 96m→128m (headroom for the worst case; average
  usage is unchanged), new caps surfaced as compose variables.
- ClickHouse `mark_cache_size` 256 MB→128 MB — generous for a single narrow
  table; lowers the RSS-overshoot OOM risk within the same 768 MB cap.

## [0.2.1] — 2026-06-05

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
