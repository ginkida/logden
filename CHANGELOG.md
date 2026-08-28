# Changelog

Format — [Keep a Changelog](https://keepachangelog.com/), versioning — [SemVer](https://semver.org/).

## [Unreleased]

## [0.4.0] — 2026-08-28

**Upgrading an existing installation.** The shipped files already carry every
change below; this is what an operator has to do or know.

1. **Stop deadlines move to 60s for both services** — `stop_grace_period` in
   `docker-compose.yml` (gateway *and* ClickHouse) and `TimeoutStopSec` in
   `deploy/logden.service`. A custom override that is still at 45s (or Docker's
   default 10s for ClickHouse) SIGKILLs mid-drain.
2. **`TRUSTED_PROXIES` must now list *every* hop**, not just the nearest one:
   `source_ip` is resolved right to left, and an unlisted intermediate becomes
   the recorded address.
3. **`/logs` error bodies changed shape** (see Changed). A client that only
   reads the status code needs no change; one that parsed the plain-text body
   does.
4. **`clickhouse/users.sql` can no longer be piped in as-is** — it now holds
   `sha256_hash` sentinels instead of a credential, and ClickHouse aborts on the
   first statement. Bare-metal installs must use the `sed` substitution recipe
   in the file's header (also in README, "Installation without Docker"). Nothing
   changes for an already-created user; this only affects a fresh install or a
   rotation.
5. **The ClickHouse healthcheck changed** to `wget --spider .../ping` with a 60s
   `start_period`. Anyone watching `docker compose ps` on a cold first start
   should expect the container to sit `starting` for up to a minute instead of
   ~15s. Warm restarts are unaffected.
6. **Upgrading a bundled client is a behaviour change even though the API is
   additive.** Batch mode now reports failures instead of swallowing them, so an
   application that relied on silence will start seeing lines (`log` in Go,
   `console.error` in Node, stderr in Python) — pass an explicit no-op sink to
   keep the old behaviour. In Go, batch-mode `Log` now always returns `nil`. An
   event that is oversized on its own is dropped with a report instead of being
   posted into a certain `413`. The default request timeout is now 5 s in all
   three (it was 2 s in Node and Python). The Python client no longer honours
   `http_proxy`/`https_proxy` and no longer follows redirects.
7. Reader queries that exceed `max_result_rows` now fail instead of returning a
   silently truncated result.

Nothing else requires action.

### Fixed
- **`X-Forwarded-For` could be spoofed in the deployment SECURITY.md recommends.**
  The gateway read the leftmost entry of the chain, but every appending proxy
  (nginx's `$proxy_add_x_forwarded_for`, Caddy's `reverse_proxy`) forwards
  `<client-supplied value>, <peer it actually saw>` — so a client behind a
  trusted proxy chose its own `source_ip`. The chain is now walked right to left
  and the first entry that is not itself a trusted proxy wins; when the whole
  chain is trusted the leftmost entry is kept (an internal sender behind the same
  proxy, which the peer would collapse onto the proxy address), and an entry that
  does not parse fails closed onto the real peer. Repeated header *lines* are
  read as one chain — Go never merges them, so a client that sent its own line
  kept it ahead of the proxy's. The walk is capped at the last 16 hops and slices
  rather than splits, so a padded 32 KiB header cannot allocate a huge slice on
  the accept path.
- **A shutdown could overrun `stop_grace_period` and lose the buffer.**
  `insertWithRetry` read the drain flag once, before its retry loop, so a batch
  already in flight when `stop()` ran kept the full retry schedule (~10s at the
  defaults) — the opposite of the single-attempt drain `stop()` promises. The
  flag is now re-read before every retry, and the batch goes straight to the
  spool. The documented budget is corrected accordingly: 15s HTTP shutdown + one
  in-flight insert that keeps its pre-drain 15s timeout +
  `ceil(BUFFER_SIZE/BATCH_SIZE) × 3s` ≈ 42s at the defaults, so
  `stop_grace_period` and `TimeoutStopSec` move from 45s to 60s.
- **Invalid UTF-8 in `message` amplified 3x and could push the gateway past its
  `mem_limit`.** `json.Unmarshal` rewrites every bad byte inside a string as a
  3-byte U+FFFD, so a 4 MiB body of binary garbage — legal under every byte cap —
  decoded and re-serialized as ~12 MiB, three times what the inflight budget was
  charged for. Each element is now sanitized run-wise before the decoder sees it,
  collapsing a whole run to one replacement; the valid path only pays a
  `utf8.Valid` scan. The worst-case row expansion is back within the ~2x the
  `BUFFER_MAX_BYTES` floor is sized on.
- **`CLICKHOUSE_URL` with a query string, fragment or credentials is rejected at
  startup.** The insert and readiness URLs are built as `chBaseURL + "/?query=…"`,
  so a fragment swallowed the whole query — ClickHouse parsed the NDJSON body as
  SQL, answered 400, and every batch was spooled and quarantined as `.bad`.
- **A single unserializable event no longer costs the whole batch** in the Go,
  Node and Python clients. An encode failure (a NaN, a cycle, a BigInt, an object
  that throws) rejected the entire in-flight chunk of up to 1000 events; the
  clients now isolate the offending event and send it with its `context` replaced
  by `{"_unserializable": true}` — the same key in all three, so one ClickHouse
  query finds every degraded row — so `message`, `level` and `timestamp` still
  land. The Python client
  additionally no longer dies on non-finite floats or lone surrogates, and
  re-initializes its lock, buffer and flusher thread after `fork()` instead of
  inheriting a possibly frozen lock.
- **Ten documented settings were silently ignored under docker-compose.**
  `docker-compose.yml` passed only 17 of the 27 variables `loadConfig` reads, so
  `CLICKHOUSE_DB`, `CLICKHOUSE_TABLE`, `MAX_RETRIES`, `SPOOL_MAX_FILES`,
  `RATE_BURST`, `MAX_BODY_BYTES`, `MAX_BATCH_EVENTS`, `MAX_MESSAGE_BYTES`,
  `MAX_CONTEXT_BYTES` and `RETENTION` looked effective when set in `.env` and did
  nothing. All 27 are now wired with the Go defaults verbatim; `.env.example` and
  `deploy/logden.env.example` document every one, marking the five that
  compose pins so nobody sets them expecting effect.
- **`examples/LoggerGatewayHandler.php` normalizes its context** inside `write()`
  (Laravel overwrites a handler's formatter unless the channel sets one), mapping
  `Throwable` to a bounded structure and converting NAN/INF, so a record with an
  exception in its context is no longer lost to an encode error.
- **Batch mode in all three clients discarded every failure in silence.** The
  background flusher swallowed send errors, so a wrong token or an unreachable
  gateway meant the application logged into nothing, indefinitely, with no
  signal — the worst outcome available to a logging client. Every background
  failure now reaches an error sink (`WithOnError` / `onError` / `on_error`), and
  the default is not silence: one line through the stdlib `log` package in Go,
  `console.error` in Node, one stderr line in Python. The sink call is wrapped,
  because a sink that throws on a background goroutine/thread/timer callback had
  no caller frame to catch it and would take the process down.
- **Batch mode ran the flush on the caller's stack.** Filling the buffer called
  `flush()` inline, so a multi-megabyte encode plus the gateway's whole latency
  landed on whichever application code path happened to record that event. A full
  buffer now only wakes the background flusher (a capacity-1 channel in Go, a
  one-shot `setImmediate` in Node, a `threading.Event` in Python); `flush()` and
  `close()` stay synchronous, since an explicit flush is the caller asking to
  wait.
- **The client batch buffer was unbounded**, so an outage turned every logged
  line into permanent heap and the client could OOM the process it was installed
  to observe. It is now capped (`WithMaxBuffer` / `maxBuffer` / `max_buffer`;
  10000 events in all three, so the same deployment gets the same ceiling in
  every language) and overflow
  drops the OLDEST events — the newest describe the incident that is still
  happening. Drops are counted and reported once per flush, not once per event,
  which would turn a log burst into a second burst inside the application. The
  caller is never blocked: backpressure here would push the outage onto the
  application's own request path.
- **An event too large to send was posted anyway and then forgotten.** The byte
  split bottomed out by shipping a single event over `MAX_BODY_BYTES`, a
  guaranteed `413` that also spent the gateway's inflight budget, after which the
  event was gone — it had already left the buffer. All three now report it
  (`ErrEventTooLarge` / a thrown `Error` / `OversizedEvent`) with project, level,
  timestamp and sizes, and never the payload: that payload is what made the event
  oversized, and the sink usually lands in the very log it did not fit into.
- **The gateway's rejection reason reaches the application.** A failed send used
  to carry only a status code, so `400` could not be distinguished from `400`.
  The clients now parse `{"error":"<reason>"}` and expose it (`*GatewayError`
  with `StatusCode`/`Reason`, `error.status`/`error.reason`,
  `GatewayError.status`/`.reason`). Only a bounded prefix of the body is read
  (4 KiB / 1 KiB / 512 B), control characters are stripped and the reason capped
  at 120 characters — a wrong URL can land on any endpoint, and remote text must
  not be able to forge a line in the application's own log.
- **Go client: `Flush` kept only the first chunk error.** Chunks fail for
  independent reasons, and first-wins hid an `ErrEventTooLarge` report behind an
  earlier network error; it now joins them with `errors.Join`.
- **Node client: two flushes could splice the same buffer.** The timer's flush
  and a caller's `flush()` each took from the head, so an earlier chunk could
  reach the gateway after a later one. Every flush is now serialized on one
  promise chain.
- **Python client opened a new TCP connection per request** (`urllib`) and had no
  context manager. It now speaks `http.client` with a small LIFO idle pool (at
  most 4), guarded by a lock that is never held across I/O, so a batching client
  pays one handshake instead of one per flush. A pooled socket the gateway closed
  on its 60s idle timeout is retried exactly once on a fresh connection — and
  only when the socket came from the pool, so an unreachable gateway is not
  retried and no delivery can be duplicated.
- **Python client: a forked child could stall `close()` for 5s.** `_check_fork`
  left `self._thread` pointing at the parent's `Thread` object, whose tstate lock
  is a frozen copy, so the child joined a thread that could never finish. The
  child now also clears the thread, re-creates the connection lock, resets the
  drop counter and closes the inherited sockets (closing the child's descriptor
  only — the parent's connection survives).
- **The ClickHouse healthcheck was answered by the temporary init server.**
  `clickhouse-client -q 'SELECT 1'` over loopback is answered by the server the
  entrypoint runs with `--listen_host=127.0.0.1` to execute
  `/docker-entrypoint-initdb.d`, so the container turned healthy and
  `depends_on: service_healthy` released the gateway *before* `20-users.sh` had
  created `writer` — and its first inserts failed authentication. The probe is
  now `wget --spider http://clickhouse:8123/ping` against the docker-network name
  the gateway itself uses: `/ping` needs no auth (a `clickhouse-client` probe from
  that address would be rejected by `docker/clickhouse-access.xml`, which pins
  `default` to loopback), the temporary server does not bind it, so a green check
  now also proves init finished — and it no longer re-executes the fat clickhouse
  binary inside the 850m cgroup every 5s.
- **`docker/initdb/20-users.sh` was mode 0644**, so the image entrypoint sourced
  it instead of executing it. The users were still created, which is why it went
  unnoticed, but `set -euo pipefail` leaked into the entrypoint's own shell for
  the rest of init, its temporary-server teardown and its final `exec`, and the
  missing-password `exit 1` aborted the whole entrypoint rather than just this
  step. Now 0755, and the header says it must stay that way.
- **`clickhouse/users.sql` shipped a working credential.** It carried
  `CHANGE_ME_writer` as a real password behind `CREATE USER IF NOT EXISTS`, so a
  run that was never corrected produced accounts whose passwords are published in
  this repository — and the corrective re-run afterwards was a silent no-op. The
  file now holds `sha256_hash` sentinels that ClickHouse refuses outright (not 64
  hex characters), documents the substitution recipe, and uses
  `CREATE USER OR REPLACE` so a re-run is the supported rotation.
- **ClickHouse had Docker's default 10s stop grace.** Compose stops in reverse
  dependency order, so it receives SIGTERM only after the gateway has drained into
  it — with the async-insert queue and the system-log buffers still to flush and a
  merge of up to `max_bytes_to_merge_at_max_space_in_pool` (1 GiB) to unwind on
  one vCPU. Now `stop_grace_period: 60s`.

### Added
- `Retry-After` on the buffer-full `503` (admission-control `503`s already had
  it), `Allow: POST` on the `405` from `/logs`, and a
  `WWW-Authenticate: Bearer` challenge on the `401` from `/logs` and `/metrics`.
  The buffer-full path deliberately stays off `logden_logs_rejected_total` —
  `deploy/alerts.yml` keys a separate rule on that counter.
- First test suites for the Node and Python clients, and gateway tests covering
  the drain, the forwarded-for walk and the UTF-8 sanitizer. The gateway also
  grew stdlib fuzz targets over the request-parsing surface, and CI now fails if
  the integration, Node or Python suites report that they ran nothing.
- **Per-project counters in `/metrics`:** `logden_project_logs_received_total{project}`
  and `logden_project_logs_dropped_total{project}` answer "who is flooding me?"
  and "who went silent?" without a SQL round trip. The label set is capped at 64
  distinct projects — `project` comes off the wire, so an unbounded set would let
  a sender grow `/metrics` without limit; everything past the cap folds into
  `project="<overflow>"`, a value the project charset cannot produce. Admission
  is sticky, so an established sender keeps its series through a flood of
  generated names, and the two gauges `logden_project_labels_tracked` /
  `logden_project_labels_capacity` make the saturation visible.
- `logden_logs_restamped_total{reason="future"|"too_old"}` counts events whose
  client timestamp was out of range and replaced with the ingest time. That
  rewrite always happened; it was previously invisible, so a fleet with a skewed
  clock or a backfill outside `RETENTION` looked healthy while losing the real
  event time.
- Alert rules for all three: `LogdenProjectSilent`, `LogdenProjectLabelsExhausted`
  and `LogdenTimestampsRestamped` in `deploy/alerts.yml`.
- Operator queries in `clickhouse/queries.sql`: a project inventory, a
  self-relative spike detector on `(project, level)`, a silence detector, an
  error-signature drill-down, and rows/disk per day from `system.parts`.
- **The full PSR-3 level vocabulary in every client.** `debug`, `notice`,
  `warning` and `critical` join `info`/`error` as helpers, matching
  `allowedLevels` in `gateway/validate.go`; `alert` and `emergency` stay reachable
  through the generic `log()`. Go and Node keep the older `warn` spelling as an
  alias of `warning` (Python never had one), so nothing existing stops compiling
  or running.
- **Client cap overrides in all three**, for an operator who retuned the gateway:
  `WithLimits(maxBatchEvents, maxBodyBytes)` in Go, `maxBatch`/`maxBodyBytes` in
  Node, `max_batch=`/`max_body_bytes=` in Python. They default to the gateway's
  own 1000 / 4 MiB and must stay at or below what the gateway enforces — only its
  copy is authoritative, and a value above it makes every request a `413`. A
  non-positive or unparseable override keeps the default rather than removing the
  cap. In all three the flush trigger is clamped to
  `min(batch, maxBatchEvents, maxBuffer)`, since a trigger above the buffer cap
  could never fire: the drop policy evicts an event for every new one, so the
  buffer never reaches `batch` and only the interval would ever send.
- Python client: a context manager (`with LoggerClient(...) as log:`), an
  exception hierarchy the application can catch (`LogdenError`, `GatewayError`,
  `DroppedEvents`, `OversizedEvent`), and endpoint validation at construction —
  a bad scheme raises there instead of on every send, and a path prefix is
  honoured (`http://host/edge` posts to `/edge/logs`).
- `docker/initdb/20-users.sh` reads `CH_WRITER_PASSWORD_FILE` /
  `CH_READER_PASSWORD_FILE`, the same `*_FILE` pattern the gateway already
  implements, so the compose-`secrets:` opt-out now exists end to end instead of
  aspirationally. The trailing newline an editor adds is stripped, since the
  gateway trims the same file and the two would otherwise present different
  secrets.
- `clients/README.md` documents the three as one client, with a capability matrix
  whose four remaining asymmetries (the `warn` alias, transport override, flush
  error joining, connection-reuse mechanism) each carry the language-level reason
  they exist, rather than being folklore.

### Changed
- **`/logs` errors answer JSON.** Every failing status (`400`, `401`, `405`,
  `413`, `429`, `503`) now returns `{"error":"<reason>"}` with
  `Content-Type: application/json; charset=utf-8` instead of the plain status
  text. The reason is the same closed vocabulary that labels
  `logden_logs_rejected_total`, so a sender can finally tell an invalid project
  name from an oversized body — the two need opposite fixes, and previously the
  difference was visible only to whoever ran the gateway. Statuses, metric names
  and the reason vocabulary are unchanged. The buffer-full `503` answers
  `{"error":"buffer_full"}`, a response-only string deliberately kept out of
  `logden_logs_rejected_total` (a rule keys on that counter). `/metrics`,
  `/readyz` and `/healthz` keep the plain-text bodies scrapers and container
  probes expect.
- **The `reader` profile aborts an oversized result instead of truncating it.**
  `result_overflow_mode` moves from `break` to `throw` in
  `clickhouse/users.d/low-mem.xml`: `break` returned a truncated aggregate with
  no error the caller could see, so a `GROUP BY` over `max_result_rows` silently
  answered wrong — worst for the agent/MCP consumer `docs/agent-access.md`
  describes. Memory protection is unchanged; the cost is one retry with a
  `LIMIT` instead of a wrong conclusion. `users.d/*.xml` is hot-reloaded, so no
  restart is needed.
- **The default per-request client timeout is 5 s in all three** (Node was
  2000 ms, Python 2.0 s; Go was already 5 s). A full 1000-event / 4 MiB flush over
  a loaded link does not finish in two seconds, and the abort costs the whole
  chunk, which `flush()` has already taken out of the buffer. Pass `timeout` (Node
  ms, Python seconds) or `WithHTTPClient` to go back to a tighter deadline.
- **Batch-mode `Log` in the Go client always returns `nil`.** When the call
  returns, the event is only buffered — the send has not happened — so the return
  value could never carry its outcome. Failures go to the `WithOnError` sink
  instead. Direct (non-batch) mode is unchanged and still returns the send error,
  as do `Flush` and `Close`.
- **The Python client no longer honours `http_proxy`/`https_proxy` and no longer
  follows redirects** (a `3xx` is now a `GatewayError`). Both fall out of speaking
  `http.client` directly instead of `urllib`, and both are deliberate for an
  internal gateway; dropping the proxy lookup also removes the fork-unsafe macOS
  system-proxy read.
- **ClickHouse system logs are cut by collection rate, not just retention.**
  `metric_log` gains `collect_interval_milliseconds` 30000 and
  `asynchronous_metrics_update_period_s` becomes 30 — roughly 30x fewer rows at
  an identical part count, because the flush intervals are untouched. `part_log`
  and `asynchronous_insert_log` move to a 30s flush (the 7.5s default drips tiny
  parts on a box where these tables feed themselves). `metric_log` and
  `asynchronous_metric_log` are kept rather than disabled: Prometheus is external
  and optional in this stack, so on a box without it these are the only
  post-mortem record. The one visible cost is that RUNBOOK's `MemoryResident`
  spot check can lag 30s; both ClickHouse alerts use `for: 10m` or longer and are
  unaffected.
- Secrets stay plain environment variables in `docker-compose.yml`, now with the
  reasoning and the residual exposure written down (`docker inspect`,
  `docker compose config`, the container config JSON, `/proc/<pid>/environ`) plus
  the exact opt-out. On a single-VPS deployment compose `secrets:` relocates the
  secret rather than protecting it — reading them needs the docker socket, which
  is root, which already reads the `.env` beside them.

## [0.3.0] — 2026-07-30

**Upgrading an existing installation:** apply the `MODIFY COLUMN timestamp
DateTime64(3, 'UTC')` migration from `clickhouse/migrations.sql` (metadata-only,
instant) — without it, a ClickHouse server whose timezone is not UTC stores every
row shifted by its offset. Check with `SELECT timezone()`; on `UTC` nothing was
shifted and the migration is still worth applying so the column is unambiguous.
Nothing else requires action: the gateway is drop-in, and the `/logs` contract is
backward compatible.

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
