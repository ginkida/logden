# RUNBOOK

Operating logden on a single ~1 GB box. Commands for the docker stack (from the repo root).

## ClickHouse unreachable / crashed

**Symptom:** `/readyz` returns 503; `logden_clickhouse_insert_failed_total`
and `logden_spool_files` climb in the metrics; `insert failed after retries` in the gateway logs.

```bash
docker compose ps
docker compose logs clickhouse --tail=200
dmesg | grep -i oom            # common cause on a 1GB box — OOM
docker compose restart clickhouse
```
If OOM kills recur, add swap as a safety margin (snippet in README, "RAM budget")
or move to a 2 GB box — the worst-case ceilings don't fit 1 GB without swap.
Data in the `ch-data` volume survives the restart. Logs accumulated during the outage sit in the
spool (`gw-spool` volume) and **are replayed automatically** once it recovers (replay every
`REPLAY_INTERVAL`). Loss shows up in `logden_logs_dropped_total` (buffer or spool full, or a
batch quarantined as `.bad`) and in `logden_logs_rejected_total` (rate limit, admission
control, bad token, invalid event) — clients do not retry, so both count.

ClickHouse's own complaints are queryable too: `text_log` is deliberately enabled at
`warning` (stock ClickHouse ships it off), and it is the only durable copy once the
container's stderr has rotated (10 MB × 3). Run it as `default` — `reader` has no grant
on that table:
```bash
docker compose exec clickhouse clickhouse-client -q \
  "SELECT event_time, level, message FROM system.text_log
   WHERE event_time > now()-INTERVAL 1 HOUR ORDER BY event_time DESC LIMIT 50"
```

**"Container is unhealthy".** The ClickHouse healthcheck is
`wget --spider http://clickhouse:8123/ping` aimed at the docker-network name — deliberately
not a `clickhouse-client` probe, because from that address `default` is rejected by
`docker/clickhouse-access.xml` (loopback only) and the container would never turn healthy.
`/ping` needs no auth. Cold-start budget: `start_period` 60 s, then 30 attempts at 5 s;
failures inside `start_period` don't count against `retries`. Still red past that is a real
failure, not a slow boot — read the logs above rather than raising `retries`.

## Rotating the shared token (no downtime)

`LOG_TOKEN` accepts several comma-separated tokens.
```bash
NEW=$(openssl rand -hex 32)
# 1) add the new one alongside the old:  LOG_TOKEN=old,new  in .env
docker compose up -d gateway
# 2) move clients onto the new token
# 3) drop the old one:  LOG_TOKEN=new  in .env
docker compose up -d gateway
```
Bare metal: edit `/etc/logden.env` + `systemctl restart logden`.

Both restarts are graceful: the gateway drains its buffer to ClickHouse or the spool
before exiting. The budget is 15 s of HTTP shutdown + one insert that was already in
flight (15 s, it keeps its pre-drain timeout) + `ceil(BUFFER_SIZE/BATCH_SIZE) × 3 s`
≈ 42 s at the defaults, which is why `stop_grace_period` (docker-compose.yml) and
`TimeoutStopSec` (deploy/logden.service) are both 60 s. Raising `BUFFER_SIZE` or
lowering `BATCH_SIZE` lengthens the drain by 3 s per extra full batch — recompute both
deadlines, or SIGKILL lands mid-drain and the buffered events never reach the spool.

## Rotating the ClickHouse passwords

`CREATE USER OR REPLACE` in both init paths makes a re-run the supported rotation.
Docker (the users live in the `ch-data` volume, so initdb will not run again):

```bash
NEW_WRITER=$(openssl rand -hex 32)
docker compose exec -e CH_WRITER_PASSWORD="$NEW_WRITER" clickhouse \
  /docker-entrypoint-initdb.d/20-users.sh
# then set CH_WRITER_PASSWORD=$NEW_WRITER in .env and: docker compose up -d gateway
```
The script recreates **both** accounts every time. Only the writer is overridden above;
the reader is re-created from the `CH_READER_PASSWORD` the container was started with, so
if you had changed it in `.env` without recreating the container, that edit is what gets
rolled back — pass `-e CH_READER_PASSWORD=…` explicitly whenever the two might disagree.

Bare metal: re-run `clickhouse/users.sql` through the `sed` hash recipe in README
("Installation without Docker"). Either way `OR REPLACE` drops the user together with
its grants — everything these two accounts need is re-granted by the same file, but a
grant added by hand elsewhere is lost and must be re-applied.

After rotating `writer`, the gateway needs the new `CLICKHOUSE_PASSWORD`
(`CH_WRITER_PASSWORD` in `.env`) and a restart, or every insert fails to authenticate
and the spool starts filling.

## Changing retention

`schema.sql` (`INTERVAL 30 DAY`) — for NEW installs only. On a live table:
```bash
docker compose exec clickhouse clickhouse-client -q \
  "ALTER TABLE logs.logs MODIFY TTL toDateTime(timestamp) + INTERVAL 14 DAY DELETE"
```
Move the gateway's `RETENTION` in the same step (`.env` → `docker compose up -d
gateway`, or `/etc/logden.env` → `systemctl restart logden`). It is the window
the gateway accepts client timestamps in, not a second TTL: an event older than
`RETENTION` is still stored, but under the ingest time instead of its own. Watch
`logden_logs_restamped_total{reason="too_old"}` after the change — a rising
counter means senders are backfilling outside the new window.

## Disk full

```bash
docker compose exec clickhouse clickhouse-client -q \
  "SELECT partition, formatReadableSize(sum(bytes_on_disk)) FROM system.parts
   WHERE database='logs' AND active GROUP BY partition ORDER BY partition"
# drop the oldest partition:
docker compose exec clickhouse clickhouse-client -q "ALTER TABLE logs.logs DROP PARTITION '20260101'"
```
CH system logs are capped at 3 days (`config.d/system-logs.xml`), and the two metric logs
are sampled every 30 s rather than every second — same number of parts per day, ~30× fewer
rows in them. `query_thread_log`, `query_views_log`, `trace_log` and `session_log` are off
entirely; `text_log` is deliberately on at `warning`, where a healthy server writes nothing,
because container stderr is capped at 10 MB × 3 and rotates an incident away.
The `ClickHouseDiskLow` alert (`deploy/alerts.yml`) fires below 2 GB free on the
data path — don't wait for a 100% full disk, ClickHouse stops merging well before that.
Other disk consumers to check: in-volume backups (`deploy/backup.sh` keeps
`RETAIN_DAYS`, 7 by default — each one is a FULL copy of the table) and the
gateway spool (capped at `SPOOL_MAX_BYTES`, 256 MB by default).

## Monitoring (ad-hoc queries)

```bash
ch() { docker compose exec -T clickhouse clickhouse-client -q "$1"; }
ch "SELECT count() FROM logs.logs WHERE timestamp > now()-INTERVAL 1 HOUR"     # throughput
ch "SELECT formatReadableSize(value) FROM system.asynchronous_metrics WHERE metric='MemoryResident'"
   # ^ async metrics refresh every 30s (asynchronous_metrics_update_period_s), so this
   #   spot check can lag half a minute behind an RSS climb. The alerts are unaffected:
   #   both ClickHouse rules use for: 10m or longer.
ch "SELECT status, count() FROM system.asynchronous_insert_log WHERE event_time > now()-INTERVAL 1 HOUR GROUP BY status"
curl -s localhost:8080/metrics | grep -E 'logden_(logs|spool|clickhouse)'
```

## Backup / restore

```bash
./deploy/backup.sh                       # rotate (RETAIN_DAYS=7), check free space, then BACKUP
# It refuses to run when free space < table size + MIN_FREE_BYTES (2 GB): a full
# disk stops ClickHouse accepting inserts, which costs more than a missed backup.
RETAIN_DAYS=14 MIN_FREE_BYTES=$((5*1024*1024*1024)) ./deploy/backup.sh   # e.g. on a bigger disk
# restore: RESTORE won't overwrite an existing table —
# restore into a temporary name and swap them:
docker compose exec clickhouse clickhouse-client -q \
  "RESTORE TABLE logs.logs AS logs.logs_restored FROM Disk('backups','logs-YYYYMMDD-HHMMSS.zip')"
docker compose exec clickhouse clickhouse-client -q \
  "EXCHANGE TABLES logs.logs_restored AND logs.logs"
docker compose exec clickhouse clickhouse-client -q "DROP TABLE logs.logs_restored"
```
RESTORE/EXCHANGE need admin rights: run them as `default` (as above —
`docker compose exec` enters from inside the container over loopback); `reader`/`writer`
won't do. For DR, copy backups to another host (see the comment in
`deploy/backup.sh`).

## Spool: `.bad` quarantine

A batch that ClickHouse rejected as invalid (HTTP 400 — e.g. the spool was written
before an incompatible schema migration) is renamed to `*.ndjson.bad` on replay and
pulled out of the queue so it doesn't block replaying the other files. Signals:
`logden_spool_quarantined_total` > 0 (the events also count toward
`logden_logs_dropped_total`) and `spool batch rejected by clickhouse` in the gateway logs.

Once the cause is fixed, the files can be requeued (`gw-spool` volume; the
distroless gateway has no shell, so use a helper container):
```bash
docker run --rm -v logden_gw-spool:/spool alpine \
  sh -c 'cd /spool && for f in *.ndjson.bad; do mv "$f" "${f%.bad}"; done'
```
`.bad` files keep counting toward `SPOOL_MAX_BYTES` (see `logden_spool_bytes` /
the `LogdenSpoolBytesNearCap` alert) — if they are not worth requeueing, delete
them the same way (`rm -- *.ndjson.bad`), otherwise they squeeze out fresh batches.

`*.ndjson.delivered` is a different case: the batch **was** inserted, but the file
could not be deleted (a read-only or full volume — check the gateway log for
`spool file delivered but not removed`). Renaming it keeps replay from inserting it
again on every tick; the content is already in ClickHouse, so delete these files
(`rm -- *.ndjson.delivered`) and fix the volume. Like `.bad`, they keep counting
toward `SPOOL_MAX_BYTES` until removed, squeezing out fresh batches.

## Scaling

- **Vertically:** raise `mem_limit` and `max_server_memory_usage` in proportion to RAM.
- **Stop deadlines are load-bearing, not padding.** Both services get
  `stop_grace_period: 60s`. Compose stops in reverse dependency order, so the gateway
  drains into a live ClickHouse and only then does ClickHouse see SIGTERM — at which point
  it still has to flush the async-insert queue and the system-log buffers (up to a 30 s
  flush interval) and unwind a merge that can cover
  `max_bytes_to_merge_at_max_space_in_pool` = 1 GiB on one vCPU. Docker's default 10 s
  SIGKILLs through all of that.
- **Stateless gateway:** scales horizontally behind an L4/L7 load balancer (one shared token);
  each instance has its own spool — no data is lost.
- **Bottleneck** is write throughput: grow `BATCH_SIZE`/`async_insert_max_data_size`.
- **>tens of millions of rows/day** — beyond the "lightweight" single-node design;
  you'll need a ClickHouse cluster/sharding (out of scope).
