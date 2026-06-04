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
Data in the `ch-data` volume survives the restart. Logs accumulated during the outage sit in the
spool (`gw-spool` volume) and **are replayed automatically** once it recovers (replay every
`REPLAY_INTERVAL`). Loss only happens if both the buffer and the spool overflow
(`logden_logs_dropped_total` > 0).

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

## Changing retention

`schema.sql` (`INTERVAL 30 DAY`) — for NEW installs only. On a live table:
```bash
docker compose exec clickhouse clickhouse-client -q \
  "ALTER TABLE logs.logs MODIFY TTL toDateTime(timestamp) + INTERVAL 14 DAY DELETE"
```

## Disk full

```bash
docker compose exec clickhouse clickhouse-client -q \
  "SELECT partition, formatReadableSize(sum(bytes_on_disk)) FROM system.parts
   WHERE database='logs' AND active GROUP BY partition ORDER BY partition"
# drop the oldest partition:
docker compose exec clickhouse clickhouse-client -q "ALTER TABLE logs.logs DROP PARTITION '20260101'"
```
CH system logs are capped at 3 days (`config.d/system-logs.xml`).

## Monitoring (ad-hoc queries)

```bash
ch() { docker compose exec -T clickhouse clickhouse-client -q "$1"; }
ch "SELECT count() FROM logs.logs WHERE timestamp > now()-INTERVAL 1 HOUR"     # throughput
ch "SELECT formatReadableSize(value) FROM system.asynchronous_metrics WHERE metric='MemoryResident'"
ch "SELECT status, count() FROM system.asynchronous_insert_log WHERE event_time > now()-INTERVAL 1 HOUR GROUP BY status"
curl -s localhost:8080/metrics | grep -E 'logden_(logs|spool|clickhouse)'
```

## Backup / restore

```bash
./deploy/backup.sh                       # BACKUP into Disk('backups') + 30-day rotation
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

## Scaling

- **Vertically:** raise `mem_limit` and `max_server_memory_usage` in proportion to RAM.
- **Stateless gateway:** scales horizontally behind an L4/L7 load balancer (one shared token);
  each instance has its own spool — no data is lost.
- **Bottleneck** is write throughput: grow `BATCH_SIZE`/`async_insert_max_data_size`.
- **>tens of millions of rows/day** — beyond the "lightweight" single-node design;
  you'll need a ClickHouse cluster/sharding (out of scope).
