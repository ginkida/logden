# RUNBOOK

Эксплуатация logden на одиночном ~1 ГБ-боксе. Команды для docker-стека (из корня репо).

## ClickHouse недоступен / упал

**Симптом:** `/readyz` отдаёт 503; в метриках растёт `logden_clickhouse_insert_failed_total`
и `logden_spool_files`; в логах шлюза `insert failed after retries`.

```bash
docker compose ps
docker compose logs clickhouse --tail=200
dmesg | grep -i oom            # частая причина на 1ГБ — OOM
docker compose restart clickhouse
```
Данные в томе `ch-data` переживают рестарт. Логи, накопленные за простой, лежат в
спуле (том `gw-spool`) и **сами зальются** после восстановления (реплей раз в
`REPLAY_INTERVAL`). Потеря только если переполнились и буфер, и спул
(`logden_logs_dropped_total` > 0).

## Ротация общего токена (без даунтайма)

`LOG_TOKEN` поддерживает несколько токенов через запятую.
```bash
NEW=$(openssl rand -hex 32)
# 1) добавить новый рядом со старым:  LOG_TOKEN=old,new  в .env
docker compose up -d gateway
# 2) перевести клиентов на новый токен
# 3) убрать старый:  LOG_TOKEN=new  в .env
docker compose up -d gateway
```
Bare metal: правка `/etc/logden.env` + `systemctl restart logden`.

## Смена ретеншна

`schema.sql` (`INTERVAL 30 DAY`) — только для НОВЫХ инсталляций. На живой таблице:
```bash
docker compose exec clickhouse clickhouse-client -q \
  "ALTER TABLE logs.logs MODIFY TTL toDateTime(timestamp) + INTERVAL 14 DAY DELETE"
```

## Диск заполнился

```bash
docker compose exec clickhouse clickhouse-client -q \
  "SELECT partition, formatReadableSize(sum(bytes_on_disk)) FROM system.parts
   WHERE database='logs' AND active GROUP BY partition ORDER BY partition"
# дроп самой старой партиции:
docker compose exec clickhouse clickhouse-client -q "ALTER TABLE logs.logs DROP PARTITION '20260101'"
```
Системные логи CH ограничены 3 днями (`config.d/system-logs.xml`).

## Мониторинг (оперативные запросы)

```bash
ch() { docker compose exec -T clickhouse clickhouse-client -q "$1"; }
ch "SELECT count() FROM logs.logs WHERE timestamp > now()-INTERVAL 1 HOUR"     # поток
ch "SELECT formatReadableSize(value) FROM system.asynchronous_metrics WHERE metric='MemoryResident'"
ch "SELECT status, count() FROM system.asynchronous_insert_log WHERE event_time > now()-INTERVAL 1 HOUR GROUP BY status"
curl -s localhost:8080/metrics | grep -E 'logden_(logs|spool|clickhouse)'
```

## Бэкап / восстановление

```bash
./deploy/backup.sh                       # BACKUP в Disk('backups') + ротация 30 дней
# восстановление: RESTORE не пишет поверх существующей таблицы —
# восстановите во временное имя и обменяйте местами:
docker compose exec clickhouse clickhouse-client -q \
  "RESTORE TABLE logs.logs AS logs.logs_restored FROM Disk('backups','logs-YYYYMMDD-HHMMSS.zip')"
docker compose exec clickhouse clickhouse-client -q \
  "EXCHANGE TABLES logs.logs_restored AND logs.logs"
docker compose exec clickhouse clickhouse-client -q "DROP TABLE logs.logs_restored"
```
RESTORE/EXCHANGE требуют админ-прав: выполняйте под `default` (как выше —
`docker compose exec` ходит изнутри контейнера через loopback), `reader`/`writer`
не подойдут. Для DR копируйте бэкапы на другой хост (см. комментарий в
`deploy/backup.sh`).

## Спул: карантин `.bad`

Батч, который ClickHouse отверг как невалидный (HTTP 400 — например, спул писался
до несовместимой миграции схемы), при реплее переименовывается в `*.ndjson.bad` и
выводится из очереди, чтобы не блокировать реплей остальных файлов. Сигналы:
`logden_spool_quarantined_total` > 0 (события также попадают в
`logden_logs_dropped_total`) и `spool batch rejected by clickhouse` в логах шлюза.

После устранения причины файлы можно вернуть в очередь (том `gw-spool`; у
distroless-шлюза нет shell, поэтому через вспомогательный контейнер):
```bash
docker run --rm -v logden_gw-spool:/spool alpine \
  sh -c 'cd /spool && for f in *.ndjson.bad; do mv "$f" "${f%.bad}"; done'
```

## Масштабирование

- **Вертикально:** поднять `mem_limit` и `max_server_memory_usage` пропорционально RAM.
- **Шлюз stateless:** масштабируется горизонтально за L4/L7-балансером (общий токен один);
  у каждого инстанса свой спул — данные не теряются.
- **Узкое место** — write-throughput: растите `BATCH_SIZE`/`async_insert_max_data_size`.
- **>десятков млн строк/день** — выходит за рамки «лёгкого» одно-нодового дизайна;
  нужен кластер/шардинг ClickHouse (out of scope).
