# logden

Компактная система централизованного логирования для нескольких проектов.
Любой сервис шлёт лог одним HTTP-POST → крошечный Go-шлюз батчит и пишет в
ClickHouse → анализ обычным SQL. Рассчитано на VPS ~1 ГБ RAM, минимум
зависимостей (шлюз — только Go stdlib).

[![CI](https://github.com/ginkida/logden/actions/workflows/ci.yml/badge.svg)](https://github.com/ginkida/logden/actions/workflows/ci.yml)

## Возможности

- **Простой универсальный API** — `POST /logs` с общим токеном; один объект,
  JSON-массив, NDJSON или gzip. Пишет кто угодно: Laravel, Node, Python, cron, bash.
- **Надёжность (на стороне шлюза)** — батчинг, ретраи с backoff, дисковый спул
  и автоматический реплей: логи переживают кратковременное падение и рестарт
  ClickHouse. Клиенты при этом намеренно тонкие и не ретраят (см. `clients/`).
- **Наблюдаемость** — Prometheus `/metrics`, readiness `/readyz`, `/version`,
  structured logging (slog/JSON).
- **Компактность** — шлюз ~10-15 МБ RAM, ClickHouse затюнен под 768 МБ; сжатие
  логов 10-30×, авто-ретеншн партициями.
- **Безопасность** — общий токен (с ротацией), rate limiting, валидация ввода,
  read-only/cap-drop контейнер, ClickHouse наружу не торчит.

## Архитектура

```
любой сервис (Laravel / Node / Python / cron / bash)
        │  POST /logs   { project, level, message, context, timestamp? }
        │  Authorization: Bearer <общий токен>
        ▼
┌──────────────────────────────────────┐
│   logden (Go, ~10-15 МБ RAM)  │
│   токен → валидация → буфер → батч     │
│   ретраи + backoff → дисковый спул     │
│   /metrics /readyz /version            │
└──────────────────┬───────────────────┘
                   │ батч INSERT (JSONEachRow, wait_for_async_insert)
                   ▼
            ┌──────────────┐
            │  ClickHouse  │  таблица logs.logs, TTL 30 дней, cap 768 МБ
            └──────┬───────┘
                   │ SQL, read-only (reader)
                   ▼
                агент / анализ
```

ClickHouse наружу не публикуется; снаружи доступен только порт шлюза.

## Контракт API

```
POST /logs
Authorization: Bearer <LOG_TOKEN>
Content-Type: application/json
Content-Encoding: gzip            (опционально)

{
  "project":   "billing-api",     // обязательно: [A-Za-z0-9._-], до 64 символов
  "level":     "error",           // опц.; нормализуется (warn→warning), по умолч. info
  "message":   "Payment timeout",  // обязательно; усекается при превышении лимита
  "context":   { "order_id": 123 },// опц.; любой JSON
  "timestamp": "2026-06-02T10:00:00Z" // опц.; RFC3339 или unix (сек/мс); иначе время вставки
}
```

- **Батч:** тело может быть JSON-массивом `[ {...}, {...} ]` или NDJSON (по объекту на строку).
- **Частичный приём:** невалидные элементы батча пропускаются (метрика
  `logden_logs_rejected_total{reason="invalid_event"}`), валидные принимаются; весь
  батч отклоняется (`400`) только если валидных нет.
- Ответ: `204 No Content`. Ошибки: `400` (валидация), `401` (токен), `413` (размер),
  `429` (rate limit), `503` (буфер переполнен).
- Токен можно слать в `Authorization: Bearer <…>` или в заголовке `X-Log-Token: <…>`.
- `source_ip` проставляет шлюз (см. `TRUSTED_PROXIES`).

### Служебные эндпоинты

| Путь        | Назначение                                              |
|-------------|---------------------------------------------------------|
| `/healthz`  | liveness (всегда 200, не трогает ClickHouse)            |
| `/readyz`   | readiness (200/503 — проверяет доступность ClickHouse)  |
| `/metrics`  | Prometheus-метрики                                      |
| `/version`  | версия/коммит/дата сборки                               |

## Быстрый старт (Docker)

```bash
cp .env.example .env
# заполните LOG_TOKEN и пароли: openssl rand -hex 32
docker compose up -d --build
```

Без сборки из исходников — готовый образ из ghcr (публикуется релизным тегом `v*`):
```bash
# в .env: GATEWAY_IMAGE=ghcr.io/ginkida/logden:0.2.0
docker compose pull gateway && docker compose up -d
```

Проверка:
```bash
set -a; . ./.env; set +a
curl -fsS -X POST http://localhost:8080/logs \
  -H "Authorization: Bearer $LOG_TOKEN" \
  -d '{"project":"demo","level":"error","message":"hello"}'

docker compose exec clickhouse clickhouse-client \
  --user reader --password "$CH_READER_PASSWORD" \
  -q "SELECT * FROM logs.logs ORDER BY timestamp DESC LIMIT 5"
```

Порт шлюза `:8080` по умолчанию слушает только loopback хоста (`GATEWAY_BIND=127.0.0.1`) —
наружу выставляйте через reverse-proxy с TLS (см. [SECURITY.md](SECURITY.md)) или
осознанно `GATEWAY_BIND=0.0.0.0`. ClickHouse наружу не публикуется вовсе. Том
`ch-data` хранит данные ClickHouse, том `gw-spool` — буфер шлюза на случай простоя.

## Установка без Docker (bare metal)

1. **ClickHouse** — скопировать `clickhouse/config.d/*` и `clickhouse/users.d/*`
   в `/etc/clickhouse-server/`, а также `docker/clickhouse-access.xml` →
   `/etc/clickhouse-server/users.d/` (даёт `default` право создавать
   пользователей для `users.sql` и запирает его на loopback). Перезапустить,
   затем:
   ```bash
   clickhouse-client --multiquery < clickhouse/schema.sql
   # поменяйте пароли в users.sql:
   clickhouse-client --multiquery < clickhouse/users.sql
   ```
2. **Шлюз** — `make build`, положить бинарь в `/usr/local/bin/`, настроить
   `deploy/logden.env.example` → `/etc/logden.env`, поставить
   `deploy/logden.service` и `systemctl enable --now logden`.

## Клиенты

Готовые модули с батчингом — `clients/` (Go-пакет, Python, Node).
**bash** — `examples/curl.sh`. **Laravel** — `examples/LoggerGatewayHandler.php`.

Клиенты намеренно тонкие: **не ретраят и не спулят**; при недоступности шлюза
событие (в batch-режиме — весь батч, молча) теряется. Вся надёжность доставки —
на участке шлюз → ClickHouse.

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

## Конфигурация шлюза (env)

| Переменная           | По умолчанию             | Описание                                        |
|----------------------|--------------------------|-------------------------------------------------|
| `LOG_TOKEN`          | —                        | общий токен(ы), через запятую; обязателен        |
| `LISTEN_ADDR`        | `:8080`                  | адрес прослушивания                              |
| `CLICKHOUSE_URL`     | `http://127.0.0.1:8123`  | адрес ClickHouse                                 |
| `CLICKHOUSE_USER`    | `writer`                 | пользователь для вставки                         |
| `CLICKHOUSE_PASSWORD`| —                        | пароль (или `*_FILE`)                            |
| `BATCH_SIZE`         | `500`                    | размер батча                                     |
| `BUFFER_SIZE`        | `2000`                   | ёмкость in-memory буфера                         |
| `FLUSH_INTERVAL`     | `1s`                     | интервал флаша                                   |
| `MAX_RETRIES`        | `3`                      | ретраи вставки перед спулом                      |
| `SPOOL_DIR`          | (пусто)                  | каталог дискового спула; пусто = выключен        |
| `REPLAY_INTERVAL`    | `30s`                    | интервал реплея спула                            |
| `RATE_LIMIT_RPS`     | `0`                      | лимит запросов/с (0 = выкл.)                      |
| `RATE_BURST`         | `0` (=`RATE_LIMIT_RPS`)  | размер всплеска токен-бакета                      |
| `TRUSTED_PROXIES`    | (пусто)                  | CIDR доверенных прокси для `X-Forwarded-For`     |
| `METRICS_TOKEN`      | (пусто)                  | токен для `/metrics`; пусто = открыто           |
| `LOG_LEVEL`          | `info`                   | уровень логов шлюза                              |
| `MAX_MESSAGE_BYTES`  | `65536`                  | потолок размера сообщения                        |
| `MAX_CONTEXT_BYTES`  | `65536`                  | потолок размера context                          |
| `MAX_BODY_BYTES`     | `4194304`                | потолок всего тела запроса (источник 413)        |
| `MAX_BATCH_EVENTS`   | `1000`                   | максимум событий в одном запросе                 |
| `SPOOL_MAX_FILES`    | `1000`                   | потолок числа батчей в спуле                      |
| `RETENTION`          | `720h`                   | отбраковка клиентских timestamp старше (≈ TTL)   |
| `CLICKHOUSE_DB` / `_TABLE` | `logs` / `logs`    | имя базы/таблицы                                 |

Источник истины по конфигу — `loadConfig` в `gateway/main.go`.
Секреты можно подавать файлом: `LOG_TOKEN_FILE`, `CLICKHOUSE_PASSWORD_FILE`.

## Анализ

Готовые запросы — `clickhouse/queries.sql` (аналитика + мониторинг). Ходить
read-only пользователем `reader`. Поиск по тексту — через `hasToken(message, …)`
(использует skip-индекс). Подключение агента/MCP к read-only `reader` —
[docs/agent-access.md](docs/agent-access.md).

## Наблюдаемость

`/metrics` отдаёт (Prometheus): `logden_logs_received_total`,
`logden_logs_inserted_total`, `logden_logs_dropped_total`,
`logden_clickhouse_insert_failed_total`, `logden_clickhouse_insert_retries_total`,
`logden_clickhouse_reachable`, `logden_spool_files`, `logden_buffer_events`,
`logden_clickhouse_insert_duration_seconds` (гистограмма), `logden_build_info`.
У ClickHouse включён встроенный prometheus-эндпоинт на `:9363` (наружу не публикуется).
Готовые alert-rules — `deploy/alerts.yml` (дропы, недоступность CH, рост спула,
заполнение буфера, латентность вставок, память ClickHouse).

## Production

- **TLS** — токен и логи идут по HTTP; поставьте reverse-proxy (caddy/nginx) с
  TLS перед шлюзом, а сам шлюз слушайте на loopback. См. [SECURITY.md](SECURITY.md).
- **Лимиты памяти** — в `docker-compose.yml` заданы `mem_limit` (жёсткий
  cgroup-потолок поверх мягкого `max_server_memory_usage`). Проверьте под свой бокс.
- **Эксплуатация** — [RUNBOOK.md](RUNBOOK.md): что делать при падении ClickHouse,
  ротация токена, смена ретеншна, бэкап, масштабирование.
- **Версии образов** запинены по digest для воспроизводимости.
- НЕ публикуйте порты ClickHouse (8123/9000/9363).

## Бюджет RAM (1 ГБ-бокс)

| Компонент        | RAM     |
|------------------|---------|
| ClickHouse (cap) | ~768 МБ (cgroup `mem_limit` 850m) |
| logden   | ~10-15 МБ (`mem_limit` 96m, `GOMEMLIMIT` 80MiB) |
| ОС + overhead    | ~150 МБ |

Замерено в простое: gateway ~2 МБ, ClickHouse ~190 МБ. Память шлюза под нагрузкой ≈
`BUFFER_SIZE` × средний размер строки; при росте `BUFFER_SIZE` поднимайте `mem_limit`/`GOMEMLIMIT`.
На 1 ГБ комфортно при дефолтах; для запаса лучше 2 ГБ.

## Надёжность: что гарантируется

Шлюз отвечает `204` после постановки в буфер, затем батчем пишет в ClickHouse с
подтверждением (`wait_for_async_insert=1`) и ретраями. При недоступности
ClickHouse батч уходит в дисковый спул и переигрывается после восстановления —
логи не теряются на кратком сбое/рестарте базы. Дисковый спул включается через
`SPOOL_DIR` (в Docker задан по умолчанию); без него durability при простое/shutdown
не гарантируется. Потеря возможна лишь если переполнены и буфер, и спул (тогда
`logden_logs_dropped_total` растёт, отдаётся `503`).

## Лицензия

[MIT](LICENSE).
