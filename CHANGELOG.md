# Changelog

Формат — [Keep a Changelog](https://keepachangelog.com/), версии — [SemVer](https://semver.org/).

## [Unreleased]

## [0.2.0] — 2026-06-04

### Added
- Конвейер приёма в шлюзе: ограниченный буфер, батчинг, ретраи с backoff,
  дисковый спул и автоматический реплей (durability при простое ClickHouse).
- Batch-приём: JSON-массив, NDJSON, gzip-тело; приём клиентского `timestamp`.
- Валидация: whitelist/нормализация `level`, проверка `project`, лимиты
  message/context/тела/числа событий.
- Наблюдаемость: Prometheus `/metrics`, readiness `/readyz`, `/version`,
  structured logging (slog/JSON). Встроенный prometheus-эндпоинт ClickHouse (:9363).
- Безопасность: несколько токенов (ротация), rate limiting, `TRUSTED_PROXIES`
  для X-Forwarded-For, секреты из файлов (`*_FILE`), hardening контейнеров
  (read_only, cap_drop, no-new-privileges), default ClickHouse ограничен loopback.
- ClickHouse prod-конфиг: `ttl_only_drop_parts`, tokenbf skip-индексы,
  TTL системных логов, тюнинг async_insert, профиль/квоты для `reader`,
  нативный BACKUP-диск.
- Эксплуатация: graceful shutdown, healthcheck-режим бинаря, Makefile,
  RUNBOOK/SECURITY/CONTRIBUTING, релизный workflow (GHCR multi-arch + SBOM/provenance),
  пин образов по digest, лимиты ресурсов и ротация логов в compose.
- Fail-fast валидация конфигурации шлюза на старте (понятные ошибки вместо паники под нагрузкой).
- Prometheus alert-rules (`deploy/alerts.yml`) и метрика `logden_buffer_capacity` (для алерта на заполнение буфера).
- Хардненинг CI: hadolint, shellcheck, yamllint (`--strict`), promtool check rules; Dependabot (gomod/actions/docker).
- Переиспользуемые клиенты с батчингом в `clients/` (Go-пакет + тест, Python, Node) — без внешних зависимостей.
- Опциональная авторизация `/metrics` через `METRICS_TOKEN`.
- Доки по доступу агента/MCP к ClickHouse (`docs/agent-access.md`) + пример loopback-оверрайда compose.
- Нагрузочный генератор `tools/loadtest` (+ `make loadtest`) для проверки throughput.
- Тесты: юнит (auth, валидация, батч, спул/реплей, clientIP, метрики, конфиг) + интеграция с ClickHouse.
- Карантин спула: батч, отвергнутый ClickHouse (HTTP 400 — например, после несовместимой
  миграции), переименовывается в `*.ndjson.bad` и не блокирует реплей остальной очереди;
  новая метрика `logden_spool_quarantined_total` (события учитываются и в `dropped`).
- Фоновая проба ClickHouse: `logden_clickhouse_reachable` обновляется и без внешних
  запросов к `/readyz` — алерт `LogdenClickHouseUnreachable` работает из коробки.
- Warning при старте, если `METRICS_TOKEN` пуст (открытый `/metrics`).
- `GATEWAY_IMAGE` в compose: запуск готового образа из ghcr без локальной сборки.
- Безопасные дефолты: порт шлюза в compose биндится на 127.0.0.1 (`GATEWAY_BIND`
  для переопределения); CI-workflow с минимальными `permissions: contents: read`.

### Fixed
- Shutdown: устранена паника `send on closed channel` (канал закрывается синхронно с enqueue) и неограниченный дренаж (реплей спула прерывается сигналом остановки).
- NDJSON с синтаксически битой строкой теперь отдаёт `400` (наблюдаемо), а не теряет валидный хвост молча.
- Клиенты: Node fire-and-forget больше не даёт unhandledRejection; Go `Close()` идемпотентен; Python `close()` джойнит фоновый флашер.
- `Authorization: Bearer` принимается регистронезависимо (RFC 7235).
- Исправлен staticcheck SA4000 в тесте rate limiter.
- Спул: осиротевшие `*.tmp` (крэш между записью и rename) удаляются при старте.
- Python-клиент: фоновый флашер переживает ошибки отправки (раньше первая сетевая
  ошибка навсегда убивала поток); batch-режим — fire-and-forget, как в Node.
- CI/Release: все GitHub Actions запинены по commit SHA, `prom/prometheus` — по digest.
- Доки: bare-metal установка требует `clickhouse-access.xml` (access_management) до
  `users.sql`; рабочая процедура RESTORE (через временную таблицу + EXCHANGE);
  явная оговорка, что клиенты не ретраят.

## [0.1.0]
- Базовая версия: тонкий шлюз (`POST /logs`, общий токен, single insert),
  схема ClickHouse с TTL 30 дней, docker-compose, init-схема/пользователи.
