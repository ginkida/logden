# Contributing

## Принципы проекта (жёсткие правила)

- **Шлюз — только Go stdlib.** Никаких внешних зависимостей в `gateway/go.mod`
  (кроме тест-only, если без них никак). Это держит образ ~10 МБ и аудит — простым.
- **Бюджет ресурсов.** Целевой хост — ~1 ГБ RAM. Изменения не должны заметно
  увеличивать потребление памяти шлюза или ClickHouse.
- **Простой контракт.** `POST /logs` остаётся тривиальным для любого языка.

## Локальная разработка

```bash
make fmt           # gofmt
make vet           # go vet
make lint          # vet + staticcheck
make test          # юнит-тесты с -race
make up            # поднять стек
LOG_TOKEN=... make smoke
make down
```

Интеграционные тесты (нужен ClickHouse; модуль объявлен в `gateway/`, запускать оттуда):
```bash
cd gateway && CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_USER=default \
  go test -tags=integration ./...
```

## Перед PR

- `gofmt` и `go vet` чисто; `make test` зелёный (или `cd gateway && go test -race ./...`).
- Изменение контракта `/logs` или схемы ClickHouse → запись в [CHANGELOG.md](CHANGELOG.md).
- Изменение схемы на живой таблице → ALTER в `clickhouse/migrations.sql`, не правка `schema.sql`.
- Коммиты — короткие, по существу.

CI прогоняет gofmt, vet, staticcheck, unit (race+coverage), интеграцию против
реального ClickHouse и сборку образа.
