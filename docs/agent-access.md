# Доступ агента к логам (SQL / MCP)

Анализ — обычный SQL под read-only пользователем `reader` (только `SELECT`,
профиль с лимитами памяти/времени + readonly-режим, доступ к нужным system-таблицам).
Агент не может ни писать, ни обойти лимиты.

## Где живёт ClickHouse

По умолчанию порты ClickHouse наружу НЕ публикуются (только docker-сеть). Чтобы
агент/MCP-сервер достучался, есть два пути.

### A. Открыть HTTP только на loopback (локальный агент)

Скопируйте `docker-compose.override.yml.example` → `docker-compose.override.yml`
(подхватывается автоматически). Он биндит 8123 только на `127.0.0.1` — наружу
порт по-прежнему закрыт:

```yaml
services:
  clickhouse:
    ports:
      - "127.0.0.1:8123:8123"
```

### B. MCP-сервер в той же docker-сети

Запустите ClickHouse-MCP сервисом в compose — он ходит к `clickhouse:8123` по
внутренней сети, и публиковать порт CH наружу не нужно.

## MCP-сервер ClickHouse

Официальный сервер — `mcp-clickhouse`. Конфиг указывает на `reader`:

```json
{
  "mcpServers": {
    "logs": {
      "command": "uvx",
      "args": ["mcp-clickhouse"],
      "env": {
        "CLICKHOUSE_HOST": "127.0.0.1",
        "CLICKHOUSE_PORT": "8123",
        "CLICKHOUSE_USER": "reader",
        "CLICKHOUSE_PASSWORD": "<CH_READER_PASSWORD>",
        "CLICKHOUSE_DATABASE": "logs"
      }
    }
  }
}
```

Дальше агент видит `logs.logs` и системные таблицы и гоняет SQL из
`clickhouse/queries.sql` (топ ошибок, динамика по уровням, поиск по `message`/`context`).

## Прямой SQL без MCP

```bash
curl -s 'http://127.0.0.1:8123/' \
  -H "X-ClickHouse-User: reader" -H "X-ClickHouse-Key: $CH_READER_PASSWORD" \
  --data-binary "SELECT project, count() FROM logs.logs
                 WHERE timestamp > now()-INTERVAL 1 HOUR GROUP BY project FORMAT JSON"
```

## Безопасность

- `reader` — только `SELECT` + readonly-режим (нельзя менять настройки/обходить лимиты).
- НЕ публикуйте 8123 в публичную сеть — loopback-биндинг (A) или внутренняя сеть (B).
- Для удалённого агента ставьте TLS-прокси перед ClickHouse.
