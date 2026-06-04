# Клиенты

Тонкие клиенты для `POST /logs`. Все — без внешних зависимостей, с опциональным
батчингом. Контракт один: `{ project, level, message, context }` + `Authorization: Bearer`.

Клиенты **не ретраят и не спулят**: при недоступности шлюза одиночная отправка
вернёт ошибку, а batch-режим потеряет батч молча (фоновый флашер ошибки глотает).
Надёжность (ретраи, спул, реплей) живёт на участке шлюз → ClickHouse. Свой батч
держите ≤ 1000 событий (`MAX_BATCH_EVENTS` шлюза). Без `close()` в batch-режиме
остаток буфера не отправится.

## Go (`clients/go`)

```go
import logden "github.com/ginkida/logden/clients/go"

c := logden.New("http://logs.internal:8080", token, "billing-api",
    logden.WithBatch(500, time.Second))
defer c.Close()
c.Error("payment timeout", map[string]any{"order_id": 123})
```

## Python (`clients/python/logden_client.py`)

```python
from logden_client import LoggerClient
log = LoggerClient("http://logs.internal:8080", TOKEN, "worker", batch=500, interval=1.0)
log.error("db down", {"attempt": 3})
log.close()
```

## Node (`clients/node/logden.mjs`)

```js
import { LoggerClient } from "./logden.mjs";
const log = new LoggerClient("http://logs.internal:8080", TOKEN, "web", { batch: 500 });
await log.error("boom", { path: "/checkout" });
await log.close();
```

Для Laravel см. `examples/LoggerGatewayHandler.php`. Для голого `curl` — `examples/curl.sh`.
