# Clients

Thin clients for `POST /logs`. All dependency-free, with optional
batching. One contract: `{ project, level, message, context }` + `Authorization: Bearer`.

The clients **do not retry or spool**: if the gateway is down, a single send
returns an error, and batch mode drops the batch silently (the background flusher swallows errors).
Reliability (retries, spool, replay) lives on the gateway → ClickHouse leg. Keep your own batch
≤ 1000 events (the gateway's `MAX_BATCH_EVENTS`). Without `close()` in batch mode
the remaining buffer is not flushed.

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

For Laravel see `examples/LoggerGatewayHandler.php`. For bare `curl` — `examples/curl.sh`.
