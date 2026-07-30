# Clients

Thin clients for `POST /logs`. All dependency-free, with optional
batching. One contract: `{ project, level, message, context, timestamp }` + `Authorization: Bearer`.

The clients **do not retry or spool**: if the gateway is down, a single send
returns an error, and batch mode drops the batch silently (the background flusher swallows errors).
Reliability (retries, spool, replay) lives on the gateway → ClickHouse leg.

- **Event time** is stamped by the client when the event is recorded, so batching delay
  and gateway queueing don't move it. The gateway falls back to its own ingest time when a
  client sends no `timestamp`.
- **Request bounds** are enforced on both axes: a flush is split into requests of at most
  1000 events (`MAX_BATCH_EVENTS`) and 4 MiB (`MAX_BODY_BYTES`), because the gateway rejects
  an oversized request as a whole (`413`) — every event in it would be lost.
- Without `close()` in batch mode the remaining buffer is not flushed.
- **Node:** outside batch mode `log()` rejects on a failed send — `await` it, or pass
  `{ onError }` to route errors to a sink instead (an unawaited rejection crashes the process).
  The batch timer is `unref`'d, so it never keeps a CLI or worker alive on its own.

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
