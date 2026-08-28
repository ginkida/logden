# Clients

Thin clients for `POST /logs` in Go, Python and Node. All three are
dependency-free, all three speak the same contract —
`{ project, level, message, context, timestamp }` + `Authorization: Bearer` —
and all three are meant to read as one client with three spellings.

**They do not retry and do not spool.** A refused request is a lost event;
reliability (retries, disk spool, replay) lives on the gateway → ClickHouse leg,
where it can be durable. What the clients do instead is make the loss *visible*:
every failure reaches the application, either as a return value/exception or
through the error sink described below.

## What all three do

- **Levels.** `debug`, `info`, `notice`, `warning`, `error`, `critical` have
  helpers; they are the gateway's PSR-3 vocabulary (`allowedLevels` in
  `gateway/validate.go`). `alert` and `emergency` are valid levels too and go
  through the generic `Log()`/`log()`. Go and Node keep the older `warn` spelling
  as an alias of `warning`; Python never had one.
- **Event time** is stamped by the client when the event is recorded, so
  batching delay and gateway queueing don't move it. The gateway falls back to
  its own ingest time only when a client sends no usable `timestamp`.
- **Request bounds on both axes.** A flush is split into requests of at most
  1000 events (`MAX_BATCH_EVENTS`) and 4 MiB (`MAX_BODY_BYTES`) — the gateway's
  defaults, overridable in all three (see the matrix) — because the gateway
  rejects an oversized request as a whole (`413`) and every event in it would be
  lost. Both halves of a split are always attempted: stopping at the
  first failure would silently drop the sibling half, which the buffer no longer
  holds.
- **Batch mode never sends on the caller's stack.** A full buffer wakes the
  background flusher (a channel in Go, a `setImmediate` in Node, an `Event` in
  Python) instead of running the request inline. Encoding a multi-megabyte body
  and waiting out the gateway's latency used to land on whichever application
  code path happened to record the event that filled the batch. `flush()` and
  `close()` stay synchronous — an explicit flush is the caller asking to wait.
- **The batch buffer is bounded, and overflow drops the OLDEST events.** The
  newest ones describe the state the application is in right now — usually the
  incident that made the gateway unreachable — so dropping them would hide an
  ongoing problem behind the head of a burst that is already history. The caller
  is never blocked to apply backpressure: that would put the outage back on the
  application's request path. Drops are counted and reported **once per flush**,
  not once per event, so an overflowing buffer cannot turn a log burst into a
  second burst inside the application. The cap defaults to 10 000 events in all
  three, and the flush trigger is clamped to `min(batch, maxBatchEvents,
  maxBuffer)`: a trigger above the buffer ceiling could never fire, because the
  drop policy evicts an event for every new one and the buffer never reaches it.
- **Background failures reach an error sink, and the default is not silence.**
  Without a sink a bad token would make the client discard every log forever
  without a word. The sink is called for a failed background send, a buffer
  overflow and an oversized event; it is wrapped so that a sink which throws
  cannot kill the process from the client's own goroutine/thread/timer.
- **The gateway's reason is surfaced.** A rejected `/logs` answers
  `{"error":"<reason>"}`, which is the difference between "retry later" and "fix
  your project name"; the clients parse it and expose it as a field. Because the
  client cannot be sure it is talking to the gateway (a wrong URL lands on any
  endpoint), only a bounded prefix of the body is read, control characters are
  stripped and the reason is capped at 120 characters before it reaches the
  application's own log.
- **An unserializable event costs its own `context`, not the batch.** A value
  JSON cannot encode (a NaN, a cycle, an object whose serializer throws) used to
  fail the encode for the whole in-flight chunk of up to 1000 events. The clients
  isolate the offending event and send it with `context` replaced by
  `{"_unserializable": true}` — the same key in all three, so one ClickHouse
  query finds every degraded row, and deliberately not the gateway's own
  `_invalid_json` / `_truncated` so the two causes stay apart — and the event's
  `message`, `level` and `timestamp` still land. The error is surfaced through
  the same channel as a failed send.
- **A single event over the body cap is dropped with a report, not posted.**
  Sending it buys nothing — the gateway answers `413` for the whole request —
  and returning quietly is worse, since the event has already left the buffer.
  The report carries project, level, timestamp and sizes; never the payload,
  which is both what made the event oversized and the last thing that belongs in
  an error line.
- **Without `close()` in batch mode the remaining buffer is not flushed.**

## Capability matrix

| | Go | Node | Python |
|---|---|---|---|
| Batching (off by default) | `WithBatch(size, interval)` | `{ batch, interval }` | `batch=`, `interval=` |
| Buffer cap (events) | `WithMaxBuffer`, default 10000 | `maxBuffer`, default 10000 | `max_buffer=`, default 10000 |
| Cap overrides | `WithLimits(maxBatchEvents, maxBodyBytes)` | `maxBatch`, `maxBodyBytes` | `max_batch=`, `max_body_bytes=` |
| Non-positive override | keeps the default | keeps the default | keeps the default |
| Error sink | `WithOnError(func(error))` | `{ onError }` | `on_error=` |
| Default sink | stdlib `log` package | `console.error` | one line on stderr |
| Default HTTP timeout | 5 s | 5000 ms | 5.0 s |
| Legacy level alias | `Warn` → `Warning` | `warn` → `warning` | — (never had one) |
| Transport override | `WithHTTPClient` | — (global `fetch`) | — (speaks `http.client`) |
| Flush error | all chunk errors, `errors.Join` | first error, every chunk attempted | first error, every chunk attempted |
| Connection reuse | stdlib default transport | `fetch` keep-alive | idle pool of 4 (LIFO) |

An override of the batch/body caps must stay **at or below** the gateway's
`MAX_BATCH_EVENTS` / `MAX_BODY_BYTES`. Only the gateway's copy is authoritative;
set them higher and every request comes back `413`, which costs the whole request
rather than one event. A non-positive or unparseable override means "keep the
default", never "no limit" — an unset key in a config object must not hand the
application an unbounded buffer.

Everything above the last four rows is the same behaviour in three spellings —
including the default sink, which is each language's own "somewhere the operator
will actually see it". The last four rows are the behavioural divergences that
remain, and each is forced by the language rather than chosen:

- **`warn`.** Go and Node shipped it before `warning` existed and cannot drop it
  without breaking callers; the Python client never had it, so adding one now
  would invent a legacy rather than preserve one.
- **Transport override.** `*http.Client` is Go's standard injection point for
  timeouts, proxies and TLS. Node's `fetch` is a global with no equivalent seam,
  and the Python client deliberately speaks `http.client` directly (see below),
  so both take the settings they need — `timeout` — as plain options instead.
- **Flush error.** Go has `errors.Join`, so nothing is lost by returning every
  chunk's failure. JavaScript and Python would have to raise an `AggregateError`
  / `ExceptionGroup`, which changes the type callers catch (`except GatewayError`
  stops matching); both attempt every chunk regardless and surface the first
  failure.
- **Connection reuse.** All three reuse connections; only the mechanism differs,
  because only Go and Node have a pooling HTTP client in their standard library.

## Go (`clients/go`)

```go
import logden "github.com/ginkida/logden/clients/go"

c := logden.New("http://logs.internal:8080", token, "billing-api",
    logden.WithBatch(500, time.Second),
    logden.WithOnError(func(err error) { myLogger.Warn(err) }))
defer c.Close()
c.Error("payment timeout", map[string]any{"order_id": 123})
```

Options: `WithBatch`, `WithHTTPClient`, `WithLimits`, `WithMaxBuffer`,
`WithOnError`. Helpers: `Debug`, `Info`, `Notice`, `Warning`, `Error`,
`Critical`, plus `Warn` as an alias of `Warning`.

- In batch mode `Log` performs no network I/O and **always returns `nil`** — the
  send has not happened yet. In direct mode it returns the send error.
- `New` clamps the flush trigger to `min(batch, maxBatchEvents, maxBuffer)`: a
  trigger above the buffer cap could never fire.
- Errors worth branching on: `ErrBufferFull`, `ErrEventTooLarge` (both via
  `errors.Is`) and `*GatewayError{StatusCode, Reason}` (via `errors.As`).
  Transport failures are wrapped as `logden: …` so one line in the application's
  log names the client.
- The default sink writes one line per failure through the standard `log`
  package. Pass `WithOnError(func(error) {})` for silence; `nil` keeps the
  default.

## Python (`clients/python/logden_client.py`)

```python
from logden_client import LoggerClient

with LoggerClient("http://logs.internal:8080", TOKEN, "worker",
                  batch=500, interval=1.0) as log:
    log.error("db down", {"attempt": 3})
```

Signature: `LoggerClient(endpoint, token, project, timeout=5.0, batch=0,
interval=1.0, max_buffer=None, max_batch=None, max_body_bytes=None,
on_error=None)`.

- Exceptions the application can catch: `LogdenError` (base), `GatewayError`
  (`.status`, `.reason`), `DroppedEvents` (`.count`), `OversizedEvent` (`.size`,
  `.limit`, `.level`, `.timestamp` — never the payload, and no reference to the
  event itself, which would pin its megabytes to the error object).
- Direct (non-batch) mode still raises to the caller; the sink is for what
  happens in the background. `flush()`/`close()` re-raise the first failure.
- `max_buffer` defaults to 10 000 events, and `batch` is clamped to
  `min(batch, max_batch, max_buffer)` — the same arithmetic as Go and Node.
- `OversizedEvent` carries `.limit`, the client's effective body cap, so a report
  from a client with a custom `max_body_bytes` does not quote the module default.
- `close()` is idempotent. `__exit__` re-raises a failed final flush on a clean
  exit, but routes it to the sink when the body is already raising, so a network
  error cannot mask the application's real exception.
- The endpoint is parsed and validated at construction (`ValueError` on a
  missing or unsupported scheme) and a path prefix is honoured:
  `http://host/edge` posts to `/edge/logs`.
- It speaks `http.client` directly and keeps an idle pool of at most 4
  connections, so a batching client pays one TCP (and with https one TLS)
  handshake instead of one per request. Two consequences: `http_proxy` /
  `https_proxy` are **ignored** (deliberate for an internal gateway, and it
  removes the fork-unsafe macOS system-proxy read `urllib` did), and redirects
  are **not** followed — a `3xx` is a `GatewayError`. A pooled socket the gateway
  closed on its 60 s idle timeout is retried exactly once on a fresh connection;
  that request never reached anyone, so the retry cannot duplicate an event, and
  it is the only retry any client does.
- After `fork()` the child re-initializes its lock, buffer, flusher thread and
  connections, so it starts empty rather than inheriting a lock the fork may have
  frozen or a socket the parent is still writing to. Events buffered before the
  fork stay with the parent and are flushed there.

## Node (`clients/node/logden.mjs`)

```js
import { LoggerClient } from "./logden.mjs";
const log = new LoggerClient("http://logs.internal:8080", TOKEN, "web", { batch: 500 });
await log.error("boom", { path: "/checkout" });
await log.close();
```

Options: `timeout`, `batch`, `interval`, `onError`, `maxBatch`, `maxBodyBytes`,
`maxBuffer`. A non-positive or unparseable option falls back to its default
rather than disabling the cap.

- Outside batch mode `log()` **rejects** on a failed send — `await` it, or pass
  `{ onError }` to route errors to a sink instead (an unawaited rejection
  crashes the process). In batch mode nothing is thrown at the caller.
- Send errors carry `error.status` and `error.reason` alongside the message
  `logden: gateway returned 400 (all_invalid)`. The reason vocabulary is the one
  in the main README's API contract.
- Every flush — the timer's, a full buffer's and the caller's own — is serialized
  on one promise chain, so two flushes cannot splice the same buffer and
  interleave their requests.
- The repeating batch timer is `unref`'d, so it never keeps a CLI or worker alive
  on its own. The one-shot wake is deliberately **not** unref'd: it holds the
  loop for a single tick, which is what lets a script that fills the buffer and
  exits without `close()` still deliver.

For Laravel see `examples/LoggerGatewayHandler.php`. For bare `curl` — `examples/curl.sh`.
