// Tiny client for the logden ingest gateway (Node 18+, global fetch).
//
//   import { LoggerClient } from "./logden.mjs";
//   const log = new LoggerClient("http://logs.internal:8080", TOKEN, "web");
//   await log.error("boom", { path: "/checkout" });
//
// With batching:
//   const log = new LoggerClient(EP, TOKEN, "web", { batch: 500, interval: 1000 });
//   await log.close();
//
// Options: timeout, batch, interval, onError, maxBatch, maxBodyBytes, maxBuffer.
// Levels: debug, info, notice, warning (warn), error, critical.
//
// Outside batch mode log() rejects on a failed send, so either await it or pass
// an onError sink — an unawaited rejection crashes the Node process.
//
// In batch mode nothing is thrown at the caller and nothing is sent on the
// caller's stack: the send is handed to the background, and every loss (a
// refused send, a full buffer, an event too big to ship) is reported to onError,
// or to console.error when the app passed no sink. A logging client that
// discards everything in silence after a bad token is the worst of the outcomes
// available to it.
//
// A context JSON.stringify refuses costs at most its own event: BigInts and
// cycles are encoded, anything else falls back to a {"_unserializable": true}
// context and is reported to onError.

// Mirrors the gateway's defaults: MAX_BATCH_EVENTS and MAX_BODY_BYTES. An
// oversized request is rejected as a whole (413), so split instead of losing it.
// Both are overridable per client because a retuned gateway moves them — an
// override must stay at or below what the gateway actually enforces, since only
// the gateway's copy is authoritative.
const MAX_BATCH = 1000;
const MAX_BODY_BYTES = 4 * 1024 * 1024;

// Batch-mode buffer ceiling, in events. Generous on purpose: 10 000 events is
// minutes of a chatty service, so nothing is dropped while the gateway is merely
// slow. But a ceiling there must be — without one, an unreachable gateway turns
// every line the application logs into permanent heap, and the client OOMs the
// process it was installed to observe.
const MAX_BUFFER = 10000;

// Default per-request timeout, in milliseconds. Shared with the Go and Python
// clients rather than tuned per language: a full 1000-event / 4 MiB flush over a
// loaded link needs more than the 2 s this used to default to, and an abort
// there costs the whole chunk, which flush() has already taken out of the
// buffer.
const DEFAULT_TIMEOUT_MS = 5000;

// How much of a rejected response is read back to find its reason. The gateway
// answers a few dozen bytes of JSON; a misrouted endpoint behind a proxy answers
// a megabyte of HTML, and this text ends up inside an Error message.
const MAX_ERROR_BODY = 1024;

// Encodes the two values a context routinely carries that JSON.stringify throws
// on: a BigInt, and a reference cycle (an Express req, an Error whose cause
// points back at it). A throw here costs a whole chunk, so keep the recursion in
// #send cold — an app that logs request objects would otherwise enter it on
// every flush.
//
// The array holds the path from the root, not every object already seen: an
// acyclic sub-object referenced from two fields is valid input, and a plain
// "seen" set would silently mangle it.
function jsonReplacer() {
  const path = [];
  return function (key, value) {
    if (typeof value === "bigint") return String(value);
    if (value === null || typeof value !== "object") return value;
    // `this` is the object holding `value`; anything the encoder has already
    // finished with sits above it on the stack and is no longer an ancestor.
    while (path.length > 0 && path[path.length - 1] !== this) path.pop();
    if (path.includes(value)) return "[circular]";
    path.push(value);
    return value;
  };
}

function encode(events) {
  return JSON.stringify(events, jsonReplacer());
}

// An option that is not a positive number falls back to the default instead of
// disabling the cap: an absent key in a config object (`{ maxBuffer: undefined }`)
// must not hand the application an unbounded buffer, and a zero maxBatch would
// turn every flush into an infinite split.
function positive(value, fallback) {
  const n = Math.floor(Number(value));
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

// Reads at most `limit` bytes off a response body and abandons the rest, so a
// reply of unknown size cannot be buffered into an Error message.
async function readCapped(stream, limit) {
  if (!stream || typeof stream.getReader !== "function") return "";
  const reader = stream.getReader();
  const chunks = [];
  let size = 0;
  try {
    while (size < limit) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
      size += value.length;
    }
  } finally {
    // The rest of the body is of no interest; cancel() on an already-finished
    // stream is a no-op and its rejection is not worth propagating.
    reader.cancel().catch(() => {});
  }
  return Buffer.concat(chunks).toString("utf8").slice(0, limit);
}

// The gateway answers a rejected /logs with {"error":"<reason>"}. That reason is
// the difference between "retry later" and "fix your project name", and a bare
// 400 tells the application neither. Anything that is not that shape — a proxy's
// HTML page, an empty body, a reply too large to be the gateway's — falls back
// to the status code alone.
async function gatewayReason(response) {
  let reason;
  try {
    reason = JSON.parse(await readCapped(response.body, MAX_ERROR_BODY)).error;
  } catch {
    return "";
  }
  if (typeof reason !== "string") return "";
  // Sanitized before it reaches the application's own log: this is remote input,
  // and a newline in it would forge a log line of the reader's choosing.
  return reason.replace(/[\u0000-\u001f\u007f]/g, " ").slice(0, 120);
}

// Identifies a lost event by its metadata only. The payload is what made the
// event too big to send, and an error sink usually ends up in the very log the
// payload did not fit into.
function describeEvent(event, bytes) {
  const message = typeof event?.message === "string" ? event.message : "";
  return (
    `${bytes} bytes, project=${event?.project}, level=${event?.level}, ` +
    `timestamp=${event?.timestamp}, message=${Buffer.byteLength(message, "utf8")} bytes`
  );
}

export class LoggerClient {
  // Serializes every flush — the timer's, a full buffer's and the caller's own.
  // Two flushes in flight at once each splice from the head of the buffer, so an
  // earlier chunk can reach the gateway after a later one and a burst lands out
  // of order.
  #chain = Promise.resolve();
  #wakeHandle;
  #dropped = 0;

  constructor(
    endpoint,
    token,
    project,
    {
      timeout = DEFAULT_TIMEOUT_MS,
      batch = 0,
      interval = 1000,
      onError,
      maxBatch = MAX_BATCH,
      maxBodyBytes = MAX_BODY_BYTES,
      maxBuffer = MAX_BUFFER,
    } = {},
  ) {
    this.endpoint = endpoint.replace(/\/+$/, "");
    this.token = token;
    this.project = project;
    this.timeout = timeout;
    this.maxBatch = positive(maxBatch, MAX_BATCH);
    this.maxBodyBytes = positive(maxBodyBytes, MAX_BODY_BYTES);
    this.maxBuffer = positive(maxBuffer, MAX_BUFFER);
    // The flush trigger can never sit above the buffer ceiling: the drop policy
    // would evict the oldest events while waiting for a batch size the buffer is
    // not allowed to reach, and only the interval would ever send anything.
    this.batch = Math.min(batch, this.maxBatch, this.maxBuffer);
    this.onError = onError;
    this.buf = [];
    if (this.batch > 0) {
      this.timer = setInterval(() => this.flush().catch((e) => this.#report(e)), interval);
      // Don't hold the event loop open: a CLI or worker must still be able to
      // exit when its work is done (close() flushes the remainder).
      this.timer.unref?.();
    }
  }

  async log(level, message, context) {
    const event = {
      project: this.project,
      level,
      message,
      // Stamped when the event is recorded, not when the gateway inserts it.
      timestamp: new Date().toISOString(),
    };
    if (context) event.context = context;
    if (this.batch > 0) {
      // Bounded buffer, oldest dropped first: when the gateway is behind, the
      // newest events describe the failure that is still going on, while the
      // head of the queue is stale backlog nobody will act on any more. The
      // alternative — making the caller wait for room — would push the gateway's
      // outage into the application's own request path, which is the one thing a
      // fire-and-forget logger must never do.
      if (this.buf.length >= this.maxBuffer) {
        this.#dropped += this.buf.splice(0, this.buf.length - this.maxBuffer + 1).length;
      }
      this.buf.push(event);
      if (this.buf.length >= this.batch) this.#wake();
      return;
    }
    // A caller that passed a sink is entitled not to await this call, so the
    // failure goes to the sink and the returned promise resolves — including
    // when the sink itself throws, which #report absorbs.
    if (this.onError) {
      try {
        await this.#send([event]);
      } catch (e) {
        this.#report(e);
      }
      return;
    }
    await this.#send([event]);
  }

  debug(message, context) {
    return this.log("debug", message, context);
  }

  info(message, context) {
    return this.log("info", message, context);
  }

  notice(message, context) {
    return this.log("notice", message, context);
  }

  warning(message, context) {
    return this.log("warning", message, context);
  }

  // PSR-3 — and so the gateway — calls this level "warning"; warn stays as an
  // alias because removing it would break every existing caller.
  warn(message, context) {
    return this.warning(message, context);
  }

  error(message, context) {
    return this.log("error", message, context);
  }

  critical(message, context) {
    return this.log("critical", message, context);
  }

  // Sends what is buffered now, in requests of at most maxBatch events:
  // concurrent log() calls can push the buffer past the gateway's cap. Unlike
  // the background path this is the caller explicitly asking to wait, so it
  // resolves only once the gateway has answered — and rejects with the first
  // failure, having attempted every chunk.
  flush() {
    return this.#queueFlush();
  }

  async close() {
    if (this.timer) clearInterval(this.timer);
    // A wake scheduled before close() would fire afterwards and flush on behalf
    // of a client the caller is done with. It would find an empty buffer in the
    // usual case, so this is symmetry with clearInterval rather than a fix for
    // an observed loss: after close() the client has nothing scheduled.
    if (this.#wakeHandle) {
      clearImmediate(this.#wakeHandle);
      this.#wakeHandle = undefined;
    }
    await this.flush();
  }

  // Hands a full buffer to the background instead of sending it on the caller's
  // stack: a flush encodes up to `batch` events (a multi-megabyte
  // JSON.stringify) and then carries the gateway's whole latency, and log() is
  // called from whatever application code happened to record the event that
  // filled the buffer. setImmediate rather than a microtask, which would still
  // run before the caller's own continuation resumes.
  //
  // Deliberately not unref'd: unlike the repeating interval, a one-shot immediate
  // holds the loop for a single tick, and that tick is what lets a script which
  // fills the buffer and exits without close() still deliver those events.
  #wake() {
    if (this.#wakeHandle) return;
    this.#wakeHandle = setImmediate(() => {
      this.#wakeHandle = undefined;
      this.#queueFlush().catch((e) => this.#report(e));
    });
  }

  #queueFlush() {
    const done = this.#chain.then(() => this.#flushNow());
    // The stored chain must never carry a rejection: it is awaited again by the
    // next flush, and a rejection nobody else subscribes to is an unhandled
    // rejection. The caller of this flush gets `done`, which does reject.
    this.#chain = done.catch(() => {});
    return done;
  }

  async #flushNow() {
    this.#reportDrops();
    let pending = this.buf.length;
    let firstError;
    while (pending > 0 && this.buf.length > 0) {
      const chunk = this.buf.splice(0, this.maxBatch);
      pending -= chunk.length;
      try {
        await this.#send(chunk);
      } catch (e) {
        firstError ??= e;
      }
    }
    if (firstError) throw firstError;
  }

  // Drops are surfaced once per flush, not once per dropped event: an
  // overflowing buffer means the application is logging faster than the gateway
  // accepts, and a sink called on every casualty would turn that burst into a
  // second burst inside the application.
  #reportDrops() {
    if (this.#dropped === 0) return;
    const dropped = this.#dropped;
    this.#dropped = 0;
    this.#report(
      new Error(`logden: buffer full at ${this.maxBuffer} events, dropped ${dropped} oldest event(s)`),
    );
  }

  // The only place a background failure can go. Without a sink it goes to
  // console.error: a bad token would otherwise discard every log the application
  // writes, forever, without a word. The call is wrapped because a sink that
  // throws inside a timer callback is an uncaught exception — the logging client
  // taking the process down.
  #report(e) {
    try {
      if (this.onError) this.onError(e);
      else console.error(e);
    } catch {
      // A sink that throws is not worth a second attempt.
    }
  }

  async #send(events) {
    let body;
    try {
      body = encode(events);
    } catch (e) {
      // Encoding is all-or-nothing and flush() has already removed these events
      // from the buffer, so one value the replacer can't rescue (a toJSON that
      // throws) would take every sibling down with it. Halve until the offending
      // event is alone. This guard stays separate from the byte guard below: a
      // lone event that won't encode must never be handed to fetch.
      if (events.length > 1) return this.#sendHalves(events);
      return this.#sendOne(events[0], e);
    }
    // Bound the request by bytes too: a few hundred events with large contexts
    // stay under maxBatch yet exceed maxBodyBytes, and the gateway answers 413
    // for the whole batch. Bytes, not UTF-16 units: the limit is on the wire
    // size, and non-ASCII messages weigh more than String.length suggests.
    const bytes = Buffer.byteLength(body, "utf8");
    if (bytes > this.maxBodyBytes) {
      if (events.length > 1) return this.#sendHalves(events);
      // The split bottoms out here, on one event that is over the cap by itself.
      // Sending it would spend the whole body on a request the gateway is
      // certain to answer 413 — so it is dropped, but never in silence: an event
      // vanishing without a trace is what made this case invisible before.
      throw new Error(`logden: dropped an oversized event: ${describeEvent(events[0], bytes)}`);
    }
    await this.#post(body);
  }

  // Both halves are always attempted: throwing on the first would silently drop
  // the sibling half, which flush() already removed from the buffer.
  async #sendHalves(events) {
    const half = Math.floor(events.length / 2);
    let firstError;
    for (const part of [events.slice(0, half), events.slice(half)]) {
      try {
        await this.#send(part);
      } catch (e) {
        firstError ??= e;
      }
    }
    if (firstError) throw firstError;
  }

  // A lone event that won't encode. The context is the only field holding a
  // value the caller chose freely, so retry with it replaced by a marker: the
  // row still lands with its message, level and timestamp, and only the context
  // is lost. The key deliberately differs from the gateway's own _invalid_json
  // and _truncated markers so the causes stay distinguishable in ClickHouse.
  async #sendOne(event, cause) {
    let body;
    try {
      body = encode([{ ...event, context: { _unserializable: true } }]);
    } catch {
      // Not even the bare event encodes, so nothing can be salvaged: lose this
      // one event, and no sibling.
      throw new Error(`logden: dropped an unserializable event: ${cause}`, { cause });
    }
    // Re-check the byte cap: the marker body is smaller than the original, but
    // an event whose MESSAGE alone is over the cap is still over it, and #send's
    // guard ran against the body that failed to encode. Posting anyway spends a
    // whole request the gateway is certain to answer 413. Reported before the
    // context notice, because on this path nothing lands at all.
    const bytes = Buffer.byteLength(body, "utf8");
    if (bytes > this.maxBodyBytes) {
      throw new Error(
        `logden: dropped an oversized event: ${describeEvent(event, bytes)} (context was unserializable: ${cause})`,
        { cause },
      );
    }
    // The event is delivered, so this is not a send failure and must not reject
    // the caller — but dropping a context in silence is what made the old
    // whole-chunk loss invisible.
    this.#report(new Error(`logden: replaced an unserializable context: ${cause}`, { cause }));
    await this.#post(body);
  }

  async #post(body) {
    const response = await fetch(`${this.endpoint}/logs`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.token}`,
        "Content-Type": "application/json",
      },
      body,
      signal: AbortSignal.timeout(this.timeout),
    });
    if (response.ok) return;
    const reason = await gatewayReason(response);
    const error = new Error(`logden: gateway returned ${response.status}${reason ? ` (${reason})` : ""}`);
    // The two fields an application can branch on (back off on a 503, alert on a
    // 401) without parsing the message.
    error.status = response.status;
    if (reason) error.reason = reason;
    throw error;
  }
}
