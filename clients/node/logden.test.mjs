// Tests for the Node client: run with `node --test clients/node/`.
//
// Everything goes through the public surface against a real loopback gateway, so
// the assertions are about what the gateway would actually receive. #send is a
// true private method and cannot be called from here; the buffer-splitting tests
// preload the public `buf` field instead, the same seam the Go tests use.

import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";

import { LoggerClient } from "./logden.mjs";

// Mirrors the client's own caps; hard-coded so a change to them fails a test
// instead of silently redefining what is asserted.
const MAX_BATCH = 1000;
const MAX_BODY_BYTES = 4 * 1024 * 1024;
const MAX_BUFFER = 10000;

// Even the client's 5 s default can abort an 8 MiB request on a loaded runner
// and would red-flag a correct client, so every test client gets a generous one.
const TIMEOUT = 30000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Records every request so a test can assert how flush() split the buffer.
// `reply(n)` picks the answer to the n-th request: a status code, or
// `{ code, body, type }` when the test needs a body to be parsed. `hold(n)` is
// awaited before answering, which is how a test reproduces a gateway that is
// merely slow rather than broken.
async function startGateway(reply = () => 204, hold) {
  const requests = [];
  const server = http.createServer((req, res) => {
    // A test that reads only part of a response makes the client destroy the
    // socket under the server's feet; without this listener that write error is
    // an unhandled 'error' event, i.e. a crashed test run.
    res.on("error", () => {});
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", async () => {
      const raw = Buffer.concat(chunks);
      requests.push({
        bytes: raw.length,
        auth: req.headers.authorization,
        events: JSON.parse(raw.toString("utf8")),
      });
      if (hold) await hold(requests.length);
      const answer = reply(requests.length);
      const { code, body = "", type } = typeof answer === "number" ? { code: answer } : answer;
      res.writeHead(code, type ? { "Content-Type": type } : undefined).end(body);
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    requests,
    delivered: () => requests.flatMap((r) => r.events),
    // fetch keeps its sockets alive, so close() alone would hang the test.
    close: async () => {
      server.closeAllConnections();
      await new Promise((resolve) => server.close(resolve));
    },
  };
}

function makeEvents(n, context) {
  return Array.from({ length: n }, (_, i) => {
    const event = {
      project: "test",
      level: "info",
      message: `event ${i}`,
      timestamp: new Date().toISOString(),
    };
    if (context) event.context = context(i);
    return event;
  });
}

// A value JSON.stringify cannot encode and the client's replacer cannot rescue:
// the throw comes from toJSON, before any replacer runs.
function poison() {
  return {
    toJSON() {
      throw new Error("no JSON for you");
    },
  };
}

function deferred() {
  let resolve;
  const promise = new Promise((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

// The background flusher runs on the event loop, not on the caller's stack, so a
// test that asserts what it did has to wait for a turn of the loop instead of
// awaiting a promise the caller never receives.
async function waitFor(predicate, what, limit = 10000) {
  const deadline = Date.now() + limit;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await sleep(5);
  }
  assert.fail(`timed out waiting for ${what}`);
}

const rejection = (promise) => promise.then(() => null, (e) => e);

test("log() posts one event as a JSON array with the bearer token", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  await log.info("hello", { path: "/checkout" });

  assert.equal(gw.requests.length, 1);
  assert.equal(gw.requests[0].auth, "Bearer tok");
  assert.ok(Array.isArray(gw.requests[0].events));
  assert.deepEqual(gw.requests[0].events, [
    {
      project: "web",
      level: "info",
      message: "hello",
      timestamp: gw.requests[0].events[0].timestamp,
      context: { path: "/checkout" },
    },
  ]);
});

test("every level helper sends the gateway's own vocabulary", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  await log.debug("a");
  await log.info("b");
  await log.notice("c");
  await log.warning("d");
  await log.warn("e");
  await log.error("f");
  await log.critical("g");

  // These are the PSR-3 names gateway/validate.go accepts as-is; anything else
  // is silently rewritten to "info" there, so a typo in a helper would lose the
  // severity of every event it sends. `warn` is the pre-existing alias and must
  // keep resolving to "warning", which is what the gateway normalizes it to.
  assert.deepEqual(
    gw.delivered().map((e) => e.level),
    ["debug", "info", "notice", "warning", "warning", "error", "critical"],
  );
});

test("flush() splits the buffer into requests of at most MAX_BATCH events", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  // Concurrent log() calls can push the buffer past the gateway's cap between
  // two flushes; preloading the buffer reproduces that state directly.
  log.buf.push(...makeEvents(2 * MAX_BATCH + 1));
  await log.flush();

  assert.equal(gw.requests.length, 3);
  for (const request of gw.requests) {
    assert.ok(request.events.length <= MAX_BATCH, `request of ${request.events.length} events`);
  }
  const delivered = gw.delivered();
  assert.equal(delivered.length, 2 * MAX_BATCH + 1);
  // Order survives the split: the gateway must see the events as recorded.
  assert.deepEqual(
    delivered.map((e) => e.message),
    makeEvents(2 * MAX_BATCH + 1).map((e) => e.message),
  );
  assert.equal(log.buf.length, 0);
});

test("flush() splits a request that exceeds MAX_BODY_BYTES", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  // Twelve events is far below MAX_BATCH but ~8.4 MiB on the wire: only the
  // byte axis can split this.
  const blob = "x".repeat(700 * 1024);
  log.buf.push(...makeEvents(12, () => ({ blob })));
  await log.flush();

  assert.ok(gw.requests.length > 1, "an 8 MiB flush must not go out as one request");
  for (const request of gw.requests) {
    assert.ok(request.bytes <= MAX_BODY_BYTES, `request of ${request.bytes} bytes`);
  }
  assert.equal(gw.delivered().length, 12);
});

test("a failing half does not drop its sibling half", async (t) => {
  // The failing request is the first one, i.e. the recursion must keep going
  // after it: those events are already out of the buffer.
  const gw = await startGateway((n) => (n === 1 ? 500 : 204));
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  const blob = "x".repeat(700 * 1024);
  log.buf.push(...makeEvents(12, () => ({ blob })));

  await assert.rejects(() => log.flush(), /gateway returned 500/);
  assert.ok(gw.requests.length > 1, "the sibling half was never attempted");
  assert.equal(gw.delivered().length, 12);
});

test("log() stamps the timestamp when the event is recorded, not at flush time", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  // An interval long enough that only the explicit flush() below can fire.
  const log = new LoggerClient(gw.url, "tok", "web", { batch: 500, interval: 60000, timeout: TIMEOUT });
  t.after(() => clearInterval(log.timer));

  const beforeLog = Date.now();
  await log.info("recorded early");
  const afterLog = Date.now();
  await sleep(50);
  const beforeFlush = Date.now();
  await log.flush();

  const stamped = Date.parse(gw.requests[0].events[0].timestamp);
  assert.ok(stamped >= beforeLog && stamped <= afterLog, `${stamped} is outside the log() window`);
  assert.ok(stamped < beforeFlush, "the timestamp must predate the flush");
});

test("an unserializable context costs only its own context, not the chunk", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const errors = [];
  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT, onError: (e) => errors.push(e) });
  log.buf.push(...makeEvents(11, (i) => (i === 7 ? { req: poison() } : { i })));
  await log.flush();

  const delivered = gw.delivered();
  assert.equal(delivered.length, 11, "the siblings of a poisoned event must still arrive");
  const degraded = delivered.filter((e) => e.context?._unserializable === true);
  assert.deepEqual(
    degraded.map((e) => e.message),
    ["event 7"],
  );
  assert.deepEqual(
    delivered.filter((e) => e.message !== "event 7").map((e) => e.context.i),
    [0, 1, 2, 3, 4, 5, 6, 8, 9, 10],
  );
  // The context is gone for good, so the loss has to reach the sink.
  assert.equal(errors.length, 1);
  assert.match(errors[0].message, /unserializable context/);
});

test("an event that cannot be serialized at all is dropped alone", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  const events = makeEvents(11);
  // Replacing the context cannot rescue this one, so the event itself is lost.
  events[3].message = poison();
  log.buf.push(...events);

  await assert.rejects(() => log.flush(), /dropped an unserializable event/);
  const delivered = gw.delivered();
  assert.equal(delivered.length, 10, "exactly one event may be lost");
  assert.deepEqual(
    delivered.map((e) => e.message),
    ["event 0", "event 1", "event 2", "event 4", "event 5", "event 6", "event 7", "event 8", "event 9", "event 10"],
  );
});

test("an event too big for the body cap is reported by identity, never by payload", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT, maxBodyBytes: 64 * 1024 });
  // The byte split bottoms out at one event; before, that event went out anyway,
  // collected a 413 and disappeared with the rest of the failed chunk.
  const secret = "s".repeat(100 * 1024);
  log.buf.push(...makeEvents(3, (i) => (i === 1 ? { blob: secret } : { i })));

  const error = await rejection(log.flush());
  assert.ok(error, "an event lost for good must reach the caller");
  assert.match(error.message, /dropped an oversized event/);
  // Enough to find the event in the application's own code...
  assert.match(error.message, /project=test/);
  assert.match(error.message, /level=info/);
  assert.match(error.message, /bytes/);
  // ...and never the payload, which is what made it too big in the first place.
  assert.ok(!error.message.includes("ssss"), "the payload must never be quoted back");

  assert.deepEqual(
    gw.delivered().map((e) => e.message),
    ["event 0", "event 2"],
    "the siblings of an oversized event must still arrive",
  );
});

test("an unserializable context on an oversized event costs no request", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT, maxBodyBytes: 4096 });
  // Both failures at once: the context will not encode, and the message alone is
  // already past the cap. Replacing the context with the marker shrinks the body
  // but cannot bring it under the limit, so posting it only buys a certain 413 —
  // the byte guard in #send ran against the body that never encoded.
  log.buf.push({
    project: "web",
    level: "info",
    message: "m".repeat(8192),
    timestamp: new Date(0).toISOString(),
    context: poison(),
  });

  const error = await rejection(log.flush());
  assert.ok(error, "an event lost for good must reach the caller");
  assert.match(error.message, /dropped an oversized event/);
  assert.match(error.message, /context was unserializable/);
  assert.equal(gw.requests.length, 0, "a request the gateway is certain to 413 must not be sent");
});

test("BigInts and cycles are encoded, a shared acyclic object is left intact", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  const shared = { id: 7 };
  const req = { name: "GET /checkout" };
  req.self = req;
  await log.error("boom", { size: 42n, req, first: shared, second: shared });

  // One request means the encode-failure recursion was never entered.
  assert.equal(gw.requests.length, 1);
  const context = gw.requests[0].events[0].context;
  assert.equal(context.size, "42");
  assert.equal(context.req.self, "[circular]");
  assert.equal(context.req.name, "GET /checkout");
  // A sub-object referenced twice is acyclic and must survive both times: a
  // plain "already seen" set would have mangled the second one.
  assert.deepEqual(context.first, { id: 7 });
  assert.deepEqual(context.second, { id: 7 });
  assert.equal(context._unserializable, undefined);
});

test("a rejected send carries the gateway's own reason", async (t) => {
  const gw = await startGateway(() => ({
    code: 400,
    body: '{"error":"invalid_project"}\n',
    type: "application/json; charset=utf-8",
  }));
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  const error = await rejection(log.info("x"));

  // Without the reason this is an opaque 400 and the application cannot tell a
  // bad project name from a body it should have split.
  assert.equal(error.message, "logden: gateway returned 400 (invalid_project)");
  assert.equal(error.status, 400);
  assert.equal(error.reason, "invalid_project");
});

test("an error body that is not the gateway's shape falls back to the status", async (t) => {
  // Three answers no logden gateway sends: a proxy's HTML page, a body far too
  // large to be a reason, and a reason carrying a newline (an application that
  // logs the error would otherwise get a second line of the sender's choosing).
  const replies = [
    { code: 502, body: "<html><body>Bad Gateway</body></html>", type: "text/html" },
    { code: 503, body: JSON.stringify({ error: "e".repeat(512 * 1024) }), type: "application/json" },
    { code: 400, body: JSON.stringify({ error: "bad\nproject" }), type: "application/json" },
  ];
  const gw = await startGateway((n) => replies[n - 1]);
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  const messages = [];
  for (let i = 0; i < replies.length; i++) messages.push((await rejection(log.info("x"))).message);

  assert.deepEqual(messages, [
    "logden: gateway returned 502",
    "logden: gateway returned 503",
    "logden: gateway returned 400 (bad project)",
  ]);
});

test("close() sends what is still buffered", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { batch: 500, interval: 60000, timeout: TIMEOUT });
  await log.info("one");
  await log.warn("two");
  assert.equal(gw.requests.length, 0, "below the batch size nothing is sent yet");

  await log.close();
  assert.equal(gw.requests.length, 1);
  assert.deepEqual(
    gw.requests[0].events.map((e) => e.message),
    ["one", "two"],
  );
});

test("close() clears the batching timer", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { batch: 500, interval: 20, timeout: TIMEOUT });
  await log.close();

  // With the interval still live this event would be flushed within 20 ms; it
  // must sit in the buffer until the caller asks for it instead.
  await log.info("after close");
  await sleep(100);
  assert.equal(gw.requests.length, 0);
  assert.equal(log.buf.length, 1);
});

test("the batching timer is unref'd and only exists in batch mode", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const plain = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT });
  assert.equal(plain.timer, undefined);

  const log = new LoggerClient(gw.url, "tok", "web", { batch: 500, interval: 60000, timeout: TIMEOUT });
  t.after(() => clearInterval(log.timer));
  // `unref?.()` no-ops silently if the call is ever lost, and the only symptom
  // is a CLI that never exits; hasRef() is what catches that.
  assert.equal(log.timer.hasRef(), false);
});

test("a full batch is handed to the background, not sent on the caller's stack", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { batch: 2, interval: 60000, timeout: TIMEOUT });
  t.after(() => clearInterval(log.timer));

  await log.info("one");
  await log.info("two");
  // The call that fills the batch must return with the buffer untouched: an
  // inline flush would already have spliced it and started the encode.
  assert.equal(log.buf.length, 2, "the send started on the caller's stack");
  assert.equal(gw.requests.length, 0);

  await waitFor(() => gw.requests.length === 1, "the background flush");
  assert.deepEqual(
    gw.requests[0].events.map((e) => e.message),
    ["one", "two"],
  );
  assert.equal(log.buf.length, 0);
});

test("a stalled gateway never delays a batch-mode log() call", async (t) => {
  const gate = deferred();
  const gw = await startGateway(() => 204, () => gate.promise);
  t.after(async () => {
    gate.resolve();
    await gw.close();
  });

  const errors = [];
  const log = new LoggerClient(gw.url, "tok", "web", {
    batch: 2,
    interval: 60000,
    timeout: TIMEOUT,
    onError: (e) => errors.push(e),
  });
  t.after(() => clearInterval(log.timer));

  await log.info("one");
  await log.info("two");
  await waitFor(() => gw.requests.length === 1, "the first background request");

  // The gateway is now holding that request open. Recording an event must still
  // cost the caller nothing but a push — this is the failure the batch buffer
  // exists to prevent: a slow gateway showing up as latency in whatever request
  // handler happened to log.
  const started = Date.now();
  for (let i = 0; i < 20; i++) await log.info(`event ${i}`);
  const elapsed = Date.now() - started;
  assert.ok(elapsed < 500, `log() waited ${elapsed} ms on a stalled gateway`);

  await sleep(20);
  assert.equal(gw.requests.length, 1, "a second flush must not overtake the one in flight");
  assert.equal(log.buf.length, 20, "the events are buffered, not in flight");

  gate.resolve();
  await log.close();
  assert.equal(gw.delivered().length, 22);
  assert.deepEqual(errors, []);
});

test("a full buffer drops its oldest events and reports the loss", async (t) => {
  const gate = deferred();
  const gw = await startGateway(() => 204, () => gate.promise);
  t.after(async () => {
    gate.resolve();
    await gw.close();
  });

  const errors = [];
  const log = new LoggerClient(gw.url, "tok", "web", {
    batch: 2,
    interval: 60000,
    maxBuffer: 4,
    timeout: TIMEOUT,
    onError: (e) => errors.push(e),
  });
  t.after(() => clearInterval(log.timer));

  // A buffer only overflows while the gateway is behind, so stall it on a
  // request it will not answer until the end of the test.
  await log.info("event 0");
  await log.info("event 1");
  await waitFor(() => gw.requests.length === 1, "the stalled first request");

  for (let i = 2; i < 8; i++) await log.info(`event ${i}`);
  // Oldest first: the two events at the head are gone, the newest six are the
  // ones describing the outage that is still going on.
  assert.deepEqual(
    log.buf.map((e) => e.message),
    ["event 4", "event 5", "event 6", "event 7"],
  );

  gate.resolve();
  await log.close();
  assert.deepEqual(
    gw.delivered().map((e) => e.message),
    ["event 0", "event 1", "event 4", "event 5", "event 6", "event 7"],
  );
  // One report for the whole overflow, not one per casualty.
  assert.equal(errors.length, 1);
  assert.match(errors[0].message, /buffer full at 4 events, dropped 2 oldest/);
});

test("the caps are overridable and nonsense values fall back to the defaults", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const plain = new LoggerClient(gw.url, "tok", "web", {});
  assert.equal(plain.maxBatch, MAX_BATCH);
  assert.equal(plain.maxBodyBytes, MAX_BODY_BYTES);
  assert.equal(plain.maxBuffer, MAX_BUFFER);

  // A config object with an absent key must not be read as "no limit".
  const sloppy = new LoggerClient(gw.url, "tok", "web", { maxBatch: 0, maxBodyBytes: "", maxBuffer: undefined });
  assert.equal(sloppy.maxBatch, MAX_BATCH);
  assert.equal(sloppy.maxBodyBytes, MAX_BODY_BYTES);
  assert.equal(sloppy.maxBuffer, MAX_BUFFER);

  // The flush trigger cannot exceed the buffer ceiling, or the drop policy would
  // evict events while waiting for a batch size the buffer can never reach.
  const tuned = new LoggerClient(gw.url, "tok", "web", { batch: 500, maxBuffer: 10, interval: 60000 });
  t.after(() => clearInterval(tuned.timer));
  assert.equal(tuned.batch, 10);
  assert.equal(tuned.maxBuffer, 10);
});

test("maxBatch splits a flush, and concurrent flushes stay serialized", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { timeout: TIMEOUT, maxBatch: 10 });
  log.buf.push(...makeEvents(25));

  // Three flushes at once: without one chain each would splice from the head of
  // the same buffer and their requests would interleave on the wire.
  await Promise.all([log.flush(), log.flush(), log.flush()]);

  assert.equal(gw.requests.length, 3);
  for (const request of gw.requests) {
    assert.ok(request.events.length <= 10, `request of ${request.events.length} events`);
  }
  assert.deepEqual(
    gw.delivered().map((e) => e.message),
    makeEvents(25).map((e) => e.message),
  );
});

test("close() delivers a buffer whose wake had not fired, and fires no second request", async (t) => {
  const gw = await startGateway();
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", { batch: 2, interval: 60000, timeout: TIMEOUT });
  await log.info("one");
  await log.info("two");
  await log.close();

  assert.equal(gw.requests.length, 1);
  assert.deepEqual(
    gw.requests[0].events.map((e) => e.message),
    ["one", "two"],
  );
  // close() cancels the pending wake, and even if it fired it would find the
  // buffer already drained: either way exactly one request leaves the client.
  await sleep(50);
  assert.equal(gw.requests.length, 1);
});

test("a throwing onError sink cannot take the process down", async (t) => {
  const gw = await startGateway(() => 500);
  t.after(() => gw.close());

  const log = new LoggerClient(gw.url, "tok", "web", {
    batch: 1,
    interval: 60000,
    timeout: TIMEOUT,
    onError: () => {
      throw new Error("sink exploded");
    },
  });
  t.after(() => clearInterval(log.timer));

  await log.info("boom");
  await waitFor(() => gw.requests.length === 1, "the background flush");
  // A throw out of the sink runs inside a setImmediate callback: unguarded it is
  // an uncaught exception, and the logging client kills the application. If it
  // escaped, this test run would already be dead.
  await sleep(50);
  assert.equal(log.buf.length, 0);

  // Same sink outside batch mode. Passing onError is what buys the caller the
  // right not to await log(), so a throwing sink there is an unhandled rejection
  // — the other way this crashes a process.
  const direct = new LoggerClient(gw.url, "tok", "web", {
    timeout: TIMEOUT,
    onError: () => {
      throw new Error("sink exploded");
    },
  });
  direct.info("boom");
  await waitFor(() => gw.requests.length === 2, "the un-awaited send");
  await sleep(50);
});

test("without an onError sink a background failure still reaches console.error", async (t) => {
  const gw = await startGateway(() => ({ code: 401, body: '{"error":"unauthorized"}', type: "application/json" }));
  t.after(() => gw.close());

  const seen = [];
  const original = console.error;
  console.error = (...args) => seen.push(args);
  t.after(() => {
    console.error = original;
  });

  const log = new LoggerClient(gw.url, "tok", "web", { batch: 1, interval: 60000, timeout: TIMEOUT });
  t.after(() => clearInterval(log.timer));

  await log.info("boom");
  // A wrong token used to discard every log the application wrote, forever,
  // without a word anywhere.
  await waitFor(() => seen.length > 0, "the fallback diagnostic");
  assert.match(String(seen[0][0]?.message), /gateway returned 401 \(unauthorized\)/);
});
