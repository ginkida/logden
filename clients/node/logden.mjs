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
// Outside batch mode log() rejects on a failed send, so either await it or pass
// an onError sink — an unawaited rejection crashes the Node process.

// Mirrors the gateway's defaults: MAX_BATCH_EVENTS and MAX_BODY_BYTES. An
// oversized request is rejected as a whole (413), so split instead of losing it.
const MAX_BATCH = 1000;
const MAX_BODY_BYTES = 4 * 1024 * 1024;

export class LoggerClient {
  constructor(endpoint, token, project, { timeout = 2000, batch = 0, interval = 1000, onError } = {}) {
    this.endpoint = endpoint.replace(/\/+$/, "");
    this.token = token;
    this.project = project;
    this.timeout = timeout;
    this.batch = Math.min(batch, MAX_BATCH);
    this.onError = onError;
    this.buf = [];
    if (batch > 0) {
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
      this.buf.push(event);
      // batch mode is fire-and-forget: don't surface the error, otherwise an
      // unawaited log() triggers unhandledRejection and may crash the process.
      if (this.buf.length >= this.batch) this.flush().catch((e) => this.#report(e));
      return;
    }
    if (this.onError) {
      try {
        await this.#send([event]);
      } catch (e) {
        this.onError(e);
      }
      return;
    }
    await this.#send([event]);
  }

  info(message, context) {
    return this.log("info", message, context);
  }

  warn(message, context) {
    return this.log("warning", message, context);
  }

  error(message, context) {
    return this.log("error", message, context);
  }

  // Sends what is buffered now, in requests of at most MAX_BATCH events:
  // concurrent log() calls can push the buffer past the gateway's cap.
  async flush() {
    let pending = this.buf.length;
    let firstError;
    while (pending > 0 && this.buf.length > 0) {
      const chunk = this.buf.splice(0, MAX_BATCH);
      pending -= chunk.length;
      try {
        await this.#send(chunk);
      } catch (e) {
        firstError ??= e;
      }
    }
    if (firstError) throw firstError;
  }

  async close() {
    if (this.timer) clearInterval(this.timer);
    await this.flush();
  }

  #report(e) {
    if (this.onError) this.onError(e);
  }

  async #send(events) {
    const body = JSON.stringify(events);
    // Bytes, not UTF-16 units: the gateway's limit is on the wire size, and
    // non-ASCII messages weigh more than String.length suggests.
    if (Buffer.byteLength(body, "utf8") > MAX_BODY_BYTES && events.length > 1) {
      // Both halves are always attempted: throwing on the first would silently
      // drop the sibling half, which flush() already removed from the buffer.
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
      return;
    }
    const response = await fetch(`${this.endpoint}/logs`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.token}`,
        "Content-Type": "application/json",
      },
      body,
      signal: AbortSignal.timeout(this.timeout),
    });
    if (!response.ok) {
      throw new Error(`logden: gateway returned ${response.status}`);
    }
  }
}
