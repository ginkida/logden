// Крошечный клиент для ingest-шлюза logden (Node 18+, глобальный fetch).
//
//   import { LoggerClient } from "./logden.mjs";
//   const log = new LoggerClient("http://logs.internal:8080", TOKEN, "web");
//   await log.error("boom", { path: "/checkout" });
//
// С батчингом:
//   const log = new LoggerClient(EP, TOKEN, "web", { batch: 500, interval: 1000 });
//   await log.close();

export class LoggerClient {
  constructor(endpoint, token, project, { timeout = 2000, batch = 0, interval = 1000 } = {}) {
    this.endpoint = endpoint.replace(/\/+$/, "");
    this.token = token;
    this.project = project;
    this.timeout = timeout;
    this.batch = batch;
    this.buf = [];
    if (batch > 0) {
      this.timer = setInterval(() => this.flush().catch(() => {}), interval);
    }
  }

  async log(level, message, context) {
    const event = { project: this.project, level, message };
    if (context) event.context = context;
    if (this.batch > 0) {
      this.buf.push(event);
      // batch-режим — fire-and-forget: не пробрасываем ошибку наружу, иначе
      // незаawait-ленный log() даёт unhandledRejection и может уронить процесс.
      if (this.buf.length >= this.batch) this.flush().catch(() => {});
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

  async flush() {
    if (this.buf.length === 0) return;
    const batch = this.buf;
    this.buf = [];
    await this.#send(batch);
  }

  async close() {
    if (this.timer) clearInterval(this.timer);
    await this.flush();
  }

  async #send(events) {
    const response = await fetch(`${this.endpoint}/logs`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(events),
      signal: AbortSignal.timeout(this.timeout),
    });
    if (!response.ok) {
      throw new Error(`logden: gateway returned ${response.status}`);
    }
  }
}
