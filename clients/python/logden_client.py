"""Крошечный клиент для ingest-шлюза logden (только stdlib).

    from logden_client import LoggerClient
    log = LoggerClient("http://logs.internal:8080", TOKEN, "billing-api")
    log.error("payment timeout", {"order_id": 123})

С батчингом:

    log = LoggerClient(EP, TOKEN, "worker", batch=500, interval=1.0)
    ...
    log.close()
"""

import json
import threading
import urllib.request


class LoggerClient:
    def __init__(self, endpoint, token, project, timeout=2.0, batch=0, interval=1.0):
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.project = project
        self.timeout = timeout
        self.batch = batch
        self._buf = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        if batch > 0:
            self._thread = threading.Thread(target=self._loop, args=(interval,), daemon=True)
            self._thread.start()

    def log(self, level, message, context=None):
        event = {"project": self.project, "level": level, "message": message}
        if context:
            event["context"] = context
        if self.batch > 0:
            with self._lock:
                self._buf.append(event)
                full = len(self._buf) >= self.batch
            if full:
                try:
                    self.flush()
                except Exception:
                    pass  # batch-режим — fire-and-forget; явный flush()/close() ошибку пробросит
        else:
            self._send([event])

    def info(self, message, context=None):
        self.log("info", message, context)

    def warning(self, message, context=None):
        self.log("warning", message, context)

    def error(self, message, context=None):
        self.log("error", message, context)

    def flush(self):
        with self._lock:
            if not self._buf:
                return
            batch, self._buf = self._buf, []
        self._send(batch)

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self.flush()

    def _loop(self, interval):
        while not self._stop.wait(interval):
            try:
                self.flush()
            except Exception:
                pass  # сетевая ошибка не должна навсегда убивать фоновый флашер

    def _send(self, events):
        data = json.dumps(events).encode()
        req = urllib.request.Request(
            self.endpoint + "/logs",
            data=data,
            method="POST",
            headers={
                "Authorization": "Bearer " + self.token,
                "Content-Type": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            resp.read()
