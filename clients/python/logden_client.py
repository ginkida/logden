"""Tiny client for the logden ingest gateway (stdlib only).

    from logden_client import LoggerClient
    log = LoggerClient("http://logs.internal:8080", TOKEN, "billing-api")
    log.error("payment timeout", {"order_id": 123})

With batching:

    log = LoggerClient(EP, TOKEN, "worker", batch=500, interval=1.0)
    ...
    log.close()
"""

import datetime
import json
import threading
import urllib.request

# Mirrors the gateway's defaults: MAX_BATCH_EVENTS and MAX_BODY_BYTES. The
# gateway rejects an oversized request as a whole (413), so the client splits
# instead of losing every event in the batch.
MAX_BATCH = 1000
MAX_BODY_BYTES = 4 * 1024 * 1024


class LoggerClient:
    def __init__(self, endpoint, token, project, timeout=2.0, batch=0, interval=1.0):
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.project = project
        self.timeout = timeout
        self.batch = min(batch, MAX_BATCH)
        self._buf = []
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        if batch > 0:
            self._thread = threading.Thread(
                target=self._loop, args=(interval,), daemon=True
            )
            self._thread.start()

    def log(self, level, message, context=None):
        event = {
            "project": self.project,
            "level": level,
            "message": message,
            # Stamped when the event is recorded: batching delay and gateway
            # queueing must not move the event in time.
            "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
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
                    pass  # batch mode is fire-and-forget; explicit flush()/close() will surface the error
        else:
            self._send([event])

    def info(self, message, context=None):
        self.log("info", message, context)

    def warning(self, message, context=None):
        self.log("warning", message, context)

    def error(self, message, context=None):
        self.log("error", message, context)

    def flush(self):
        """Send what is buffered now, in requests of at most MAX_BATCH events.

        Concurrent log() calls can push the buffer past the gateway's cap between
        two flushes; an oversized request is rejected as a whole.
        """
        with self._lock:
            pending = len(self._buf)
        sent = 0
        first_error = None
        while sent < pending:
            with self._lock:
                if not self._buf:
                    break
                chunk, self._buf = self._buf[:MAX_BATCH], self._buf[MAX_BATCH:]
            sent += len(chunk)
            # Keep draining after a failure (the chunk is already out of the
            # buffer either way) and surface the first error at the end.
            try:
                self._send(chunk)
            except Exception as exc:  # noqa: BLE001 - re-raised below
                first_error = first_error or exc
        if first_error is not None:
            raise first_error

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
                pass  # a network error must not permanently kill the background flusher

    def _send(self, events):
        data = json.dumps(events).encode()
        if len(data) > MAX_BODY_BYTES and len(events) > 1:
            # Both halves are always attempted: raising on the first would
            # silently drop the sibling half, which flush() already removed
            # from the buffer.
            half = len(events) // 2
            first_error = None
            for part in (events[:half], events[half:]):
                try:
                    self._send(part)
                except Exception as exc:  # noqa: BLE001 - re-raised below
                    first_error = first_error or exc
            if first_error is not None:
                raise first_error
            return
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
