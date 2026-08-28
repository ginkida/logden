"""Tiny client for the logden ingest gateway (stdlib only).

    from logden_client import LoggerClient
    log = LoggerClient("http://logs.internal:8080", TOKEN, "billing-api")
    log.error("payment timeout", {"order_id": 123})

With batching, which never sends on the calling thread:

    with LoggerClient(EP, TOKEN, "worker", batch=500, interval=1.0) as log:
        log.info("started")

Batch mode is fire-and-forget: recording an event cannot raise, so the
failures the caller would otherwise never hear about — a bad token, an
unreachable gateway, a buffer overflow, an event too large to send — reach the
application through the `on_error` sink. It defaults to one line on stderr,
because silence is what let a misconfigured token discard every log forever
with no signal at all. `flush()` and `close()` stay synchronous and re-raise:
that is the caller explicitly asking to wait.

The batch buffer is bounded (`max_buffer`, 10 000 events by default — the
same ceiling the Go and Node clients use). On overflow the OLDEST events go and
the count is reported through the sink: the caller is never blocked to apply
backpressure, so the only alternative to a bounded buffer is an application
heap that grows for as long as the gateway is down.

Keyword arguments: timeout, batch, interval, max_buffer, max_batch,
max_body_bytes, on_error. max_batch/max_body_bytes mirror the gateway's own
MAX_BATCH_EVENTS / MAX_BODY_BYTES and exist for an operator who retuned them;
a non-positive value keeps the default rather than removing the cap.

Levels are the gateway's PSR-3 vocabulary: debug, info, notice, warning, error
and critical have helpers, the rest go through log().

Connections are reused (http.client keeps the socket open), so a batching
client pays one TCP — and with https one TLS — handshake instead of one per
request. The endpoint is contacted directly: http_proxy/https_proxy are
ignored, which is deliberate for an internal log gateway and is also what
keeps this fork-safe on macOS, where reading the system proxy configuration in
a forked child is not.

Fork: the buffer, the background flusher and the pooled connections belong to
the process that created them. A child notices the changed pid and starts over
with an empty buffer, its own flusher and no inherited socket; whatever the
parent had buffered at fork time stays the parent's to deliver. That is what
keeps a preloaded gunicorn/uWSGI/Celery master from having every worker
re-send the same inherited backlog, and what keeps two processes from
interleaving their bytes on one TCP stream.
"""

import datetime
import http.client
import json
import os
import sys
import threading
import urllib.parse

# Mirrors the gateway's defaults: MAX_BATCH_EVENTS and MAX_BODY_BYTES. The
# gateway rejects an oversized request as a whole (413), so the client splits
# instead of losing every event in the batch.
MAX_BATCH = 1000
MAX_BODY_BYTES = 4 * 1024 * 1024

# Default buffer cap, in events. Batch mode never blocks the caller to apply
# backpressure, so a gateway that is down or slow has to hit a wall somewhere:
# without a cap the buffer is the application's heap. Generous on purpose —
# dropping is for an outage, never for normal traffic. Flat rather than a
# multiple of `batch`, and the same number the Go and Node clients use: a cap
# that moved with `batch` gave the same deployment a different memory ceiling in
# every language, which is not something an operator can reason about.
MAX_BUFFER = 10000

# Default per-request socket timeout, in seconds. Shared with the Go and Node
# clients: a full 1000-event / 4 MiB flush over a loaded link needs more than the
# 2s this used to default to, and a timeout there costs the whole chunk, which
# flush() has already taken out of the buffer.
DEFAULT_TIMEOUT = 5.0

# Idle sockets kept for reuse. A batching client needs exactly one (a single
# flusher thread); the rest is headroom for a direct-mode application logging
# from a handful of request threads. Beyond that a connection is closed after
# use, so a thread spike cannot leave a pile of sockets behind.
_MAX_IDLE_CONNECTIONS = 4

# How much of an error body to read, and how much of the reason inside it to
# keep. The gateway answers with a few dozen bytes; the caps are there because
# anything between the two (a proxy, a load balancer) can answer with a page.
_MAX_BODY_READ = 512
_MAX_REASON_CHARS = 120

# A pooled socket the other side closed while it sat idle. The gateway's
# IdleTimeout is 60s, so a low-traffic client meets this routinely.
_STALE_ERRORS = (http.client.BadStatusLine, ConnectionError)

# allow_nan=False: the default emits bare NaN/Infinity literals, which are not
# JSON — the gateway's decoder fails on that element and answers 400 for the
# WHOLE body, so one non-finite float in one context loses every event in the
# request. ensure_ascii=False keeps text as UTF-8 instead of tripling every
# non-ASCII character into a \uXXXX escape, which inflated both the wire and the
# MAX_BODY_BYTES budget the split below is measured against. default=str keeps
# the usual unserializable values (datetime, Decimal, UUID) on the wire instead
# of failing the encode.
_ENCODE = {
    "ensure_ascii": False,
    "allow_nan": False,
    "separators": (",", ":"),
    "default": str,
}

# The marker that replaces a context no encoder could serialize. Deliberately
# not the gateway's own {"_invalid_json": true} / {"_truncated": true}, so the
# two causes stay apart in ClickHouse — and deliberately the same key the Go
# and Node clients write, so one query finds every degraded row whatever wrote
# it.
_INVALID_CONTEXT = {"_unserializable": True}


class LogdenError(Exception):
    """Base for everything this client reports through on_error."""


class GatewayError(LogdenError):
    """The gateway refused a request.

    `reason` is the gateway's own {"error": "<reason>"} value when it sent one
    (auth, all_invalid, too_large, ...) — that is what turns an opaque 400
    into "your project name is invalid". It is None when the body was not that
    shape (a proxy answering for the gateway, say) and only `status` is known.
    """

    def __init__(self, status, reason=None):
        self.status = status
        self.reason = reason
        text = "gateway returned %d" % status
        if reason:
            text += ": " + reason
        super().__init__(text)


class DroppedEvents(LogdenError):
    """The batch buffer overflowed; the oldest `count` events were dropped."""

    def __init__(self, count):
        self.count = count
        super().__init__(
            "dropped %d buffered event(s): the gateway is not keeping up" % count
        )


class OversizedEvent(LogdenError):
    """One event exceeds the body cap, so no request can carry it.

    Only the identifying fields are kept, never the payload: the field that
    made the event oversized is the one the caller filled freely, this string
    lands in the application's own log, and holding the event itself would keep
    those megabytes alive for as long as the error object.
    """

    def __init__(self, event, size, limit=MAX_BODY_BYTES):
        self.size = size
        # The client's effective cap, which max_body_bytes may have moved: a
        # report quoting the module default would send the operator looking for
        # a limit this client is not actually enforcing.
        self.limit = limit
        self.level = event.get("level")
        self.timestamp = event.get("timestamp")
        super().__init__(
            "dropped one oversized event: %d bytes > %d (level=%s, timestamp=%s)"
            % (size, limit, self.level, self.timestamp)
        )


def _positive(value, fallback):
    """An override is honoured only when it is a positive number.

    None, 0 and a negative all mean "keep the default" rather than "no limit":
    an unset key in a config dict must not hand the application an unbounded
    buffer, and a zero max_batch would turn every flush into an endless split.
    The Go (WithLimits/WithMaxBuffer) and Node (positive()) clients read their
    overrides the same way.
    """
    try:
        value = int(value)
    except (TypeError, ValueError):
        return fallback
    return value if value > 0 else fallback


def _stderr_on_error(exc):
    """Default error sink: one line on stderr, and nothing that can raise.

    Deliberately not the logging module: an application that routes logging
    into this client would re-enter it from its own failure path. stderr is the
    one sink every process has.
    """
    try:
        line = "logden: %s: %s" % (type(exc).__name__, exc)
        # One line, always: an exception whose text spans several would break up
        # whatever log format the application's own stderr already has.
        sys.stderr.write(line.replace("\r", " ").replace("\n", " ") + "\n")
    except Exception:
        pass  # stderr can be closed (a daemonized process); losing the notice must not raise


def _dumps(obj):
    # errors="replace" because a lone surrogate — what surrogateescape hands
    # back for a non-UTF-8 filename, argv or environment value — makes the
    # UTF-8 encoder raise, and that exception would take down a whole chunk
    # flush() has already removed from the buffer.
    return json.dumps(obj, **_ENCODE).encode("utf-8", "replace")


def _encode(events):
    """Encode a chunk for the wire; one bad event must not cost its siblings.

    Returns (body, error). Encoding the array is all-or-nothing, and flush() has
    already taken these events out of the buffer, so a single unserializable
    context would otherwise destroy a whole chunk of events. On failure the
    events are encoded one at a time: the offending event keeps its message,
    level and timestamp with a marker context, and only a value that stays
    unencodable without its context is dropped. body is None when nothing
    survived — the gateway answers 400 "empty" to an empty array, so the caller
    must not send it.
    """
    try:
        return _dumps(events), None
    except (ValueError, TypeError) as exc:
        first_error = exc
    safe = []
    for event in events:
        try:
            _dumps(event)
        except (ValueError, TypeError):
            event = dict(event, context=dict(_INVALID_CONTEXT))
            try:
                _dumps(event)
            except (ValueError, TypeError):
                continue  # unencodable outside the context: this event alone is lost
        safe.append(event)
    if not safe:
        return None, first_error
    return _dumps(safe), first_error


def _gateway_reason(body):
    """Pull the reason out of the gateway's {"error": "<reason>"} body."""
    try:
        payload = json.loads(body.decode("utf-8", "replace"))
    except ValueError:
        return None
    if not isinstance(payload, dict):
        return None
    reason = payload.get("error")
    if not isinstance(reason, str):
        return None
    # The body is not necessarily the gateway's — anything in between can answer
    # with its own JSON — and this string is about to be written into the
    # application's log. Drop what a terminal would act on, and cap the length.
    reason = "".join(c if c.isprintable() else " " for c in reason.strip())
    return reason[:_MAX_REASON_CHARS].strip() or None


def _close_quietly(conn):
    try:
        conn.close()
    except Exception:
        pass  # a socket that is already broken must not mask the real error


def _roundtrip(conn, path, data, headers):
    """One request/response on `conn`; returns (status, body, reusable)."""
    conn.request("POST", path, body=data, headers=headers)
    resp = conn.getresponse()
    # Read a bounded prefix: the gateway's error bodies are a few dozen bytes,
    # but a proxy in between can answer with a page of HTML and this runs inside
    # the application's own process. A body that did not fit is a body left
    # undrained, so that connection cannot go back into the pool.
    body = resp.read(_MAX_BODY_READ + 1)
    reusable = len(body) <= _MAX_BODY_READ and resp.isclosed() and not resp.will_close
    return resp.status, body[:_MAX_BODY_READ], reusable


class LoggerClient:
    def __init__(
        self,
        endpoint,
        token,
        project,
        timeout=DEFAULT_TIMEOUT,
        batch=0,
        interval=1.0,
        max_buffer=None,
        max_batch=None,
        max_body_bytes=None,
        on_error=None,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.token = token
        self.project = project
        self.timeout = timeout
        # The gateway caps the client splits its requests against. Overridable
        # for a retuned gateway, exactly like Go's WithLimits and Node's
        # maxBatch/maxBodyBytes; keep an override at or below what the gateway
        # actually enforces, since only its copy is authoritative and a request
        # over the real cap comes back 413 as a whole.
        self._max_batch = _positive(max_batch, MAX_BATCH)
        self._max_body_bytes = _positive(max_body_bytes, MAX_BODY_BYTES)
        self._max_buffer = _positive(max_buffer, MAX_BUFFER)
        # The flush trigger can never sit above either ceiling. Above
        # _max_buffer it would be unreachable: the drop policy evicts an event
        # for every new one, so the buffer never gets to `batch` and only the
        # interval ever sends. Above _max_batch it would build a batch flush()
        # has to split anyway.
        self.batch = min(batch, self._max_batch, self._max_buffer)
        self._on_error = on_error or _stderr_on_error
        # Split once, at construction: a typo in the endpoint fails here instead
        # of on every send, and http.client wants host/port/path separately.
        url = urllib.parse.urlsplit(self.endpoint)
        if url.scheme not in ("http", "https") or not url.hostname:
            raise ValueError(
                "logden: endpoint must be http(s)://host[:port][/prefix], got %r"
                % (endpoint,)
            )
        self._tls = url.scheme == "https"
        self._host = url.hostname
        self._port = url.port  # None lets http.client pick 80/443
        self._path = url.path + "/logs"
        # Kept on the instance because the flusher has to be re-created with the
        # same interval in a forked child.
        self._interval = interval
        self._pid = os.getpid()
        self._buf = []
        self._dropped = 0
        self._lock = threading.Lock()
        # A second lock, never the buffer one: holding the lock log() needs
        # across a request is exactly the stall batch mode exists to avoid. It
        # guards only the idle list, never the I/O itself.
        self._conn_lock = threading.Lock()
        self._idle = []
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = None
        if self.batch > 0:
            self._start_flusher()

    def log(self, level, message, context=None):
        self._check_fork()
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
                overflow = len(self._buf) - self._max_buffer
                if overflow > 0:
                    # Drop the OLDEST. Keeping the newest is what keeps the log
                    # describing the situation as it is now: a buffer frozen at
                    # the moment it filled would hide the very outage that
                    # filled it. The count is reported through on_error, so the
                    # missing head is never a silent hole.
                    del self._buf[:overflow]
                    self._dropped += overflow
                full = len(self._buf) >= self.batch
            if full:
                # Wake the flusher instead of sending here: doing the request on
                # the caller's thread injected the gateway's latency into
                # whatever application code happened to log the event that
                # filled the batch.
                self._wake.set()
            return
        self._send([event])

    # The gateway's PSR-3 vocabulary (validate.go); alert and emergency stay
    # reachable through log() rather than growing the surface of every client.
    def debug(self, message, context=None):
        self.log("debug", message, context)

    def info(self, message, context=None):
        self.log("info", message, context)

    def notice(self, message, context=None):
        self.log("notice", message, context)

    def warning(self, message, context=None):
        self.log("warning", message, context)

    def error(self, message, context=None):
        self.log("error", message, context)

    def critical(self, message, context=None):
        self.log("critical", message, context)

    def flush(self):
        """Send what is buffered now, in requests of at most max_batch events.

        Synchronous, and re-raises the first failure: an explicit flush() is the
        caller asking to wait. The background flusher runs the same code and
        routes that failure to on_error instead.

        Concurrent log() calls can push the buffer past the gateway's cap between
        two flushes; an oversized request is rejected as a whole.
        """
        self._check_fork()
        self._report_drops()
        with self._lock:
            pending = len(self._buf)
        sent = 0
        first_error = None
        while sent < pending:
            with self._lock:
                if not self._buf:
                    break
                chunk, self._buf = (
                    self._buf[: self._max_batch],
                    self._buf[self._max_batch :],
                )
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
        """Stop the flusher, send the remainder, release the sockets.

        Idempotent: a second call flushes an empty buffer and joins a thread
        that has already exited, which is what makes __exit__ safe next to a
        caller who also closes explicitly.
        """
        self._stop.set()
        self._wake.set()  # exit now instead of waiting out the interval
        # After _stop, so that a child neither joins the parent's thread (its
        # tstate lock is a frozen copy, and join() would block for the full
        # timeout) nor starts a flusher it is about to stop.
        self._check_fork()
        if self._thread is not None:
            self._thread.join(timeout=5)
        try:
            self.flush()
        finally:
            self._close_idle()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        # A failing final flush must not replace the exception the body raised:
        # the caller would be left chasing a network error while the real fault
        # — the one that made them look at the logs — disappeared. On a clean
        # exit close() is the caller asking to wait, so its error propagates.
        if exc_type is None:
            self.close()
            return False
        try:
            self.close()
        except Exception as err:  # noqa: BLE001 - reported, not raised
            self._report(err)
        return False

    def _start_flusher(self):
        self._thread = threading.Thread(
            target=self._loop, args=(self._interval,), daemon=True
        )
        self._thread.start()

    def _report(self, exc):
        try:
            self._on_error(exc)
        except Exception:
            pass  # a sink that raises must not take down the flusher or the caller

    def _report_drops(self):
        # Coalesced into one report per flush rather than one per dropped event:
        # an overflow drops in bursts, and the sink runs on the flusher thread.
        with self._lock:
            dropped, self._dropped = self._dropped, 0
        if dropped:
            self._report(DroppedEvents(dropped))

    def _check_fork(self):
        """Start over when the pid changed, before anything touches the buffer.

        fork() clones only the calling thread: the flusher is gone in the child
        and never comes back, and a lock another thread held at fork time stays
        locked there forever, so the child's first log() would hang on it. The
        inherited buffer is the parent's to deliver — keeping it here would send
        those events once per worker.
        """
        pid = os.getpid()
        if pid == self._pid:
            return
        # Deliberately lock-free: the only locks reachable here are the ones the
        # fork may have frozen. A child has a single thread at this point.
        self._pid = pid
        self._lock = threading.Lock()
        self._conn_lock = threading.Lock()
        self._buf = []
        self._dropped = 0
        # The inherited sockets are the parent's: two processes writing into one
        # TCP stream interleave their bytes. Closing here releases only the
        # child's descriptor — the connection stays up for the parent, which
        # holds its own — while leaving them open would leak an fd per child.
        idle, self._idle = self._idle, []
        for conn in idle:
            _close_quietly(conn)
        parent_thread, self._thread = self._thread, None
        if parent_thread is None or self._stop.is_set():
            return  # not batching, or closed before the fork
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._start_flusher()

    def _loop(self, interval):
        while not self._stop.is_set():
            # wait(), not sleep(): a full batch sets _wake, so the send starts
            # at once without the caller ever touching the network. Cleared
            # before the flush, so an event recorded during the flush wakes the
            # next round instead of being lost until the interval expires.
            self._wake.wait(interval)
            self._wake.clear()
            if self._stop.is_set():
                return  # close() does the final flush, synchronously
            try:
                self.flush()
            except Exception as exc:  # noqa: BLE001 - reported, not raised
                # A network error must not permanently kill the background
                # flusher, and it must not vanish either.
                self._report(exc)

    def _send(self, events):
        data, encode_error = _encode(events)
        if data is None:
            raise encode_error  # nothing in this chunk could be encoded at all
        if len(data) > self._max_body_bytes:
            if len(events) > 1:
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
            # The split bottoms out here: one event over the cap on its own. The
            # gateway answers 413 for the whole body, so sending it is a
            # guaranteed loss that also spends the gateway's inflight budget —
            # drop it and say which event it was instead of losing it in silence.
            raise OversizedEvent(events[0], len(data), self._max_body_bytes)
        self._post(data)
        if encode_error is not None:
            # The encodable events are on their way; the caller still has to
            # hear that one of theirs was not, through the same channel as a
            # failed send — flush()/close() re-raise it, the background flusher
            # hands it to on_error.
            raise encode_error

    def _post(self, data):
        headers = {
            # Read per request, not cached at construction: rotating the token
            # on a live client (RUNBOOK's three-step rotation) then works.
            "Authorization": "Bearer " + self.token,
            "Content-Type": "application/json",
        }
        conn, reused = self._take_connection()
        try:
            status, body, reusable = _roundtrip(conn, self._path, data, headers)
        except _STALE_ERRORS:
            _close_quietly(conn)
            if not reused:
                raise
            # A pooled socket the other side had already closed — the gateway's
            # 60s IdleTimeout, or a proxy in between. The request never reached
            # anyone, so this one retry cannot duplicate an event, and without it
            # every idle client loses a whole batch to a race it can neither see
            # nor prevent. Nothing else is retried: reliability lives on the
            # gateway's spool, not here.
            conn = self._new_connection()
            try:
                status, body, reusable = _roundtrip(conn, self._path, data, headers)
            except Exception:
                _close_quietly(conn)
                raise
        except Exception:
            _close_quietly(conn)
            raise  # a half-written socket must never go back into the pool
        if reusable:
            self._put_connection(conn)
        else:
            _close_quietly(conn)
        if not 200 <= status < 300:
            raise GatewayError(status, _gateway_reason(body))

    def _new_connection(self):
        cls = http.client.HTTPSConnection if self._tls else http.client.HTTPConnection
        return cls(self._host, self._port, timeout=self.timeout)

    def _take_connection(self):
        with self._conn_lock:
            if self._idle:
                # LIFO: the most recently used socket is the least likely to
                # have been closed for being idle.
                return self._idle.pop(), True
        return self._new_connection(), False

    def _put_connection(self, conn):
        with self._conn_lock:
            if len(self._idle) < _MAX_IDLE_CONNECTIONS:
                self._idle.append(conn)
                return
        _close_quietly(conn)

    def _close_idle(self):
        with self._conn_lock:
            idle, self._idle = self._idle, []
        for conn in idle:
            _close_quietly(conn)
