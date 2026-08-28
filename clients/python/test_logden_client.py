"""Tests for the logden Python client (stdlib only: unittest + http.server).

    cd clients/python && python3 -m unittest -v

Every request the stub gateway receives is parsed with parse_constant wired to
fail, so any test that puts a bare NaN/Infinity literal on the wire — which the
Go gateway answers with a 400 for the whole body — fails here first.
"""

import contextlib
import datetime
import http.client
import io
import json
import math
import os
import socket
import threading
import time
import traceback
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from logden_client import (
    MAX_BATCH,
    MAX_BODY_BYTES,
    MAX_BUFFER,
    DroppedEvents,
    GatewayError,
    LoggerClient,
    OversizedEvent,
    _stderr_on_error,
)

# allowedLevels in gateway/validate.go. A helper that sends anything outside
# this set is normalized to "info" by the gateway, silently.
GATEWAY_LEVELS = {
    "debug",
    "info",
    "notice",
    "warning",
    "error",
    "critical",
    "alert",
    "emergency",
}


def _reject_constant(name):
    raise AssertionError("body carries a bare %s literal, which is not JSON" % name)


class _Collector(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        with self.server.lock:
            self.server.connections += 1  # one per accepted TCP connection

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        with self.server.lock:
            self.server.bodies.append(body)
            self.server.auth.append(self.headers.get("Authorization"))
            self.server.paths.append(self.path)
            status, reply, delay = (
                self.server.status,
                self.server.reply,
                self.server.delay,
            )
        if delay:
            time.sleep(delay)
        payload = reply.encode() if reply else b""
        self.send_response(status)
        self.send_header("Content-Length", str(len(payload)))
        if payload:
            self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def log_message(self, *args):
        pass  # the default handler writes every request to stderr


class _StubGateway(ThreadingHTTPServer):
    daemon_threads = True
    # socketserver defaults the listen backlog to 5, and a burst of concurrent
    # senders overflows it: macOS answers the overflow with an RST, so the test
    # would blame the client for the stub's queue. The Go gateway listens with
    # the OS maximum.
    request_queue_size = 128


class GatewayCase(unittest.TestCase):
    """Runs a stub gateway on a loopback port and closes every client it hands out."""

    def setUp(self):
        self.server = _StubGateway(("127.0.0.1", 0), _Collector)
        self.server.lock = threading.Lock()
        self.server.bodies = []
        self.server.auth = []
        self.server.paths = []
        self.server.connections = 0
        # What the stub answers with; a test that needs a failure changes these.
        self.server.status = 204
        self.server.reply = None
        self.server.delay = 0
        self.endpoint = "http://127.0.0.1:%d" % self.server.server_address[1]
        # A short poll interval: shutdown() waits for serve_forever to notice,
        # and the default 0.5s would dominate the runtime of every test.
        self.serving = threading.Thread(
            target=self.server.serve_forever, args=(0.01,), daemon=True
        )
        self.serving.start()
        self.addCleanup(self._shutdown)

    def _shutdown(self):
        self.server.shutdown()
        self.server.server_close()
        self.serving.join(timeout=5)
        self.assertFalse(self.serving.is_alive(), "stub gateway thread still running")

    def client(self, **kwargs):
        log = LoggerClient(self.endpoint, "test-token", "proj", timeout=5, **kwargs)
        # LIFO cleanups: clients are closed before the gateway goes away, so no
        # flusher thread outlives the test.
        self.addCleanup(self._close_quietly, log)
        return log

    def _close_quietly(self, log):
        try:
            log.close()
        except Exception:
            pass
        if log._thread is not None:
            self.assertFalse(log._thread.is_alive(), "flusher thread still running")

    def bodies(self):
        with self.server.lock:
            return list(self.server.bodies)

    def requests(self):
        return [json.loads(b, parse_constant=_reject_constant) for b in self.bodies()]

    def events(self):
        return [event for req in self.requests() for event in req]

    def messages(self):
        return [event["message"] for event in self.events()]

    def wait_for(self, predicate, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not predicate() and time.monotonic() < deadline:
            time.sleep(0.005)
        return predicate()


class SplitTest(GatewayCase):
    def test_flush_splits_at_max_batch_events(self):
        log = self.client()
        # Concurrent log() calls can push the buffer past MAX_BATCH between two
        # flushes, which no public call can produce deterministically; the
        # gateway would answer 413 for the whole request.
        log._buf = [
            {"project": "proj", "level": "info", "message": "e%d" % i}
            for i in range(MAX_BATCH + 7)
        ]
        log.flush()
        self.assertEqual([len(req) for req in self.requests()], [MAX_BATCH, 7])
        self.assertEqual(self.messages()[-1], "e%d" % (MAX_BATCH + 6))
        self.assertEqual(self.server.auth[0], "Bearer test-token")

    def test_flush_splits_at_max_body_bytes(self):
        log = self.client(batch=4, interval=60)
        blob = "x" * (1500 * 1000)
        for i in range(4):
            log.info("big %d" % i, {"blob": blob})
        self.assertTrue(self.wait_for(lambda: len(self.bodies()) >= 2))
        bodies = self.bodies()
        self.assertEqual(len(bodies), 2, "one 6 MB request instead of two halves")
        for body in bodies:
            self.assertLessEqual(len(body), MAX_BODY_BYTES)
        self.assertEqual(len(self.events()), 4)

    def test_caps_are_overridable_and_nonsense_falls_back(self):
        # A retuned gateway moves MAX_BATCH_EVENTS / MAX_BODY_BYTES, and the
        # split has to follow it or every request comes back 413 as a whole.
        tuned = self.client(max_batch=10, max_body_bytes=4096, max_buffer=64)
        self.assertEqual((tuned._max_batch, tuned._max_body_bytes), (10, 4096))
        self.assertEqual(tuned._max_buffer, 64)
        # Not "no limit": an unset key must never hand the application an
        # unbounded buffer, and a zero max_batch would split forever.
        sloppy = self.client(max_batch=0, max_body_bytes=-1, max_buffer=None)
        self.assertEqual(sloppy._max_batch, MAX_BATCH)
        self.assertEqual(sloppy._max_body_bytes, MAX_BODY_BYTES)
        self.assertEqual(sloppy._max_buffer, MAX_BUFFER)
        # The flush trigger cannot exceed either ceiling.
        clamped = self.client(batch=500, max_buffer=10, interval=60)
        self.assertEqual(clamped.batch, 10)

    def test_max_batch_override_drives_the_split(self):
        log = self.client(max_batch=10)
        log._buf = [
            {"project": "proj", "level": "info", "message": "e%d" % i}
            for i in range(23)
        ]
        log.flush()
        self.assertEqual([len(req) for req in self.requests()], [10, 10, 3])

    def test_max_body_bytes_override_drives_the_split(self):
        log = self.client(batch=4, interval=60, max_body_bytes=16 * 1024)
        for i in range(4):
            log.info("big %d" % i, {"blob": "x" * 6000})
        self.assertTrue(self.wait_for(lambda: len(self.events()) == 4))
        for body in self.bodies():
            self.assertLessEqual(len(body), 16 * 1024)

    def test_non_ascii_is_not_escaped(self):
        log = self.client()
        log.info("café — 200 µs")
        self.assertEqual(self.messages(), ["café — 200 µs"])
        # \uXXXX escaping tripled every non-ASCII body against the same 4 MiB cap.
        self.assertIn("café".encode(), self.bodies()[0])

    def test_oversized_single_event_is_reported_and_costs_no_sibling(self):
        log = self.client(batch=1000, interval=60)
        log.info("before")
        log.info("huge", {"blob": "x" * (5 * 1024 * 1024)})
        log.info("after")
        with self.assertRaises(OversizedEvent) as caught:
            log.flush()
        # The split bottoms out at the one event that cannot fit; its siblings
        # still go out, and the loss is reported instead of silent.
        self.assertEqual(self.messages(), ["before", "after"])
        self.assertGreater(caught.exception.size, MAX_BODY_BYTES)
        self.assertEqual(caught.exception.level, "info")
        text = str(caught.exception)
        self.assertNotIn("xxxx", text, "the report carries the payload")
        self.assertIn("level=info", text)

    def test_oversized_single_event_reaches_on_error_in_batch_mode(self):
        errors = []
        log = self.client(batch=1, interval=0.01, on_error=errors.append)
        log.info("huge", {"blob": "x" * (5 * 1024 * 1024)})
        self.assertTrue(self.wait_for(lambda: errors))
        self.assertIsInstance(errors[0], OversizedEvent)
        self.assertEqual(self.bodies(), [], "a body the gateway would 413 was sent")


class LevelTest(GatewayCase):
    def test_helpers_cover_the_gateway_vocabulary(self):
        names = ["debug", "info", "notice", "warning", "error", "critical"]
        log = self.client(batch=100, interval=60)
        for name in names:
            getattr(log, name)(name)
        log.close()
        levels = [event["level"] for event in self.events()]
        self.assertEqual(levels, names)
        # A level outside the gateway's set is silently rewritten to "info".
        self.assertTrue(GATEWAY_LEVELS.issuperset(levels))

    def test_log_reaches_the_levels_without_a_helper(self):
        log = self.client()
        log.log("emergency", "the datacenter is on fire")
        self.assertEqual(self.events()[0]["level"], "emergency")


class TimestampTest(GatewayCase):
    def test_timestamp_is_stamped_when_the_event_is_recorded(self):
        log = self.client(batch=100, interval=60)
        before = datetime.datetime.now(datetime.timezone.utc)
        log.info("first")
        after = datetime.datetime.now(datetime.timezone.utc)
        time.sleep(0.05)
        log.info("second")
        log.close()
        first, second = (
            datetime.datetime.fromisoformat(event["timestamp"])
            for event in self.events()
        )
        # The send happens after the sleep: a timestamp taken at flush time
        # would sit past `after`.
        self.assertLessEqual(before, first)
        self.assertLessEqual(first, after)
        self.assertLess(first, second)


class EncodeTest(GatewayCase):
    def test_non_finite_context_never_reaches_the_wire(self):
        log = self.client()
        with self.assertRaises(ValueError):
            log.info("ratio", {"ratio": float("nan"), "peak": math.inf})
        # requests() rejects bare NaN/Infinity, so parsing is the assertion.
        event = self.events()[0]
        self.assertEqual(event["message"], "ratio")
        self.assertEqual(event["context"], {"_unserializable": True})

    def test_non_finite_context_costs_only_its_own_event(self):
        log = self.client(batch=100, interval=60)
        log.info("good one")
        log.info("bad", {"ratio": float("nan")})
        log.info("good two")
        with self.assertRaises(ValueError):
            log.flush()
        self.assertEqual(self.messages(), ["good one", "bad", "good two"])
        events = self.events()
        self.assertNotIn("context", events[0])
        self.assertEqual(events[1]["context"], {"_unserializable": True})

    def test_lone_surrogate_does_not_raise(self):
        log = self.client(batch=100, interval=60)
        # What surrogateescape hands back for a non-UTF-8 filename; encoding it
        # strictly raises UnicodeEncodeError and would lose the whole chunk.
        name = b"file \xff.txt".decode("utf-8", "surrogateescape")
        log.info(name, {"path": name})
        log.info("after the surrogate")
        log.flush()
        self.assertEqual(self.messages(), ["file ?.txt", "after the surrogate"])

    def test_unserializable_context_costs_only_its_own_context(self):
        log = self.client(batch=100, interval=60)
        cyclic = {}
        cyclic["self"] = cyclic
        log.info("good one", {"order_id": 7})
        log.info("cyclic", cyclic)
        log.info("good two")
        with self.assertRaises(ValueError):
            log.flush()
        events = self.events()
        self.assertEqual(
            [e["message"] for e in events], ["good one", "cyclic", "good two"]
        )
        self.assertEqual(events[0]["context"], {"order_id": 7})
        self.assertEqual(events[1]["context"], {"_unserializable": True})

    def test_event_unencodable_beyond_its_context_is_dropped_alone(self):
        log = self.client(batch=100, interval=60)
        cyclic = {}
        cyclic["self"] = cyclic
        log.info("good one")
        log.log("info", cyclic)  # the message itself cannot be encoded
        log.info("good two")
        with self.assertRaises(ValueError):
            log.flush()
        self.assertEqual(self.messages(), ["good one", "good two"])

    def test_chunk_that_cannot_be_encoded_at_all_sends_nothing(self):
        log = self.client(batch=100, interval=60)
        cyclic = {}
        cyclic["self"] = cyclic
        log.log("info", cyclic)
        with self.assertRaises(ValueError):
            log.flush()
        # An empty array is a 400 "empty" at the gateway.
        self.assertEqual(self.bodies(), [])

    def test_datetime_context_is_stringified(self):
        log = self.client()
        log.info("when", {"at": datetime.date(2026, 8, 28)})
        self.assertEqual(self.events()[0]["context"], {"at": "2026-08-28"})


class FlushTest(GatewayCase):
    def test_close_flushes_the_remainder(self):
        log = self.client(batch=100, interval=60)
        log.info("one")
        log.info("two")
        self.assertEqual(self.bodies(), [], "flushed before close()")
        log.close()
        self.assertEqual(self.messages(), ["one", "two"])
        self.assertFalse(log._thread.is_alive())

    def test_background_flusher_sends_on_the_interval(self):
        log = self.client(batch=100, interval=0.02)
        log.info("periodic")
        self.assertTrue(self.wait_for(lambda: self.messages() == ["periodic"]))

    def test_full_batch_flushes_without_close(self):
        log = self.client(batch=3, interval=60)
        for i in range(3):
            log.info("e%d" % i)
        self.assertTrue(self.wait_for(lambda: len(self.events()) == 3))

    def test_close_is_idempotent(self):
        log = self.client(batch=100, interval=60)
        log.info("one")
        log.close()
        log.close()
        self.assertEqual(self.messages(), ["one"])


class NonBlockingTest(GatewayCase):
    def test_a_full_batch_does_not_send_on_the_callers_thread(self):
        self.server.delay = 0.5
        log = self.client(batch=2, interval=60)
        start = time.monotonic()
        for i in range(4):  # two full batches, so two sends are triggered
            log.info("e%d" % i)
        elapsed = time.monotonic() - start
        # Before the flusher handled the batch-full case, the log() call that
        # filled the batch performed the request itself, so a slow gateway
        # stalled whatever application code happened to make that call.
        self.assertLess(elapsed, 0.25, "log() waited for the gateway")
        self.assertTrue(self.wait_for(lambda: len(self.events()) == 4, timeout=10))

    def test_explicit_flush_still_waits(self):
        self.server.delay = 0.2
        log = self.client(batch=1000, interval=60)
        log.info("one")
        start = time.monotonic()
        log.flush()
        # flush() is the caller asking to wait: the events are on the gateway
        # by the time it returns, which is what makes close() a valid last act.
        self.assertGreaterEqual(time.monotonic() - start, 0.2)
        self.assertEqual(self.messages(), ["one"])


class BufferCapTest(GatewayCase):
    def test_default_cap_is_generous(self):
        log = self.client(batch=500, interval=60)
        # Dropping is for an outage, not for normal traffic. Flat, and the same
        # ceiling the Go and Node clients use: the same deployment must not get
        # a different memory ceiling per language.
        self.assertEqual(log._max_buffer, MAX_BUFFER)

    def test_overflow_drops_the_oldest_and_reports_it(self):
        errors = []
        # batch=1000 with interval=60 keeps the flusher parked for the whole
        # test, so the cap is the only thing acting on the buffer. The cap is
        # lowered after construction because __init__ clamps the flush trigger
        # down to it, which would wake the flusher mid-test.
        log = self.client(batch=1000, interval=60, on_error=errors.append)
        log._max_buffer = 3
        for i in range(5):
            log.info("e%d" % i)
        self.assertEqual(errors, [], "reported before anything was flushed")
        log.flush()
        # The newest survive: a buffer frozen at the moment it filled would
        # describe the start of the outage and nothing since.
        self.assertEqual(self.messages(), ["e2", "e3", "e4"])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], DroppedEvents)
        self.assertEqual(errors[0].count, 2)
        self.assertIn("2", str(errors[0]))

    def test_drops_are_reported_once(self):
        errors = []
        log = self.client(batch=1000, interval=60, on_error=errors.append)
        log._max_buffer = 1  # after construction: see the test above
        log.info("one")
        log.info("two")
        log.flush()
        log.flush()
        self.assertEqual(len(errors), 1, "the same drop was reported twice")

    def test_overflow_never_blocks_the_caller(self):
        self.server.delay = 0.5
        # A sink of its own: the default one would write every drop to the
        # stderr of the test run.
        log = self.client(batch=2, interval=60, max_buffer=4, on_error=lambda e: None)
        start = time.monotonic()
        for i in range(50):
            log.info("e%d" % i)
        self.assertLess(time.monotonic() - start, 0.25, "log() applied backpressure")


class ErrorSinkTest(GatewayCase):
    def test_background_failure_reaches_on_error_with_the_gateway_reason(self):
        self.server.status = 400
        self.server.reply = '{"error":"all_invalid"}'
        errors = []
        log = self.client(batch=1, interval=0.01, on_error=errors.append)
        log.info("nope")
        self.assertTrue(self.wait_for(lambda: errors))
        self.assertIsInstance(errors[0], GatewayError)
        self.assertEqual(errors[0].status, 400)
        self.assertEqual(errors[0].reason, "all_invalid")
        self.assertIn("all_invalid", str(errors[0]))

    def test_status_only_when_the_body_is_not_the_gateway_shape(self):
        self.server.status = 502
        self.server.reply = "<html>bad gateway</html>"
        log = self.client()
        with self.assertRaises(GatewayError) as caught:
            log.info("nope")
        self.assertEqual(caught.exception.status, 502)
        self.assertIsNone(caught.exception.reason)
        self.assertIn("502", str(caught.exception))

    def test_reason_from_a_proxy_cannot_carry_control_characters(self):
        self.server.status = 400
        self.server.reply = json.dumps({"error": "bad \x1b[31mred\x07"})
        log = self.client()
        with self.assertRaises(GatewayError) as caught:
            log.info("nope")
        # The reason lands in the application's own log; an escape sequence
        # there is an injection into whoever reads it.
        self.assertNotIn("\x1b", caught.exception.reason)
        self.assertNotIn("\x07", caught.exception.reason)
        self.assertIn("red", caught.exception.reason)

    def test_a_raising_sink_does_not_kill_the_flusher(self):
        def boom(exc):
            raise RuntimeError("the sink itself is broken")

        self.server.status = 500
        log = self.client(batch=1, interval=0.01, on_error=boom)
        log.info("one")
        self.assertTrue(self.wait_for(lambda: len(self.bodies()) == 1))
        self.server.status = 204
        log.info("two")
        self.assertTrue(self.wait_for(lambda: len(self.bodies()) == 2))
        self.assertTrue(log._thread.is_alive())

    def test_default_sink_writes_one_line_to_stderr(self):
        self.server.status = 401
        self.server.reply = '{"error":"auth"}'
        captured = io.StringIO()
        log = self.client(batch=1, interval=0.01)
        with contextlib.redirect_stderr(captured):
            log.info("nope")
            self.assertTrue(self.wait_for(lambda: "auth" in captured.getvalue()))
        line = captured.getvalue().strip()
        self.assertEqual(len(line.splitlines()), 1)
        self.assertTrue(line.startswith("logden: GatewayError:"), line)

    def test_default_sink_collapses_a_multiline_error(self):
        captured = io.StringIO()
        with contextlib.redirect_stderr(captured):
            _stderr_on_error(ValueError("first\nsecond"))
        # One line: the application's stderr already has a format of its own.
        self.assertEqual(captured.getvalue(), "logden: ValueError: first second\n")

    def test_default_sink_survives_a_closed_stderr(self):
        closed = io.StringIO()
        closed.close()
        with contextlib.redirect_stderr(closed):
            # A daemonized process has no stderr; losing the notice is fine,
            # raising from the flusher thread is not.
            _stderr_on_error(RuntimeError("boom"))

    def test_direct_mode_still_raises(self):
        self.server.status = 400
        self.server.reply = '{"error":"too_large"}'
        log = self.client(on_error=lambda exc: None)
        # Outside batch mode the caller holds the send, so the error belongs to
        # them; the sink is for what happens in the background.
        with self.assertRaises(GatewayError):
            log.info("nope")


class ConnectionTest(GatewayCase):
    def test_connection_is_reused_across_sends(self):
        log = self.client()
        for i in range(3):
            log.info("e%d" % i)
        self.assertEqual(len(self.bodies()), 3)
        # One handshake for three events instead of three.
        self.assertEqual(self.server.connections, 1)

    def test_close_releases_the_pooled_connection(self):
        log = self.client()
        log.info("one")
        self.assertEqual(len(log._idle), 1)
        log.close()
        self.assertEqual(log._idle, [])

    def test_error_responses_keep_the_connection_usable(self):
        self.server.status = 400
        self.server.reply = '{"error":"all_invalid"}'
        log = self.client()
        for _ in range(3):
            with self.assertRaises(GatewayError):
                log.info("nope")
        self.assertEqual(self.server.connections, 1)

    def test_concurrent_senders_share_the_pool(self):
        log = self.client()
        failures = []

        def worker(n):
            try:
                for i in range(20):
                    log.info("t%d-%d" % (n, i))
            except Exception as exc:  # noqa: BLE001 - asserted below
                failures.append(exc)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        self.assertEqual(failures, [])
        self.assertEqual(len(self.events()), 160)
        # Reuse under contention, without a socket per call and without the
        # idle list growing past its cap.
        self.assertLess(self.server.connections, 160)
        self.assertLessEqual(len(log._idle), 4)

    def test_https_endpoint_gets_a_tls_connection(self):
        log = LoggerClient("https://logs.internal/edge/", "t", "proj")
        conn = log._new_connection()  # constructing one does not connect
        self.addCleanup(conn.close)
        self.assertIsInstance(conn, http.client.HTTPSConnection)
        self.assertEqual(conn.port, 443)
        self.assertEqual(log._path, "/edge/logs")

    def test_endpoint_path_prefix_is_kept(self):
        log = LoggerClient(self.endpoint + "/edge/", "test-token", "proj", timeout=5)
        self.addCleanup(log.close)
        log.info("behind a prefix")
        self.assertEqual(self.server.paths, ["/edge/logs"])

    def test_bad_endpoint_fails_at_construction(self):
        for bad in ("", "logs.internal:8080", "ftp://logs.internal", "http://"):
            with self.assertRaises(ValueError, msg=bad):
                LoggerClient(bad, "t", "proj")


class ContextManagerTest(GatewayCase):
    def test_with_block_closes_the_client(self):
        with self.client(batch=100, interval=60) as log:
            log.info("inside")
            self.assertEqual(self.bodies(), [], "flushed before the block ended")
        self.assertEqual(self.messages(), ["inside"])
        self.assertFalse(log._thread.is_alive())

    def test_exit_raises_on_a_clean_block(self):
        self.server.status = 500
        with self.assertRaises(GatewayError):
            with self.client(batch=100, interval=60, on_error=lambda e: None) as log:
                log.info("inside")

    def test_exit_does_not_mask_the_bodys_exception(self):
        self.server.status = 500
        errors = []
        with self.assertRaises(ValueError):
            with self.client(batch=100, interval=60, on_error=errors.append) as log:
                log.info("inside")
                raise ValueError("the real failure")
        # The application's exception is what the caller has to see; the failed
        # flush goes to the sink instead of replacing it.
        self.assertTrue(any(isinstance(e, GatewayError) for e in errors), errors)


class _RawGateway:
    """A listener that hangs up on the first connection after answering it.

    ThreadingHTTPServer never closes an idle keep-alive connection, and that is
    exactly the case a pooling client has to survive: the gateway's IdleTimeout
    (60s) drops sockets a low-traffic client is still holding.
    """

    def __init__(self):
        self.sock = socket.socket()
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.sock.settimeout(0.02)
        self.port = self.sock.getsockname()[1]
        self.bodies = []
        self.hung_up = threading.Event()
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.thread.start()

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=5)
        self.sock.close()

    def _accept_loop(self):
        served = 0
        while not self.stopped.is_set():
            try:
                conn, _ = self.sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            served += 1
            hangup = served == 1
            try:
                self._serve(conn, hangup)
            finally:
                conn.close()
            if hangup:
                self.hung_up.set()

    def _serve(self, conn, hangup):
        buf = b""
        while True:
            while b"\r\n\r\n" not in buf:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                buf += chunk
            head, _, buf = buf.partition(b"\r\n\r\n")
            length = 0
            for line in head.split(b"\r\n")[1:]:
                name, _, value = line.partition(b":")
                if name.strip().lower() == b"content-length":
                    length = int(value)
            while len(buf) < length:
                chunk = conn.recv(65536)
                if not chunk:
                    return
                buf += chunk
            self.bodies.append(buf[:length])
            buf = buf[length:]
            conn.sendall(b"HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
            if hangup:
                return


class StaleConnectionTest(unittest.TestCase):
    def setUp(self):
        self.gateway = _RawGateway()
        self.addCleanup(self.gateway.close)

    def test_a_hung_up_pooled_connection_is_retried_once(self):
        log = LoggerClient(
            "http://127.0.0.1:%d" % self.gateway.port, "t", "proj", timeout=5
        )
        self.addCleanup(log.close)
        log.info("first")
        self.assertTrue(self.gateway.hung_up.wait(5), "the stub kept the connection")
        log.info("second")  # takes the pooled socket, which is already dead
        messages = [json.loads(b)[0]["message"] for b in self.gateway.bodies]
        self.assertEqual(messages, ["first", "second"], "the retry lost or duplicated")

    def test_an_unreachable_gateway_is_not_retried_away(self):
        probe = socket.socket()
        probe.bind(("127.0.0.1", 0))
        dead_port = probe.getsockname()[1]
        probe.close()
        log = LoggerClient("http://127.0.0.1:%d" % dead_port, "t", "proj", timeout=2)
        self.addCleanup(log.close)
        # A fresh connection that fails is a real failure: reporting it is the
        # whole point, and the client never retries a delivery.
        with self.assertRaises(OSError):
            log.info("nope")


@unittest.skipUnless(hasattr(os, "fork"), "fork() is unavailable on this platform")
class ForkTest(GatewayCase):
    def parent_with_a_buffered_event(self):
        log = self.client(batch=100, interval=60)
        # One real send first: it leaves a pooled connection behind, which is
        # exactly the state a child must not inherit.
        log.info("warm up")
        log.flush()
        self.assertEqual(len(log._idle), 1)
        log.info("parent only")
        return log

    def run_child(self, body):
        pid = os.fork()
        if pid == 0:
            code = 0
            try:
                code = body()
            except BaseException:  # noqa: BLE001 - reported through the exit code
                traceback.print_exc()
                code = 1
            os._exit(code)
        _, status = os.waitpid(pid, 0)
        self.assertTrue(os.WIFEXITED(status), "child died on a signal")
        self.assertEqual(os.WEXITSTATUS(status), 0, "child reported a failure")

    def test_child_starts_with_an_empty_buffer_and_a_live_flusher(self):
        log = self.parent_with_a_buffered_event()

        def child():
            log.info("from child")
            if log._thread is None or not log._thread.is_alive():
                return 3  # the flusher did not survive the fork
            if [e["message"] for e in log._buf] != ["from child"]:
                return 4  # the parent's buffer came along
            if log._idle:
                return 5  # the parent's socket came along: two writers, one stream
            log.close()
            return 0

        self.run_child(child)
        log.close()
        # "parent only" is delivered once, by the parent that recorded it.
        self.assertEqual(
            sorted(self.messages()), ["from child", "parent only", "warm up"]
        )
        # The child opened its own connection instead of sharing the parent's.
        self.assertGreaterEqual(self.server.connections, 2)

    def test_child_close_does_not_resend_the_parents_buffer(self):
        log = self.parent_with_a_buffered_event()

        def child():
            started = time.monotonic()
            log.close()
            # A child that joins the parent's thread object waits out the full
            # join timeout on a lock the fork froze.
            return 0 if time.monotonic() - started < 2 else 6

        self.run_child(child)
        self.assertEqual(self.messages(), ["warm up"])
        log.close()
        self.assertEqual(self.messages(), ["warm up", "parent only"])

    def test_parent_keeps_using_its_connection_after_a_child_exits(self):
        log = self.parent_with_a_buffered_event()
        before = self.server.connections

        def child():
            log.info("from child")
            log.close()  # closes the child's copy of the inherited descriptor
            return 0

        self.run_child(child)
        log.flush()
        # The parent's socket outlives the child closing its own descriptor:
        # the kernel keeps the connection until the last one goes.
        self.assertIn("parent only", self.messages())
        self.assertEqual(self.server.connections, before + 1, "the parent reconnected")


if __name__ == "__main__":
    unittest.main()
