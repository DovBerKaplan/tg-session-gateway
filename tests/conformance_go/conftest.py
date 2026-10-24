"""Black-box conformance: the REAL tggw binary against a fake Telegram.

The Go port's compatibility proof (plan, stage D): the gateway
lifecycle runs end-to-end over HTTP with only the base URL swapped —
no Go internals imported, no in-process fakes. The Bot API plane is
pure HTTP so it conformance-tests fully; the MTProto plane needs a
real Telegram handshake, so its behavior contract lives in the Go test
suite (internal/mtproto) and the cutover dry-run.

Usage: TGGW_BIN=/path/to/tggw pytest -q tests/conformance_go
"""

from __future__ import annotations

import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

TGGW = os.environ.get("TGGW_BIN") or os.environ.get("TGGW_BIN_PATH")
TOKEN = "700000:conformance"
ADM = {"authorization": "Bearer adm-secret"}
APP = {"authorization": "Bearer app-secret"}


def require_binary() -> str:
    if not TGGW:
        pytest.fail("TGGW_BIN env var must point at the built tggw binary")
    return TGGW


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class FakeTelegram:
    """Answers Bot API calls like api.telegram.org would."""

    def __init__(self):
        self.lock = threading.Lock()
        self.getupdates_calls = 0
        self.sent: list[dict] = []
        # updates the test injects after boot (simulates Telegram
        # delivering an update mid-session); served like the batch:
        # everything with update_id >= the poller's offset, until acked
        self.pending: list[dict] = []
        # when set, every method EXCEPT getUpdates answers 500 (upstream
        # outage simulation without tearing down the server)
        self.fails = False
        self._closed = False
        # long-poll wake: like real Telegram, getUpdates with
        # timeout > 0 HOLDS until an update for that offset arrives —
        # without this the gateway's poller hot-spins (timeout=0) and
        # a pure-Python fake becomes the bottleneck of every benchmark
        self._cond = threading.Condition()
        self.batch = [
            {"update_id": 1001, "message": {"text": "first"}},
            {"update_id": 1002, "message": {"text": "second"}},
        ]
        handler = type("H", (BaseHTTPRequestHandler,), {"log_message": lambda *a: None})
        # production-realistic socket behavior: without these, the fake
        # itself becomes the benchmark bottleneck — HTTP/1.0 closes
        # every connection, and with Nagle ON the second small write
        # stalls ~40ms on the client's delayed ACK (the classic
        # Nagle+delayed-ACK interplay). Real api.telegram.org has
        # TCP_NODELAY + keep-alive; the fake must too.
        handler.protocol_version = "HTTP/1.1"
        handler.disable_nagle_algorithm = True
        handler.do_POST = lambda rh, _f=self: _f._do_post(rh)
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def _do_post(self, handler):
        body = {}
        length = int(handler.headers.get("Content-Length") or 0)
        if length:
            body = json.loads(handler.rfile.read(length) or b"{}")
        path = handler.path
        if self.fails and not path.endswith("/getUpdates"):
            payload = json.dumps({"ok": False, "error_code": 500,
                                  "description": "fake upstream outage"}).encode()
            handler.send_response(500)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("Content-Length", str(len(payload)))
            handler.end_headers()
            handler.wfile.write(payload)
            return
        result, status = {"ok": True, "result": True}, 200

        if path.endswith("/deleteWebhook"):
            result = {"ok": True, "result": True}
        elif path.endswith("/getUpdates"):
            with self._cond:
                self.getupdates_calls += 1
                calls = self.getupdates_calls
                offset = int(body.get("offset") or 0)
                source = self.batch if calls == 1 else self.pending

                def _avail():
                    return any(u["update_id"] >= offset for u in source)

                if not _avail():
                    hold = float(body.get("timeout") or 0)
                    if hold > 0 and not self._closed:
                        self._cond.wait_for(
                            lambda: self._closed or _avail(), timeout=hold)
                updates = [u for u in source if u["update_id"] >= offset]
            result = {"ok": True, "result": updates}
        elif path.endswith("/sendMessage"):
            with self.lock:
                self.sent.append(body)
            result = {"ok": True, "result": {"message_id": 42, "text": body.get("text")}}
        else:
            result = {"ok": True, "result": True}

        payload = json.dumps(result).encode()
        handler.send_response(status)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)

    def deliver(self, update: dict) -> None:
        """Inject one update as if Telegram had just produced it.
        Wakes any held long-poll, exactly like Telegram would."""
        with self._cond:
            self.pending.append(update)
            self._cond.notify_all()

    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def close(self):
        # idempotent + closes the LISTENING socket, so a test can kill
        # the upstream mid-life and observe real connection-refused
        if self._closed:
            return
        with self._cond:
            self._closed = True
            self._cond.notify_all()  # release held long-polls
        self.server.shutdown()
        self.server.server_close()


class Gateway:
    """One tggw botapi process lifetime."""

    def __init__(self, data_dir: str, upstream: str, extra_env: dict | None = None):
        self.port = free_port()
        self.proc = subprocess.Popen(
            [
                require_binary(),
                "botapi",
            ],
            env={
                **os.environ,
                "GW_HOST": "127.0.0.1",
                "GW_PORT": str(self.port),
                "GW_DATA_DIR": data_dir,
                "GW_UPSTREAM": upstream,
                "GW_FERNET_KEY": "CfR-eL8QzhZJQb22PBP1zxH7CzJMFK4LwKvFACh1pfw=",
                "GW_ADMIN_SECRET": "adm-secret",
                "GW_APP_SECRET": "app-secret",
                "GW_POLL_TIMEOUT": "0",
                "GW_POLL_BACKOFF_MAX": "0.1",
                "GW_ALLOW_EPHEMERAL_DATA": "1",
                **(extra_env or {}),
            },
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        self.base = f"http://127.0.0.1:{self.port}"

    def wait_ready(self, timeout: float = 10.0):
        import httpx

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                r = httpx.get(f"{self.base}/healthz", timeout=1)
                if r.status_code == 200:
                    return
            except Exception:
                pass
            time.sleep(0.1)
        self.stop()
        raise AssertionError(f"tggw did not become ready; output=\n{self.output()}")

    def output(self) -> str:
        try:
            self.proc.stdout.flush()  # type: ignore[union-attr]
        except Exception:
            pass
        # non-blocking read of what's buffered
        import fcntl

        fd = self.proc.stdout.fileno()  # type: ignore[union-attr]
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        try:
            return (self.proc.stdout.read() or b"").decode(errors="replace")  # type: ignore[union-attr]
        finally:
            fcntl.fcntl(fd, fcntl.F_SETFL, fl)

    def stop(self):
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


@pytest.fixture()
def fake_telegram():
    fake = FakeTelegram()
    yield fake
    fake.close()


@pytest.fixture()
def data_dir():
    with tempfile.TemporaryDirectory(prefix="tggw-conf-") as d:
        yield d


@pytest.fixture()
def gw(fake_telegram, data_dir):
    g = Gateway(data_dir, fake_telegram.url())
    g.wait_ready()
    yield g
    g.stop()


def wait_for(predicate, timeout: float = 8.0, what: str = "condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError(f"timed out waiting for {what}")


def make_pyrofork_session(path: str) -> None:
    """A real pyrofork-layout .session file (sqlite, exact DDL)."""
    db = sqlite3.connect(path)
    db.executescript(
        """
CREATE TABLE sessions (dc_id INTEGER PRIMARY KEY, api_id INTEGER, test_mode INTEGER,
    auth_key BLOB, date INTEGER NOT NULL, user_id INTEGER, is_bot INTEGER);
CREATE TABLE peers (id INTEGER PRIMARY KEY, access_hash INTEGER, type INTEGER NOT NULL,
    username TEXT, phone_number TEXT, last_update_on INTEGER NOT NULL);
CREATE TABLE update_state (id INTEGER PRIMARY KEY, pts INTEGER, qts INTEGER,
    date INTEGER, seq INTEGER);
CREATE TABLE version (number INTEGER PRIMARY KEY);
"""
    )
    key = bytes((i * 7) % 256 for i in range(256))
    db.execute("INSERT INTO sessions VALUES (2, 12345, 0, ?, 1700000000, 42, 1)", (key,))
    db.commit()
    db.close()


# the tests import nothing from the repo — everything goes over HTTP
_ = sys.modules[__name__]
