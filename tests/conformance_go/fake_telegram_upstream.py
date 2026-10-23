"""In-cluster fake Telegram for kind verification (verify-kind.sh).

A single-file Bot API upstream: one batch of updates on the first
getUpdates poll, empty after; sendMessage echoes a message_id. Deployed
as a ConfigMap by the script — no external deps, stdlib only.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BATCH = [
    {"update_id": 1001, "message": {"text": "first"}},
    {"update_id": 1002, "message": {"text": "second"}},
]
STATE = {"calls": 0, "lock": threading.Lock()}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = {}
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            body = json.loads(self.rfile.read(length))
        if self.path.endswith("/getUpdates"):
            with STATE["lock"]:
                STATE["calls"] += 1
                calls = STATE["calls"]
            offset = int(body.get("offset") or 0)
            pending = [u for u in BATCH if u["update_id"] >= offset] if calls == 1 else []
            out = {"ok": True, "result": pending}
        elif self.path.endswith("/sendMessage"):
            out = {"ok": True, "result": {"message_id": 42, "text": body.get("text")}}
        else:
            out = {"ok": True, "result": True}
        data = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 18090), Handler).serve_forever()
