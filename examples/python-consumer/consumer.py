"""Python consumer example: raw HTTP against the gateway — any Bot API
library works by swapping its base URL, but this example shows the
actual wire contract (Bearer app secret, envelope, offset ack).

Env:
    TGGW_BASE       gateway base URL     (default http://127.0.0.1:8080)
    TGGW_APP_SECRET GW_APP_SECRET value  (required)
    BOT_TOKEN       the token registered in the gateway (required)
"""

from __future__ import annotations

import os
import time

import httpx

base = os.environ.get("TGGW_BASE", "http://127.0.0.1:8080").rstrip("/")
secret = os.environ["TGGW_APP_SECRET"]
token = os.environ["BOT_TOKEN"]
headers = {"Authorization": f"Bearer {secret}"}
offset = 0


def get_updates(client: httpx.Client) -> list[dict]:
    global offset
    body = {"timeout": 25, "limit": 100}
    if offset:
        body["offset"] = offset + 1
    r = client.post(f"{base}/tgapi/bot{token}/getUpdates",
                    json=body, headers=headers, timeout=30)
    r.raise_for_status()
    updates = r.json()["result"]
    for u in updates:
        offset = max(offset, u["update_id"])
    return updates


def send(client: httpx.Client, chat_id: int, text: str) -> None:
    r = client.post(f"{base}/tgapi/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": text},
                    headers={**headers, "Idempotency-Key": f"echo-{chat_id}-{text}"},
                    timeout=15)
    if r.status_code == 429:
        retry = r.json().get("parameters", {}).get("retry_after", 1)
        time.sleep(retry)


def main() -> None:
    with httpx.Client() as client:
        while True:
            try:
                for u in get_updates(client):
                    msg = u.get("message") or {}
                    chat = (msg.get("chat") or {}).get("id")
                    if chat and msg.get("text"):
                        send(client, chat, f"echo: {msg['text']}")
            except httpx.HTTPError as e:
                print(f"pull failed: {e}")
                time.sleep(1)


if __name__ == "__main__":
    main()
