"""Record MTProto update golden files from the REAL Python sidecar.

Boots mtgateway's actual FastAPI app with the integration test's
FakeClient (only Telegram is faked), fires one Pyrogram-shaped update
dict per case through the raw handler — exactly what
tests/test_integration_mtgateway.py does — and saves what the consumer
receives via getUpdates (dynamic _gw_* keys stripped).

The corpus uses the classic Pyrogram to_dict shapes (constructor name
in "_", snake_case params), which is the contract SerializeUpdate
emulates in Go.

Regenerate with:
    .venv/bin/python tests/conformance_go/record_golden_mt.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

import httpx

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "tests"))

from test_integration_mtgateway import FakeClient, make_cfg

import mtgateway.sessions as mt_sessions
from mtgateway.app import create_app

OUT_DIR = os.path.join(REPO, "go", "internal", "mtproto", "testdata", "golden")

# ── the corpus: Pyrogram-shaped update dicts ───────────────────────
# (classic pyrogram to_dict output — "_" carries the constructor name)

GOLDEN_CASES = {
    "new_message_private": {
        "_": "UpdateNewMessage",
        "update_id": 9001,
        "message": {
            "_": "Message",
            "id": 42,
            "date": 1700000000,
            "message": "hello world",
            "out": False,
            "peer_id": {"_": "PeerUser", "user_id": 10},
            "from_id": {"_": "PeerUser", "user_id": 20},
            "reply_to": None,
            "entities": [
                {"_": "MessageEntityBold", "offset": 0, "length": 5},
                {"_": "MessageEntityBlockquote", "offset": 6, "length": 5, "collapsed": True},
            ],
            "media": None,
            "reply_markup": None,
        },
        "pts": 100,
        "pts_count": 1,
    },
    "new_message_group": {
        "_": "UpdateNewMessage",
        "update_id": 9002,
        "message": {
            "_": "Message",
            "id": 43,
            "date": 1700000001,
            "message": "in the group",
            "peer_id": {"_": "PeerChat", "chat_id": 77},
            "entities": [],
        },
        "pts": 101,
        "pts_count": 1,
    },
    "new_message_channel": {
        "_": "UpdateNewChannelMessage",
        "update_id": 9003,
        "message": {
            "_": "Message",
            "id": 44,
            "date": 1700000002,
            "message": "channel post",
            "peer_id": {"_": "PeerChannel", "channel_id": 1234},
            "entities": [],
        },
        "pts": 102,
        "pts_count": 1,
    },
    "edit_message": {
        "_": "UpdateEditMessage",
        "update_id": 9004,
        "message": {
            "_": "Message",
            "id": 42,
            "date": 1700000000,
            "message": "hello world (edited)",
            "peer_id": {"_": "PeerUser", "user_id": 10},
            "entities": [],
        },
        "pts": 103,
        "pts_count": 0,
    },
    "callback_query": {
        "_": "UpdateBotCallbackQuery",
        "update_id": 9005,
        "query_id": 555,
        "user_id": 20,
        "peer": {"_": "PeerUser", "user_id": 10},
        "msg_id": 7,
        "chat_instance": -5500000000000000000,
        # NOTE: real pyrogram hands bytes here and Python's json.dumps
        # would dead-letter the update; the pass-through dict path (and
        # the Go serializer) carries str data — pinned as such.
        "data": "payload",
    },
    "message_with_reply_markup": {
        "_": "UpdateNewMessage",
        "update_id": 9006,
        "message": {
            "_": "Message",
            "id": 45,
            "date": 1700000003,
            "message": "pick one",
            "peer_id": {"_": "PeerUser", "user_id": 10},
            "reply_markup": {
                "_": "ReplyInlineMarkup",
                "rows": [
                    {
                        "_": "KeyboardButtonRow",
                        "buttons": [
                            {"_": "KeyboardButton", "text": "Open"},
                        ],
                    },
                ],
            },
        },
        "pts": 104,
        "pts_count": 1,
    },
}

DYNAMIC_KEYS = {"_gw_seq", "_gw_enqueued_at"}


def strip_dynamic(obj):
    if isinstance(obj, dict):
        return {k: strip_dynamic(v) for k, v in obj.items() if k not in DYNAMIC_KEYS}
    if isinstance(obj, list):
        return [strip_dynamic(v) for v in obj]
    return obj


async def record() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    FakeClient.instances = []
    FakeClient.start_calls = 0
    saved = mt_sessions.PyrogramClient
    mt_sessions.PyrogramClient = FakeClient
    try:
        cfg = make_cfg("/tmp/tggw-golden-recording/data")
        app = create_app(cfg)
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://mt") as c,
        ):
            await c.post(
                "/v1/admin/sessions",
                json={"alias": "gold", "token": "7:g"},
                headers={"authorization": "Bearer mt-adm"},
            )
            for _ in range(200):
                if FakeClient.start_calls == 1:
                    break
                await asyncio.sleep(0.02)
            fake = FakeClient.instances[-1]

            recorded: list[tuple[str, dict]] = []
            for name, payload in GOLDEN_CASES.items():
                await fake.fire_update(payload)
                r = await c.post(
                    "/v1/sessions/gold/getUpdates",
                    json={"timeout": 0, "limit": 10},
                    headers={"authorization": "Bearer mt-app"},
                )
                results = r.json().get("result") or []
                match = None
                for u in results:
                    if u.get("_") == payload["_"] and u.get("update_id") == payload["update_id"]:
                        match = u
                        break
                if match is None:
                    raise SystemExit(
                        f"case {name}: Python sidecar did not deliver the update "
                        f"(got {json.dumps(results)[:300]})"
                    )
                out = strip_dynamic(match)
                recorded.append((name, out))
            return recorded
    finally:
        mt_sessions.PyrogramClient = saved
    return recorded


def main() -> None:
    for name, out in asyncio.run(record()):
        path = os.path.join(OUT_DIR, f"{name}.json")
        with open(path, "w") as fh:
            json.dump(out, fh, indent=2, sort_keys=True)
            fh.write("\n")
        print(f"recorded {path}")


if __name__ == "__main__":
    main()
