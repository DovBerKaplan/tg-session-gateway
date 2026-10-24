"""Record MTProto update goldens from REAL pyrofork raw TL objects.

Stage 2.0/2.3 of the revised plan: the natural update contract is
json.loads(str(raw_update)) — pyrogram's own serialization of raw TL
objects ("_" carries the "types."-prefixed constructor name, fields
are snake_case, defaults are omitted, bytes render as their repr).

The Python sidecar never actually delivered this shape (its
_serialize_update needs .to_dict, which pyrofork does not define —
real updates were dropped). The GO sidecar's SerializeUpdate
implements it, and these goldens pin that contract to the exact bytes
pyrofork itself would produce.

Regenerate: .venv/bin/python tests/conformance_go/record_golden_raw.py
(the venv carries pyrofork; see tests/conformance_go/README.md)
"""

from __future__ import annotations

import json
import os

from pyrogram.raw.types import (
    Message,
    MessageEntityBlockquote,
    MessageEntityBold,
    PeerChannel,
    PeerChat,
    PeerUser,
    UpdateBotCallbackQuery,
    UpdateEditMessage,
    UpdateNewChannelMessage,
    UpdateNewMessage,
    KeyboardButton,
    KeyboardButtonCallback,
    KeyboardButtonRow,
    KeyboardButtonUrl,
    ReplyInlineMarkup,
)

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DIR = os.path.join(REPO, "internal", "mtproto", "testdata", "golden")


def build_cases() -> dict[str, object]:
    private = UpdateNewMessage(
        message=Message(
            id=42,
            date=1700000000,
            message="hello world",
            peer_id=PeerUser(user_id=10),
            from_id=PeerUser(user_id=20),
            entities=[
                MessageEntityBold(offset=0, length=5),
                MessageEntityBlockquote(offset=6, length=5, collapsed=True),
            ],
        ),
        pts=100,
        pts_count=1,
    )
    group = UpdateNewMessage(
        message=Message(
            id=43,
            date=1700000001,
            message="in the group",
            peer_id=PeerChat(chat_id=77),
        ),
        pts=101,
        pts_count=1,
    )
    channel = UpdateNewChannelMessage(
        message=Message(
            id=44,
            date=1700000002,
            message="channel post",
            peer_id=PeerChannel(channel_id=1234),
        ),
        pts=102,
        pts_count=1,
    )
    edit = UpdateEditMessage(
        message=Message(
            id=42,
            date=1700000000,
            message="hello world (edited)",
            peer_id=PeerUser(user_id=10),
        ),
        pts=103,
        pts_count=0,
    )
    callback = UpdateBotCallbackQuery(
        query_id=555,
        user_id=20,
        peer=PeerUser(user_id=10),
        msg_id=7,
        chat_instance=-5500000000000000000,
        data=b"payload",  # NOTE: str() renders bytes as "b'payload'"
    )
    markup = UpdateNewMessage(
        message=Message(
            id=45,
            date=1700000003,
            message="pick one",
            peer_id=PeerUser(user_id=10),
            reply_markup=ReplyInlineMarkup(
                rows=[
                    KeyboardButtonRow(
                        buttons=[
                            KeyboardButton(text="Open"),
                            KeyboardButtonCallback(text="Go", data=b"go"),
                            KeyboardButtonUrl(text="Docs", url="https://example.com"),
                        ]
                    )
                ]
            ),
        ),
        pts=104,
        pts_count=1,
    )
    return {
        "new_message_private": private,
        "new_message_group": group,
        "new_message_channel": channel,
        "edit_message": edit,
        "callback_query": callback,
        "message_with_reply_markup": markup,
    }


def main() -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    for name, obj in build_cases().items():
        payload = json.loads(str(obj))
        path = os.path.join(OUT_DIR, f"{name}.json")
        with open(path, "w") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.write("\n")
        print(f"recorded {path}")


if __name__ == "__main__":
    main()
