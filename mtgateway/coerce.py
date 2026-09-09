"""JSON → Pyrogram object coercion for method arguments.

Consumers speak JSON over HTTP; Pyrogram methods want typed objects.
The shapes here mirror what a serialized Pyrogram object looks like,
so a reply_markup or InputMedia that was serialized at the consumer
round-trips into a real object before the call.
"""

from __future__ import annotations

from typing import Any

from pyrogram.types import (
    ForceReply,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
)

_MEDIA_TYPES = {
    "photo": InputMediaPhoto,
    "video": InputMediaVideo,
    "document": InputMediaDocument,
}


def coerce_args(kwargs: dict[str, Any]) -> dict[str, Any]:
    """Coerce every argument that has a Pyrogram object equivalent."""
    return {k: _coerce(v) for k, v in kwargs.items()}


def _coerce(value: Any) -> Any:
    if isinstance(value, list):
        return [_coerce(v) for v in value]
    if not isinstance(value, dict):
        return value

    if "inline_keyboard" in value:
        rows = [[_button(b) for b in row] for row in value["inline_keyboard"]]
        return InlineKeyboardMarkup(inline_keyboard=rows)
    if "type" in value and value.get("type") in _MEDIA_TYPES and "media" in value:
        cls = _MEDIA_TYPES[value["type"]]
        return cls(**{k: v for k, v in value.items() if k not in ("type",)})
    if "keyboard" in value:
        return ReplyKeyboardMarkup(
            keyboard=value["keyboard"],
            resize_keyboard=value.get("resize_keyboard", True),
            is_persistent=value.get("is_persistent", False),
        )
    if value.get("force_reply"):
        return ForceReply()
    if value.get("remove_keyboard"):
        return ReplyKeyboardRemove()
    return value


def _button(b: Any) -> InlineKeyboardButton:
    if isinstance(b, InlineKeyboardButton):
        return b
    allowed = [
        "text",
        "url",
        "callback_data",
        "web_app",
        "switch_inline_query",
        "switch_inline_query_current_chat",
        "login_url",
    ]
    return InlineKeyboardButton(**{k: v for k, v in b.items() if k in allowed and v is not None})
