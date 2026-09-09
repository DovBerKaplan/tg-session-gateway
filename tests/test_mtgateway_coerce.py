"""JSON → Pyrogram coercion: reply_markup and InputMedia round-trips."""

from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto

from mtgateway.coerce import coerce_args


class TestReplyMarkup:
    def test_inline_keyboard_becomes_object(self):
        args = coerce_args(
            {
                "chat_id": 10,
                "text": "hi",
                "reply_markup": {
                    "inline_keyboard": [
                        [{"text": "Watch", "url": "https://t.me/x"}],
                        [{"text": "Again", "callback_data": "again:1"}],
                    ]
                },
            }
        )
        rm = args["reply_markup"]
        assert isinstance(rm, InlineKeyboardMarkup)
        assert isinstance(rm.inline_keyboard[0][0], InlineKeyboardButton)
        assert rm.inline_keyboard[0][0].url == "https://t.me/x"
        assert rm.inline_keyboard[1][0].callback_data == "again:1"
        # untouched args pass through verbatim
        assert args["chat_id"] == 10 and args["text"] == "hi"

    def test_button_extra_fields_ignored(self):
        args = coerce_args({"reply_markup": {"inline_keyboard": [[{"text": "t", "junk": "x"}]]}})
        assert args["reply_markup"].inline_keyboard[0][0].text == "t"


class TestInputMedia:
    def test_media_photo(self):
        args = coerce_args(
            {
                "chat_id": 5,
                "message_id": 7,
                "media": {"type": "photo", "media": "https://x/y.jpg", "caption": "c"},
            }
        )
        m = args["media"]
        assert isinstance(m, InputMediaPhoto)
        assert m.media == "https://x/y.jpg" and m.caption == "c"

    def test_plain_media_string_passthrough(self):
        args = coerce_args({"chat_id": 5, "photo": "https://x/y.jpg"})
        assert args["photo"] == "https://x/y.jpg"
