"""Dumps pyformat test expectations from the REAL pyrogram parser.

Runs the installed pyrofork Parser (client=None) over a corpus in all
three modes and writes UTF-16-adjusted (text, entities) to
go/internal/mtproto/pyformat/testdata/cases.json.

Regenerate: .venv/bin/python tests/conformance_go/record_pyformat_cases.py
"""

from __future__ import annotations

import json
import os
import sys

from pyrogram.enums import ParseMode
from pyrogram.parser import Parser

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT = os.path.join(REPO, "internal", "mtproto", "pyformat", "testdata", "cases.json")

CORPUS = [
    # plain
    "hello world",
    # bold / italic / underline / strike / spoiler (toggle pairs)
    "a **bold** b __italic__ c --underline-- d ~~strike~~ e ||spoiler|| f",
    # code and pre with language
    "inline `code` here",
    "```python\nprint('hi')\n```",
    "```\nnaked pre\n```",
    # nesting: bold containing italic
    "**outer __inner__ tail**",
    # links + emoji
    "see [the docs](https://example.com/a?b=1) now",
    "![party](tg://emoji?id=5222424)",
    # html tags in DEFAULT mode are ACTIVE (combined)
    "mix <b>html bold</b> and **md bold**",
    "<blockquote>quoted</blockquote> after",
    "<blockquote expandable>collapsible</blockquote> after",
    "<code>a**b**</code> stars inside code stay literal",
    # blockquote markdown lines
    "> quoted line one\n> quoted line two\nplain line",
    "**> expandable quote\nstill quote\ndone",
    # mention links
    "ping <a href=\"tg://user?id=123\">him</a> please",
    # unmatched/edge
    "unbalanced **bold",
    "escape ]( test",
    "unicode: Café 中文 𠀀 astral",
    # NOTE: "![x](https://non-tg)" is deliberately NOT in the corpus:
    # pyrogram's own parser CRASHES on it (int('https://...') ValueError
    # in the emoji branch) — the Go port degrades gracefully instead of
    # replicating the crash.
]

MODES = {
    "combined": ParseMode.DEFAULT,
    "markdown": ParseMode.MARKDOWN,
    "html": ParseMode.HTML,
}


def utf16(s: str) -> int:
    return len(s.encode("utf-16-le")) // 2


def to_utf16_entities(text: str, entities: list) -> list[dict]:
    out = []
    for e in entities or []:
        # raw TL objects use __slots__: read attributes directly
        off, ln = e.offset, e.length
        ent = {
            "type": type(e).__name__,
            "offset": utf16(text[:off]),
            "length": utf16(text[off: off + ln]),
        }
        for k in ("url", "language", "collapsed", "document_id", "user_id"):
            v = getattr(e, k, None)
            if v is not None:
                ent[k] = v
        out.append(ent)
    return sorted(out, key=lambda x: (x["offset"], x["length"], x["type"]))


async def main() -> None:
    parser = Parser(None)
    cases = []
    for text in CORPUS:
        for mode_name, mode in MODES.items():
            parsed = await parser.parse(text, mode)
            msg = parsed["message"]
            cases.append({
                "input": text,
                "mode": mode_name,
                "message": msg,
                "entities": to_utf16_entities(msg, parsed["entities"] or []),
            })
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as fh:
        json.dump(cases, fh, indent=1, sort_keys=True)
        fh.write(chr(10))
    print(f"recorded {len(cases)} cases → {OUT}")


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
