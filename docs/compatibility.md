# Compatibility

Current for **0.1.0** (Go). The Python gateway (<= 0.0.5) lives on the
`legacy-python` branch; its images remain on GHCR for rollback.

## Bot API plane

| Consumer library | Compatible? | Note |
|---|---|---|
| aiogram (2.x / 3.x) | yes | `TelegramAPIServer.from_base("http://tggw:8080/tgapi")` |
| python-telegram-bot | yes | `base_url="http://tggw:8080/tgapi/bot"` |
| grammY / Telegraf | yes | point `client` / `apiRoot` at the gateway |
| Raw HTTP (any language) | yes | the contract is plain Bot API + Bearer secret; see [api-reference.md](api-reference.md) |
| Local Bot API server-only features (files on disk) | no | the gateway proxies, it does not re-implement the API server |

Parse modes: `sendMessage` without an explicit `parse_mode` runs
pyrogram's combined markdown+HTML parser (the sidecar's `pyformat`
port) — same as the Python sidecar; explicit `html` / `markdown` /
`disabled` behave like Pyrogram's enums.

urlencoded form bodies forward VERBATIM (a deliberate fix over the
Python plane, which coerced `"007"` to 7 and `"true"` to boolean).

## MTProto sidecar plane

Pyrogram-shaped serialization (goldens recorded from real pyrofork
raw TL objects; `json.loads(str(raw))` contract). 19-method allowlist
with 501 + list for anything else — see
[sidecar-spec.md](sidecar-spec.md) section 10. Telethon-compatible:
NOT supported (official statement there).

## Session files

pyrofork/Pyrogram `.session` files convert in place with
`tggw migrate` (no new Telegram login). gotd session blobs are sealed
at rest with the volume fernet key since 0.1.0.
