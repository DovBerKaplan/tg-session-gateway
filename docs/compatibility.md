# Library Compatibility

## MTProto Sidecar (`mtgateway/`)

| Library | Version Tested | Status | Notes |
|---|---|---|---|
| Pyrogram | 2.0.106 | Tested | Primary target; used in CI |
| pyrofork | latest | Assumed compatible | Fork of Pyrogram; same Client API. Add to CI when stable version pinned |
| Telethon | — | Not supported | Different session format, different Client construction. Planned for V2 |

### Pyrogram

The sidecar creates `pyrogram.Client` instances internally. Your application
never imports Pyrogram — it talks HTTP/WS to the sidecar. Any Pyrogram
version that the sidecar supports is sufficient (tested with 2.0.106).

### pyrofork

pyrofork is a maintained fork of Pyrogram with the same `Client` API. The
sidecar should work with pyrofork by changing the import in
`mtgateway/requirements.txt` from `pyrogram` to `pyrofork`. This is
**assumed but not tested** — add pyrofork to CI to verify.

### Telethon (V2 goal)

Telethon uses a different session format (SQLite-based with its own schema),
a different event system (not Pyrogram's handler groups), and a different
connection model. Supporting Telethon requires:
- A separate client adapter (TelethonClient instead of PyrogramClient)
- Session file conversion (Telethon .session ↔ Pyrogram .session)
- Or: a separate Telethon sidecar instance

This is planned for V2 but is not blocking V1.

## Bot API Gateway (`gateway/`)

| Library | Version Tested | Status | Integration |
|---|---|---|---|
| aiogram | 3.x | Tested | `AiohttpSession(api=TelegramAPIServer.from_base(...))` |
| python-telegram-bot | 20+ | Tested | `.base_url(".../tgapi/bot")` |
| grammY | latest | Theoretical | `apiRoot` config |
| Telegraf | latest | Theoretical | `telegram.options.apiRoot` |
| Raw HTTP | — | Tested | `POST /tgapi/bot<token>/<method>` |

The Bot API gateway is protocol-agnostic — it proxies HTTP to
`api.telegram.org`. Any library or raw HTTP client that lets you override
the base URL works.

## Known Issues

| Issue | Affects | Status |
|---|---|---|
| tgcrypto C extension fails on Alpine/slim | mtgateway Docker build | Optional dependency; pyrogram falls back to pyaes |
| Telethon session format incompatible | mtgateway | Not supported in V1 |
| pyrofork with TgCrypto | mtgateway | Not tested in CI |
