# Compatibility

Current for **0.1.0** (Go). The Python gateway (<= 0.0.5) lives on the
`legacy-python` branch; its images remain on GHCR for rollback.

## Bot API plane

| Consumer library | Compatible? | Note |
|---|---|---|
| aiogram (2.x / 3.x) | yes | see [per-library setup](#per-library-setup) below |
| python-telegram-bot | yes | see [per-library setup](#per-library-setup) below |
| grammY / Telegraf | yes | see [per-library setup](#per-library-setup) below |
| go-telegram/bot, telebot, teloxide | yes | see [per-library setup](#per-library-setup) below |
| Raw HTTP (any language) | yes | the contract is plain Bot API + Bearer secret; see [api-reference.md](api-reference.md) |
| Local Bot API companion (big files, local upstream) | yes (opt-in) | `docker compose --profile local-botapi`; clients keep one gateway URL; writes/reads pass through the gateway **Rate Guard**; companion is internal-only ([runbook](runbook.md#auto-environment-detection)) |
| Big uploads/downloads (2000 MB class) | yes (opt-in companion) | gateway raises multipart/time limits when upstream kind is `local`; cloud path stays the default |
| Absolute `file_path` from `--local` | yes (rewritten) | `getFile` disk paths become gateway URLs under `/file/bot<token>/…` — your bot container does **not** need the companion volume |
| Companion upstream rate limits | gateway Rate Guard | FAQ buckets and 429 isolation apply on the way in/out; the tdlib server is not your public API surface |

### Per-library setup

Point every library at the **gateway**, not at `api.telegram.org` or the
companion. Default path prefix is `GW_PATH_PREFIX=/tgapi` (unset in env
also defaults to `/tgapi`). Set `GW_PATH_PREFIX=none` for bare routes
(`/bot<token>/…` and `/file/bot<token>/…` at the gateway root).

Replace `http://tggw:8080` with your host/port. File downloads use the
same prefix, e.g. `http://tggw:8080/tgapi/file/bot<token>/…`.

**aiogram 3.x**

```python
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
session = AiohttpSession(api=TelegramAPIServer.from_base("http://tggw:8080/tgapi"))
```

**python-telegram-bot v20+**

```python
app = (Application.builder()
       .token(token)
       .base_url("http://tggw:8080/tgapi/bot")
       .base_file_url("http://tggw:8080/tgapi/file/bot")
       .build())
```

**grammY**

```ts
import { Bot } from "grammy";
const bot = new Bot(token, {
  client: { apiRoot: "http://tggw:8080/tgapi" },
});
```

**Telegraf**

```js
import { Telegraf } from "telegraf";
const bot = new Telegraf(token, { apiRoot: "http://tggw:8080/tgapi" });
```

**go-telegram/bot**

```go
import "github.com/go-telegram/bot"
b, _ := bot.New(token, bot.WithServerURL("http://tggw:8080/tgapi"))
```

**telebot (gopkg.in/telebot.v3)**

```go
pref := telebot.Settings{
  Token: token,
  URL:   "http://tggw:8080/tgapi",
}
```

**teloxide**

```rust
let url = url::Url::parse("http://tggw:8080/tgapi/").unwrap();
let bot = teloxide::Bot::with_url(token, url);
```

**curl**

```bash
curl -s "http://tggw:8080/tgapi/bot${TOKEN}/getMe"
curl -s "http://tggw:8080/tgapi/file/bot${TOKEN}/photos/file_1.jpg"
```

**Webhooks vs polling:** the gateway owns `getUpdates` for each
registered token. `getWebhookInfo` returns an empty `url` (libraries
that check webhook state on boot stay happy). `setWebhook` is rejected —
use gateway→consumer delivery via `POST /admin/bots/{alias}/push`
instead ([spec](spec.md)).

Parse modes: `sendMessage` without an explicit `parse_mode` runs
pyrogram's combined markdown+HTML parser (the sidecar's `pyformat`
port) — same as the Python sidecar; explicit `html` / `markdown` /
`disabled` behave like Pyrogram's enums.

urlencoded form bodies forward VERBATIM (a deliberate fix over the
Python plane, which coerced `"007"` to 7 and `"true"` to boolean).

## MTProto sidecar plane

Pyrogram-shaped serialization (goldens recorded from real pyrofork
raw TL objects; `json.loads(str(raw))` contract). 19 core methods +
15 scanner methods (history/dialogs/participants, channel admin,
document-by-reference sends) with 501 + list for anything else — see
[sidecar-spec.md](sidecar-spec.md) section 10. Telethon StringSessions
import (0.2) via the admin API; running the Telethon library itself
stays out of scope.

## Session files

pyrofork/Pyrogram `.session` files convert in place with
`tggw migrate` (no new Telegram login). Telethon StringSessions import
via `POST /v1/admin/sessions/{alias}/import` (0.2). gotd session blobs
are sealed at rest with the volume fernet key since 0.1.0.
