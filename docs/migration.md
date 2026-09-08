# Migration Guide

Moving an existing bot onto the gateway with minimal code change.
The Bot API gateway is designed so the receive path needs **one URL
change**; the MTProto sidecar needs its send/receive calls rewritten
to HTTP.

## A. Bot API gateway (aiogram / PTB / Telegraf / grammY)

### 1. Run the gateway

```bash
docker run -d --name tggw \
  -v tgw-data:/data \
  -e GW_ADMIN_SECRET=$(openssl rand -hex 16) \
  -e GW_APP_SECRET=$(openssl rand -hex 16) \
  -p 8080:8080 \
  ghcr.io/dovberkaplan/tg-session-gateway:latest
```

### 2. Register your bot (once)

```bash
curl -X POST http://localhost:8080/admin/bots \
  -H "Authorization: Bearer $GW_ADMIN_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"token": "12345:ABC", "alias": "mybot"}'
```

The gateway immediately becomes the single poller for that token.

### 3. Point your library at the gateway

Every Bot API library has a way to change the API base URL. The
gateway exposes the exact Bot API surface at `/tgapi`:

**aiogram 3:**
```python
from aiogram.client.session.aiohttp import AiohttpSession
session = AiohttpSession(api="http://tggw:8080/tgapi")  # note: no /bot<token> suffix
bot = Bot("12345:ABC", session=session)
```

**python-telegram-bot v20+:**
```python
from telegram import Bot
from telegram.request import HTTPXRequest
request = HTTPXRequest(base_url="http://tggw:8080/tgapi/bot")
bot = Bot("12345:ABC", request=request)
```

**Telegraf (Node):**
```js
new Telegraf(token, { apiRoot: "http://tggw:8080/tgapi" });
```

Your existing `getUpdates`-based polling now reads from the gateway's
queue. Nothing else changes: same handlers, same send calls (they are
proxied upstream through the Rate Guard).

### 4. (Recommended) switch to the alias route

The `/tgapi/bot{token}` surface keeps your token flowing through your
app. For harder security, use the alias surface instead — your app
never sees the token again:

```
POST http://tggw:8080/bots/mybot/sendMessage
Authorization: Bearer $GW_APP_SECRET
{"chat_id": 10, "text": "hi"}
```

Libraries that let you pass per-request headers/URL templates can use
this directly; otherwise keep `/tgapi` — it is fully supported.

### 5. Deploys from now on

`docker compose down && up` your app freely. The poller, the queue and
the offset live in the gateway. Unacked updates survive even a
**gateway** restart (SQLite WAL under `/data`).

### Rollback

Point the library's base URL back at `api.telegram.org` and your app
polls directly again. No data format changes; the offset continues
from where Telegram last saw your token.

## B. MTProto sidecar (Pyrogram)

There is no URL swap for MTProto — the app must call the sidecar's
HTTP API instead of a local Pyrogram client.

### 1. Run the sidecar

```bash
GW_ADMIN_SECRET=… GW_APP_SECRET=… GW_API_ID=… GW_API_HASH=… \
docker compose -f docker-compose.mtgateway.yml up -d
```

### 2. Register the session (once — creates the session file)

```bash
curl -X POST http://localhost:8081/v1/admin/sessions \
  -H "Authorization: Bearer $GW_ADMIN_SECRET" \
  -d '{"alias": "mybot", "token": "12345:ABC"}'
```

The sidecar acquires the singleton lock, starts Pyrogram once, and
owns the auth_key from here on.

### 3. Replace client calls with HTTP

| Pyrogram in your app | Sidecar HTTP |
|---|---|
| `await client.send_message(chat_id, text)` | `POST /v1/sessions/mybot/sendMessage` `{"chat_id": …, "text": …}` |
| `@client.on(messages)` handlers | `GET /v1/sessions/mybot/updates` (long-poll) or WS connect |
| `await client.download_media(file_id, in_memory=True)` | `GET /v1/sessions/mybot/files/{file_id}` |

Updates arrive JSON-serialized, one object per update, with
monotonic ids you can use as a resume cursor.

### Idempotency (do this during migration)

Sends through the sidecar accept an `_idempotency_key` **body field**.
If your deploy kills the app mid-send and a retry lands, the sidecar
deduplicates — the message goes out once. Add a stable key per logical
send (e.g. `f"{user_id}:{intent}:{ts_bucket}"`):

```json
POST /v1/sessions/mybot/sendMessage
{"_idempotency_key": "u42:welcome:1", "chat_id": 42, "text": "hi"}
```

### Rollback

Stop the sidecar's container **last**. As long as it's down, nothing
holds the session; your app can go back to opening the `.session` file
directly. Never run the sidecar and an in-process Pyrogram client on
the same session simultaneously — that's the exact
`AUTH_KEY_DUPLICATED` scenario the lock exists to prevent (the lock
will also refuse the second one, but don't rely on it across
machines).

## C. kot-style deployment (reference)

The reference consumer is a multi-bot production deployment (private
bot + group modbot):

- gateway/sidecar container on the same Docker network as the app,
  `/data` on a named volume, backed up regularly (it holds the
  encrypted tokens, offsets and queue)
- each bot registered under its own alias; the app only ever holds
  `GW_APP_SECRET`, never the bot tokens
- observability: both products export Prometheus metrics at `/metrics`
  — the sidecar adds `tg_gateway_real_flood_wait_total`,
  `tg_gateway_deploy_to_first_message_seconds` and
  `tg_gateway_send_paused`; the Bot API gateway exports
  `tg_gateway_real_429_total`, `tg_gateway_synthetic_429_total` and
  queue depth/drop counters. Alert on any real-limit counter > 0 (the
  preventive buckets should keep them at zero) and on queue depth
  growth.
