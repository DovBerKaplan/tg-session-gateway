# TG Session Gateway

**Deploy your Telegram bot without dropping its session or tripping rate limits.**

Two products, one repo. Pick the one that matches your bot's protocol:

| Your bot uses... | Use | Directory |
|---|---|---|
| Bot API (aiogram, python-telegram-bot, grammY, Telegraf) | **Bot API Gateway** | `gateway/` |
| Pyrogram / pyrofork (MTProto) | **MTProto Sidecar** | `mtgateway/` |

Both share the same principles: a long-lived infrastructure container
that owns the Telegram connection, enforces the official
[Bot API FAQ rate limits](https://core.telegram.org/bots/faq), and lets
you deploy your application freely — no FloodWait, no Conflict, no
AUTH_KEY_DUPLICATED.

## Bot API Gateway

Drop-in `api.telegram.org` replacement for HTTP Bot API bots.

```python
# aiogram 3.x
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
session = AiohttpSession(
    api=TelegramAPIServer.from_base("http://tg-gateway:8080", is_local=True))
bot = Bot(token, session=session)

# python-telegram-bot
app = (Application.builder()
       .token(t)
       .base_url("http://tg-gateway:8080/tgapi/bot")
       .base_file_url("http://tg-gateway:8080/file/bot")
       .build())
```

That's the whole integration. The gateway speaks the Bot API verbatim.

[Full documentation →](docs/spec.md)

## MTProto Sidecar

For Pyrogram/pyrofork bots. The sidecar is the **sole MTProto client** —
it holds the auth_key, the TCP connection, and the update loop. Your
application talks HTTP/WebSocket to the sidecar; deploys and rolling
updates never touch Telegram.

```python
# Send a message through the sidecar
POST /v1/sessions/{alias}/send_message
  {"chat_id": 123, "text": "hello"}
  → 200 {"ok": true, "result": {...}}

# Receive updates (WebSocket push)
WS /v1/sessions/{alias}/updates

# Or pull (Bot API compatible)
POST /v1/sessions/{alias}/getUpdates
```

**What it solves:**
- **Auth FloodWait**: session file persists; `importBotAuthorization` runs once, never on deploy
- **Rolling updates**: only the sidecar holds the auth_key; app replicas connect/disconnect freely
- **Rate limits**: FAQ buckets (1/s private, 20/min group, 30/s global) + real FLOOD_WAIT parsing with per-peer adaptive backoff

[Engineering plan →](docs/mtproto-gateway-plan.md)

## Guarantees (both products)

- **Two lifecycles.** The gateway/sidecar never lives in your Compose file. `docker compose down` on your app cannot touch it.
- **One owner per token/session.** No two pollers, no two sockets on one auth_key. A singleton file lock prevents AUTH_KEY_DUPLICATED at the process level.
- **Rate Guard.** FAQ-exact token buckets. Real 429s from Telegram ≈ 0 during normal operation. One hot chat cannot silence the bot for everyone.
- **Update queue.** Survives app restarts. Bounded, with drop-oldest or reject policies.

## Quickstart

```bash
# Bot API gateway
sudo bash deploy/install.sh

# MTProto sidecar
GW_ADMIN_SECRET=x GW_APP_SECRET=y GW_API_ID=z GW_API_HASH=h \
  docker compose -f docker-compose.mtgateway.yml up -d
```

## Documentation

| Document | Description |
|---|---|
| [Spec](docs/spec.md) | Bot API gateway specification |
| [Sidecar Spec](docs/sidecar-spec.md) | MTProto sidecar requirements |
| [Engineering Plan](docs/mtproto-gateway-plan.md) | MTProto sidecar implementation plan |
| [Roadmap](docs/roadmap-10-of-10.md) | Path to v1.0 |
| [Gap Analysis](docs/gap-analysis-report.md) | External review findings |

## License

MIT
