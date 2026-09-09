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
    api=TelegramAPIServer.from_base("http://tg-gateway:8080/tgapi"))
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

## Invariants

Design rules the code enforces — with the tests that prove them
(0.0.x honesty: proven where a test is linked, aspirational elsewhere):

- **One owner per token.** The gateway runs the only `getUpdates`
  poller; a singleton file lock (PID + heartbeat) blocks a second
  sidecar process **before** it opens a connection to Telegram.
  *Tests: two-process lock refusal + stale reclaim
  (`tests/test_session_lock.py`), gateway-owns-getUpdates
  (`tests/test_proxy.py`).*
- **A hot chat cannot silence the bot.** Rate Guard buckets are
  per-chat; a real 429 or `FloodWait` pauses that chat only.
  *Tests: `test_chat_429_pauses_only_that_chat`,
  `test_other_chat_unaffected` (`tests/test_rateguard.py`).*
- **Updates survive kills.** The update queue is SQLite-WAL-persisted;
  unacked updates replay after a gateway restart, and consumer offsets
  resume.
  *Test: restart-with-unacked-updates
  (`tests/test_integration_gateway.py`).*
- **App deploys never touch Telegram.** The gateway/sidecar lives in
  its own Compose project; `docker compose down` on your app cannot
  kill the Telegram session.
- **Replica overlap loses nothing.** During a rolling deploy, old and
  new replicas can both poll (real Telegram returns 409 Conflict to
  the second poller; the gateway allows the overlap). Every update
  reaches at least one replica, and each replica's own stream is
  duplicate-free when it acks Bot API-style (`offset = seen + 1`).
  *Test: replica overlap (`tests/test_integration_gateway.py`).*
  NOTE: the offset contract is subtle — until v0.1, treat the queue
  ack path as fresh code and keep the tests green before upgrading.

**Status, plainly:** the Bot API gateway is a working prototype with
the tests above. The MTProto sidecar is earlier — the lock, rate guard
and HTTP façade are implemented and unit-tested, but it has not carried
production traffic. Do not put someone else's production on either
half yet; the sidecar least of all.

**Single-loop assumption.** The rate guards keep all state in-process
(single asyncio loop). Do not front the gateway with a multi-worker
gunicorn/uvicorn setup — the buckets would not be shared and the
limits would multiply. One process per gateway, always.

## Quickstart

Fastest path — from source with plain Compose (read
[docker-compose.yml](docker-compose.yml) first; it publishes on
loopback only):

```bash
git clone https://github.com/DovBerKaplan/tg-session-gateway && cd tg-session-gateway
docker network create tg-gateway-net
printf 'GW_ADMIN_SECRET=%s\nGW_APP_SECRET=%s\n' \
  "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" > .env
docker compose up -d          # builds + starts on 127.0.0.1:8080
curl -s localhost:8080/healthz
```

Pre-built multi-arch images (amd64/arm64), published on every version tag:

```bash
docker pull ghcr.io/dovberkaplan/tg-session-gateway:latest      # Bot API gateway
docker pull ghcr.io/dovberkaplan/tg-session-gateway-mtproto:latest  # MTProto sidecar
```

For a server install with the external network, dedicated volume and
generated secrets in one step: `sudo bash deploy/install.sh`
(read it before running — never curl-pipe-sudo).

MTProto sidecar:

```bash
GW_ADMIN_SECRET=x GW_APP_SECRET=y GW_API_ID=z GW_API_HASH=h \
  docker compose -f docker-compose.mtgateway.yml up -d
```

## When to use something else

| You need... | Use that instead | Why |
|---|---|---|
| Full local Bot API server (files, big uploads, every method) | [tdlibBotApiServer](https://github.com/tdlib/td/blob/master/README.md#telegram-bot-api) or the official [local Bot API server](https://core.telegram.org/bots/api#using-a-local-bot-api-server) | This gateway proxies, it doesn't re-implement the API server; no local file storage |
| Every MadelineProto/MTProto method over HTTP | [TelegramApiServer](https://github.com/xtrime-ru/TelegramApiServer), [docker-telethon-plus](https://github.com/psyb0t/docker-telethon-plus) | Broader method coverage, battle-tested; this sidecar is Façade-minimal (send + updates + files) |
| Session persistence for one in-process bot | A volume + Pyrogram file sessions | Less moving parts; you only need this repo when deploys/replicas share one session |
| Managed scaling for grammY bots | grammY runner / BotMux-style gateways | Ecosystem-native; this repo is library-agnostic HTTP |
| A user MTProto proxy | [mtproto proxy](https://core.telegram.org/mtproto/mtproto-proxy) implementations | Different problem; out of scope here |

The honest pitch: **Bot API FAQ buckets + synthetic 429 + Compose
isolation** for HTTP bots, and — if it matures — a Pyrogram-shaped
long-lived sidecar for the client you already have.

## Documentation

| Document | Description |
|---|---|
| [Architecture](docs/architecture.md) | How both products are built — planes, diagrams, failure model |
| [Migration Guide](docs/migration.md) | Move an existing aiogram/PTB/Pyrogram bot onto the gateway |
| [Comparison](docs/comparison.md) | vs direct polling, webhooks, in-process Pyrogram — and when NOT to use this |
| [Spec](docs/spec.md) | Bot API gateway specification |
| [Sidecar Spec](docs/sidecar-spec.md) | MTProto sidecar requirements |
| [Engineering Plan](docs/mtproto-gateway-plan.md) | MTProto sidecar implementation plan |
| [Roadmap](docs/roadmap-10-of-10.md) | Path to v1.0 |
| [Gap Analysis](docs/gap-analysis-report.md) | External review findings |
| [Compatibility](docs/compatibility.md) | Pyrogram / pyrofork / Telethon support matrix |

## License

MIT
