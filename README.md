# Telegram Session Gateway

> **Your bot container should be disposable. Your Telegram session shouldn't be.**

**1 container · 1 URL change · zero deployment downtime.**

Self-hosted Bot API gateway with session persistence, rate limiting,
FloodWait backoff, and a crash-safe update queue.

```
   deploy / crash / scale — anytime            ┌──────────────┐
  ┌─────────┐   ┌─────────┐   ┌─────────┐      │   Telegram   │
  │ bot v1  │ → │ bot v2  │ → │ bot v3  │      └──────▲───────┘
  └────┬────┘   └────┬────┘   └────┬────┘             │ one session,
       └─────────────┴─────────────┴────►  ┌──────────┴─────────┐
                HTTP (Bot API, verbatim)    │  session gateway   │
                                           │  · owns the poller │
                                           │  · rate guard      │
                                           │  · durable queue   │
                                           └────────────────────┘
```

**Start in 30 seconds** (Bot API bots — aiogram / python-telegram-bot
/ grammY / Telegraf):

```bash
docker run -d --name tggw -p 8080:8080 \
  -e GW_ADMIN_SECRET=$(openssl rand -hex 16) -e GW_APP_SECRET=$(openssl rand -hex 16) \
  -v tggw-data:/data ghcr.io/dovberkaplan/tg-session-gateway:latest
# then the ONLY change in your bot:
#   TelegramAPIServer.from_base("http://tggw:8080/tgapi")
```

*(images pending public visibility — until then `git clone` +
`docker compose up -d` works from source, see Quickstart below)*

| | Direct Bot API | Local Bot API server | MTProxy | **Session Gateway** |
|---|---|---|---|---|
| Session lifecycle independent of app deploys | ❌ | partial | ❌ | ✅ |
| Zero-downtime bot deploys | ❌ | ❌ | ❌ | ✅ |
| Built-in rate guard + 429 isolation per chat | ❌ | ❌ | — | ✅ |
| Update queue that survives kill -9 | ❌ | ❌ | — | ✅ |

## The problem

Every Telegram bot in Docker eventually hits this:

```
docker compose up -d          # deploy v2
        ↓
bot container replaced        # the process that held the session is GONE
        ↓
Telegram session dies         # disconnect → re-auth → AUTH flood risk
        ↓
FloodWait / downtime          # "A wait of 3342 seconds is required"
```

The session and the app live in the same process — so every deploy
restarts Telegram too. Multi-worker setups make it worse: two
processes open the same session → `AUTH_KEY_DUPLICATED` → the
auth_key can be invalidated **permanently**.

## The fix: separate the lifecycles

```
        BEFORE                          AFTER
┌─────────────────┐           ┌──────────────────────┐
│  Bot container  │           │   Bot container(s)   │
│  ├ session      │           │  └ business logic    │
│  ├ update loop  │           └─────────┬────────────┘
│  ├ rate limits  │                     │ HTTP / WS
│  └ your code    │           ┌─────────▼────────────┐
└─────────────────┘           │  Telegram Session     │
                              │  Gateway              │
  deploy = all dies           │  ├ session (persisted)│
                              │  ├ update loop        │
                              │  ├ rate guard         │
                              │  └ durable queue      │
                              └─────────┬────────────┘
                                        │
                                   Telegram
                          deploy the bot — Telegram never notices
```

## Proof (real output, not a mockup)

Four live bot sessions. The gateway container is killed and restarted —
the deploy scenario. Count the Telegram logins after the restart:

```
$ curl .../v1/admin/status          # BEFORE
  private → live   movies → live   raw → live   trending → live

$ docker restart tg-mtgateway       # "deploy"

$ docker logs tg-mtgateway
  MTProto gateway up — restored 4 session(s)
  [private]  live as @...  (id=8473414404)
  [movies]   live as @...  (id=8999546714)
  [raw]      live as @...  (id=8890815097)
  [trending] live as @...  (id=8443277244)

  Telegram logins performed: 0        # ← sessions restored from disk
```

Sessions persist to disk (SQLite WAL + session files), unacked updates
survive `kill -9`, and a singleton lock makes a second process fail
**before** it ever reaches Telegram. A chaos test in CI proves all
three: zero duplicates, zero lost updates, exactly-once replay.

## Also: Pyrogram / MTProto bots

Everything above is the **Bot API gateway** (`gateway/`) — the drop-in
`api.telegram.org` replacement, one base-URL change. If your bot runs
on **Pyrogram/pyrofork (MTProto)**, the repo also ships an MTProto
sidecar (`mtgateway/`): your app speaks HTTP/WebSocket, the sidecar
owns the auth_key, connection, and update loop. Same principles, same
rate guard — earlier in its lifecycle (one production consumer so
far), so treat it as the younger sibling: [sidecar docs →](docs/sidecar-spec.md)

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
the tests above. The MTProto sidecar is newer but has carried real
traffic: four live sessions against production Telegram (sends, edits,
inline keyboards), session-restore across restarts verified, and a
rolling-deploy test with zero re-auths and zero FloodWaits — in test
mode, for one consumer. Not yet proven at scale or with strangers'
production; treat it as exactly that.

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

**Then live the aha moment** (any bot token from @BotFather, ~2 min):

```bash
# 1. register the bot — the gateway becomes its single Telegram owner
curl -X POST localhost:8080/admin/bots -H "Authorization: Bearer $GW_ADMIN_SECRET" \
     -H 'Content-Type: application/json' -d '{"alias":"mybot","token":"<TOKEN>"}'

# 2. point your bot's base URL at the gateway (aiogram example)
#    AiohttpSession(api=TelegramAPIServer.from_base("http://<host>:8080/tgapi"))

# 3. deploy "new versions" all you like, then prove it:
docker restart <your-bot-container>      # bot restarts...
curl -s -H "Authorization: Bearer $GW_ADMIN_SECRET" localhost:8080/admin/status
# mybot → live, session never blinked, no re-auth, queue intact
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
