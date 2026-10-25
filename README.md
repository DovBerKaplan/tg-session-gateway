# Telegram Session Gateway — `tggw`

[![CI](https://github.com/DovBerKaplan/tg-session-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/DovBerKaplan/tg-session-gateway/actions/workflows/ci.yml)
[![Release](https://github.com/DovBerKaplan/tg-session-gateway/actions/workflows/release.yml/badge.svg)](https://github.com/DovBerKaplan/tg-session-gateway/actions/workflows/release.yml)
[![Latest release](https://img.shields.io/github/v/release/DovBerKaplan/tg-session-gateway)](https://github.com/DovBerKaplan/tg-session-gateway/releases)
[![Docker pulls](https://img.shields.io/docker/pulls/dovberkaplan/tggw)](https://hub.docker.com/r/dovberkaplan/tggw)
[![Go version](https://img.shields.io/badge/go-1.25-00ADD8?logo=go&logoColor=white)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

> **Your bot container should be disposable. Your Telegram session shouldn't be.**

**1 container · 1 URL change · zero deployment downtime.**

Self-hosted Bot API gateway with session persistence, rate limiting,
FloodWait backoff, and a crash-safe update queue.

**Which plane do you need?**

| Your bot is... | Run | What you change |
|---|---|---|
| A Bot API bot (aiogram, python-telegram-bot, grammY, Telegraf...) | `tggw botapi` | one base URL → `/tgapi/` — the poller, rate guard and durable queue move into the gateway |
| A Pyrogram-shaped client needing user sessions / big files | `tggw mtproto` | point it at the sidecar facade (send + updates + files over gotd) |

```
   deploy / crash / scale — anytime            ┌──────────────┐
  ┌─────────┐   ┌─────────┐   ┌─────────┐      │   Telegram   │
  │ bot v1  │ → │ bot v2  │ → │ bot v3  │      └──────▲───────┘
  └────┬────┘   └────┬────┘   └────┬────┘             │ one session,
       └─────────────┴─────────────┴────►  ┌──────────┴─────────┐
                HTTP (Bot API, verbatim)   │  session gateway   │
                                           │  · owns the poller │
                                           │  · rate guard      │
                                           │  · durable queue   │
                                           └────────────────────┘
```

**Start in 30 seconds** (Bot API bots — aiogram / python-telegram-bot
/ grammY / Telegraf):

```bash
# one-time: the token-encryption key. The image refuses to invent
# secrets inside the container (they'd die with it) — postgres, not nginx
docker run --rm ghcr.io/dovberkaplan/tg-session-gateway:latest genkey

docker run -d --name tggw -p 8080:8080 \
  -e GW_ADMIN_SECRET=$(openssl rand -hex 16) \
  -e GW_APP_SECRET=$(openssl rand -hex 16) \
  -e GW_FERNET_KEY=$(docker run --rm ghcr.io/dovberkaplan/tg-session-gateway:latest genkey) \
  -v tggw-data:/data \
  ghcr.io/dovberkaplan/tg-session-gateway:latest botapi
# then the ONLY change in your bot:
#   TelegramAPIServer.from_base("http://tggw:8080/tgapi")
```

Pin the version tag (`0.1.0-rc.1`). `latest` exists but moves only on stable
releases — never pre-releases. Identical tags live on GHCR
(`ghcr.io/dovberkaplan/tg-session-gateway{,-mtproto}`).

**Status, honestly:** 0.1.0-rc.1 — the Bot API plane is production-
shaped; the MTProto plane has NOT soaked against real Telegram yet
(Python <= 0.0.5 remains the production-recommended path until then).
Images are published but PRIVATE until the repo owner flips package
visibility; until then build from source (`docker compose up -d`).
Upgrading from 0.0.5? Read [the migration guide](docs/migration.md)
first — image names changed, MTProto methods are an allowlist, Redis
is gone.

No Docker? `go install github.com/DovBerKaplan/tg-session-gateway/cmd/tggw@latest`
(or `make install`) — one static binary, both planes.

**Measured, not claimed:** gateway overhead on the hot paths is
~2-3ms (send p50 6.4ms end-to-end including a ~4ms Python test
client; pull-with-ack 5.5ms; SIGKILL recovery instant). Resting
footprint in kind: ~1m CPU / ~7Mi RAM — one process carries many bots.
Methodology and reproduction: [docs/performance.md](docs/performance.md).
Working consumer code (Go SDK, Python, Node) in [examples/](examples/).

| | Direct Bot API | Local Bot API server | MTProxy | **Session Gateway** |
|---|---|---|---|---|
| Session lifecycle independent of app deploys | no | partial | no | yes |
| Zero-downtime bot deploys | no | no | no | yes |
| Built-in rate guard + 429 isolation per chat | no | no | — | yes |
| Update queue that survives kill -9 | no | no | — | yes |

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
│  └ your code    │           ┌─────────▼─────────────┐
└─────────────────┘           │  Telegram Session     │
                              │  Gateway              │
  deploy = all dies           │  ├ session (persisted)│
                              │  ├ update loop        │
                              │  ├ rate guard         │
                              │  └ durable queue      │
                              └─────────┬─────────────┘
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

Everything above is the **Bot API plane** (`tggw botapi`) — the
drop-in `api.telegram.org` replacement, one base-URL change. If your
bot runs on **Pyrogram/MTProto**, the same binary serves the MTProto
sidecar (`tggw mtproto`): your app speaks HTTP/WebSocket, the sidecar
owns the auth_key, connection, and update loop. Same principles, same
rate guard: [sidecar docs →](docs/sidecar-spec.md)

Both share the same principles: a long-lived infrastructure container
that owns the Telegram connection, enforces the official
[Bot API FAQ rate limits](https://core.telegram.org/bots/faq), and lets
you deploy your application freely — no FloodWait, no Conflict, no
AUTH_KEY_DUPLICATED.

## The Go build is the product

This repo ships **`tggw`** — a single static Go binary covering both
planes (`tggw botapi` / `tggw mtproto`) — with embedded SQLite on a
PVC, no Redis, and a native Kubernetes chart. Status: implemented,
conformance-tested and kind-verified; **soak against real Telegram
traffic is the remaining gate before the 1.0 defaults flip** (see the
runbook checklist). The previous Python implementation lives on the
[`legacy-python`](../../tree/legacy-python) branch; its images (≤0.0.5)
remain on GHCR for rollback.

What this tree contains (module
`github.com/DovBerKaplan/tg-session-gateway`):

- Both planes behind the same HTTP contract, the same `GW_*` env and
  the same metric names, opening the existing SQLite files **in place**
  (near-free data migration)
- `tggw migrate` — pyrofork sessions → gotd sessions with no re-login;
  `Telegram logins required: 0` is the cutover gate
- Helm chart + rendered `install.yaml`, distroless image (~22MB),
  ReadWriteOncePod PVC, NetworkPolicy (ingress by namespace label,
  egress to Telegram CIDRs only), PDB, preStop drain hook, and a split
  admin port (`GW_ADMIN_PORT`) so operators keep `/admin` off the API
  Service
- Measured on kind: **idle 1m CPU / 7Mi RAM** per pod (Python baseline
  80–150MB), pod-kill recovery in ~4s with exactly-once update replay

Try it: `docker compose up` (same `.env`),
or [the Helm chart →](deploy/helm/tggw/README.md). The verification is
reproducible: `deploy/helm/verify-kind.sh` and `tests/conformance_go/`
run the real binary black-box. Reasoning: [ADR-0002](docs/adr/0002-go-rewrite.md);
phases in the [changelog](CHANGELOG.md).

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

Bots also get an auto typing indicator for free: from the moment a user
messages or presses a button (private chats by default) until your app
answers, the gateway keeps a `typing...` heartbeat alive — honestly,
because it stops the moment that chat is rate-paused by a real 429, and
on timeout it tells the user (in English) that the bot is having
provider trouble. Control it per bot or gateway-wide via the admin API;
your app can stop it with `sendChatAction {"action": "cancel"}` or
suppress just the timeout notice with `"cancel_timeout"`.
[Details →](docs/spec.md#104-auto-typing-indicator)

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

## Invariants

Design rules the code enforces — with the tests that prove them.
When they break anyway: [runbook](docs/runbook.md) · [alerting](docs/alerting.md) · [alert rules](monitoring/alerts.yml)
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
  *Tests: restart-with-unacked-updates
  (`tests/test_integration_gateway.py`), kill -9 interleavings
  (`tests/test_restart_semantics.py`).*
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

### Restart semantics (kill -9 mid-FloodWait)

What exactly happens when the process dies mid-backoff — a worker
waiting out a FLOOD_WAIT (retry at T+30s) takes kill -9 and the
supervisor restarts it before T+30s. Pinned by
`tests/test_restart_semantics.py`:

- **The pending retry belongs to the consumer, not the gateway.** On a
  real 429/FLOOD_WAIT the gateway answers with `retry_after` and keeps
  no outbound retry queue. The guard's preventive chat pause is
  in-memory and dies with the process — after a restart the chat is
  unpaused, and if the consumer retries before Telegram's own window
  elapses, Telegram re-issues FLOOD_WAIT and the guard re-learns.
  Telegram is the authority; the pause is an optimization.
- **Queued updates are durable and duplicate-free across the crash.**
  Every queue push commits synchronously to SQLite-WAL — safe against
  process death (`kill -9`); `synchronous=NORMAL` trades machine
  power-loss durability for speed. On restart only unacked updates
  replay, and the persisted consumer offset floors delivery: even a
  Telegram redelivery that raced the kill cannot re-deliver an update
  the consumer already acked. Unacked updates replay at-least-once —
  ack early if you need at-most-once side effects.
- **The file lock arbitrates the old/new overlap.** A dead holder's
  lock is reclaimed immediately (PID liveness check), so a fast
  supervisor restart is the normal path, not a 60s wait; the heartbeat
  window only bounds a frozen holder or PID reuse
  (`tests/test_session_lock.py`).

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
printf 'GW_ADMIN_SECRET=%s\nGW_APP_SECRET=%s\nGW_FERNET_KEY=%s\n' \
  "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" \
  "$(CGO_ENABLED=0 go build -o /tmp/tggw ./cmd/tggw && /tmp/tggw genkey)" > .env
docker compose up -d   # builds + starts on 127.0.0.1:8080
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

Pre-built multi-arch images (amd64/arm64); one image, both planes.
Docker Hub is the short pull command; GHCR is the source of truth with
identical tags. Pin the version — `latest` moves only on stable
releases, never pre-releases:

```bash
docker pull ghcr.io/dovberkaplan/tg-session-gateway:0.1.0   # = dovberkaplan/tggw:0.1.0
docker run --rm ghcr.io/dovberkaplan/tg-session-gateway:0.1.0 mtproto \
  -e GW_API_ID -e GW_API_HASH ...   # same image, MTProto plane
```

For Kubernetes: [the Helm chart →](deploy/helm/tggw/README.md) (or the
rendered [install.yaml](deploy/helm/install.yaml)), kind-verified —
`deploy/helm/verify-kind.sh` reproduces the whole acceptance locally.

**Verify what you pull** — the image (GHCR and the Docker Hub mirror)
and the release `checksums.txt` are signed keylessly on every `v*`
tag; the certificate binds to this repository's CI identity:

```bash
cosign verify ghcr.io/dovberkaplan/tg-session-gateway:0.1.0 \
  --certificate-identity-regexp '^https://github.com/DovBerKaplan/tg-session-gateway/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

**Python ≤0.0.5** (rollback images): `ghcr.io/dovberkaplan/tg-session-gateway`
and `...-mtproto`, source on the [`legacy-python`](../../tree/legacy-python) branch.

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
| [API Reference](docs/api-reference.md) | OpenAPI spec for both planes |
| [Migration Guide](docs/migration.md) | Move an existing aiogram/PTB/Pyrogram bot onto the gateway |
| [Comparison](docs/comparison.md) | vs direct polling, webhooks, in-process Pyrogram — and when NOT to use this |
| [Performance](docs/performance.md) | Measured latency/load numbers and how to reproduce them |
| [Scaling](docs/scaling.md) | Sharding multi-account fleets across instances |
| [Spec](docs/spec.md) | Bot API gateway specification |
| [Sidecar Spec](docs/sidecar-spec.md) | MTProto sidecar requirements |
| [Compatibility](docs/compatibility.md) | Pyrogram / pyrofork / Telethon support matrix |
| [Helm chart](deploy/helm/tggw/README.md) | Kubernetes install for the Go build (tggw) |
| [ADR-0002](docs/adr/0002-go-rewrite.md) | Why and how the Go rewrite happened |
| [ADR-0003](docs/adr/0003-flock-single-ownership.md) | flock locking; Redis lease backend removed (supersedes ADR-0001) |

## License

MIT
