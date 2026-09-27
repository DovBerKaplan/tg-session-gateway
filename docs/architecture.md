# Architecture

Two independent products, one repo. They share nothing at runtime —
pick the one that matches how your bot talks to Telegram.

## Product 1: Bot API Gateway (`gateway/`)

For bots built on HTTP Bot API libraries (aiogram, python-telegram-bot,
Telegraf, grammY, ...).

```
                        ┌─────────────────────────────────────────────┐
                        │              gateway process                │
   your app replica(s)  │                                             │
  ┌───────────────┐     │  ┌─────────┐   ┌──────────┐   ┌─────────┐  │
  │ aiogram / PTB │────▶│  │  proxy  │──▶│ BotRate  │──▶│ httpx   │──┼──▶ api.telegram.org
  │  (send calls) │     │  │ routes  │   │ Guard    │   │ client  │  │    (writes only)
  └───────────────┘     │  └─────────┘   └──────────┘   └─────────┘  │
        │               │        │                                      │
        │  getUpdates   │        ▼                                      │
  ┌───────────────┐     │  ┌──────────────────┐    ┌───────────────┐  │
  │ aiogram / PTB │◀────│  │ UpdateQueue      │◀───│ poller (one   │  │
  │ (receive loop)│     │  │ deque + SQLite   │    │ per token)    │  │
  └───────────────┘     │  │ WAL (queue.db)   │    └───────────────┘  │
                        │  └──────────────────┘                       │
                        │        ▲                                    │
                        │  ┌─────┴──────┐   ┌───────────────┐         │
                        │  │ Store      │   │ admin API     │         │
                        │  │ gateway.db │   │ /admin/bots…  │         │
                        │  │ (encrypted │   └───────────────┘         │
                        │  │  tokens,   │                             │
                        │  │  offsets)  │   /healthz  /readyz         │
                        │  └────────────┘   Prometheus /metrics       │
                        └─────────────────────────────────────────────┘
```

**The one rule that makes deploys safe** (spec §4.6): the poller is the
only thing in the world calling `getUpdates` upstream for a token.
Applications call `getUpdates` against the gateway and are served from
the internal queue. One Telegram session per token, forever — your app
replicas connect and reconnect freely underneath it.

**Planes:**

- **Session plane** — one poller task per registered bot. States:
  `connecting → live → (paused | draining) → error`. The Telegram
  offset is persisted (`gateway.db`) so a gateway restart resumes
  exactly where it stopped.
- **Update plane** — the queue is dual-layer: an in-memory deque for the
  hot path plus SQLite WAL (`queue.db`) so unacked updates survive a
  gateway restart. Bounded (`drop_oldest` or `reject`) with TTL expiry.
- **Send plane** — every write method passes the Rate Guard (FAQ-exact
  token buckets: 1/s private burst 3, 20/min group, 30/s global, soft
  read bucket, opt-in paid 1000/s lane). `on_limit=queue` parks the
  request until a token is available; `on_limit=reject` returns a Bot
  API-shaped 429.
- **Push mode** — instead of your app pulling, set a `push_url` and the
  gateway POSTs each Update to you in official webhook shape (body =
  the Update object). Webhook libraries connect without changes.

**Auth surfaces:** `/tgapi/bot{token}/{method}` (token itself, Bot API
compatible), `/bots/{alias}/{method}` (Bearer `GW_APP_SECRET`),
`/admin/*` (Bearer `GW_ADMIN_SECRET`). Tokens at rest are Fernet-
encrypted; logs carry alias + last-4 suffix only.

## Product 2: MTProto Sidecar (`mtgateway/`)

For bots built on MTProto client libraries (Pyrogram, pyrofork).

```
                        ┌────────────────────────────────────────────┐
   your app replica(s)  │                sidecar process            │
  ┌───────────────┐     │                                            │
  │  HTTP client  │────▶│  FastAPI app                               │
  │  (no Pyrogram │     │   POST /v1/sessions/{alias}/{method}      │
  │   in your app)│◀────│   GET  …/updates   (pull)                  │
  └───────┬───────┘     │   WS   …/updates   (push)                 │
          │ WS updates  │        │                                   │
          ▼             │        ▼                                   │
                        │  ┌──────────────┐  ┌─────────────────────┐ │
                        │  │ MTSession    │  │ SessionLock         │ │
                        │  │  Pyrogram    │  │ O_CREAT|O_EXCL file │ │
                        │  │  Client      │  │ + PID + heartbeat,  │ │
                        │  │  (owns the   │  │ acquired BEFORE     │ │
                        │  │   auth_key)  │  │ connect()           │ │
                        │  └──────┬───────┘  └─────────────────────┘ │
                        │         │            ┌────────────────────┐ │
                        │         ▼            │ MTStore (SQLite)   │ │
                        │  ┌──────────────┐    │ sessions, pts/qts  │ │
                        │  │ MTRateGuard  │    │ update queue,      │ │
                        │  │ FAQ buckets  │    │ encrypted tokens   │ │
                        │  │ + FLOOD_WAIT │    └────────────────────┘ │
                        │  │  parsing +   │   /healthz  /metrics     │
                        │  │  per-peer    │   admin, Prometheus      │
                        │  │  backoff     │                          │
                        │  └──────┬───────┘                          │
                        └─────────┼──────────────────────────────────┘
                                  ▼
                            Telegram MTProto
```

**Why the lock exists:** a second Pyrogram process opening the same
session file makes Telegram see two connections on one auth_key —
`AUTH_KEY_DUPLICATED`, and the key can be invalidated permanently. The
lock file (`.lock`, PID + heartbeat, stale detection) is taken before
`connect()`; a second process fails at the filesystem and never reaches
the network.

**Rolling deploys:** only the sidecar holds the auth_key. App replicas
attach/detach via HTTP; the MTProto connection and its session file
never notice. Deploy-to-first-message is measured and exported
(`tg_gateway_deploy_to_first_message_seconds`).

**Rate handling:** preventive FAQ buckets on every send, plus reactive
parsing of real MTProto errors — `FloodWait(n)`, premium and slowmode
variants, `PEER_FLOOD` — with exponential per-peer backoff
(`flood_backoff_base`) and a full pause switch (`pause_sending`) for
incidents.

## Failure model (why both exist)

| Layer | Failure | Gateway answer |
|---|---|---|
| Auth | `AUTH_FLOOD_WAIT` on (re)login | Session persists; login happens once, ever |
| Session | `AUTH_KEY_DUPLICATED` | Single poller / singleton lock — second owner never reaches Telegram |
| Rate | 429 / `FLOOD_WAIT` | FAQ buckets prevent; real waits parsed, backed off per peer |

## Deployment shape

The gateway/sidecar is deliberately **not** in your app's Compose file.
It has its own lifecycle (`docker compose down` on your app cannot kill
the Telegram sessions), its own volumes (`/data`: `gateway.db`,
`queue.db`, session files, lock files) and its own secrets
(`GW_ADMIN_SECRET`, `GW_APP_SECRET`, `GW_FERNET_KEY`, and for the
sidecar `GW_API_ID`/`GW_API_HASH`).
