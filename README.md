# TG Session Gateway

**Deploy your bot without dropping its Telegram session.**

> **Status: 0.0.1 — proof of concept.** Architecture and consumer
> contract per the spec; hardened against a real bot library
> (update_id offsets, multipart media, webhook-format push), but not yet
> battle-tested in production. Known limitations below.

A long-lived infrastructure container that owns the connection to Telegram
for any number of Bot API tokens. Your application talks only to the
gateway — redeploys, crashes and rebuilds of your app no longer cause
`Conflict`, reconnects, or FloodWait downtime. The gateway also enforces
the official [Bot API rate limits](https://core.telegram.org/bots/faq) so
your bot never trips a real 429.

```
Telegram ◄── one connection per token ──► ┌──────────────────────┐
                                          │   tg-gateway         │
                                          │  poller · queue ·    │
                                          │  rate guard · proxy  │
                                          └──────┬───────┬───────┘
                                             app A     app B
                                        (redeploy freely)
```

## Why

| Today | With the gateway |
|---|---|
| Every deploy reconnects the bot (Conflict / delay) | Session stays live; the new app picks up where the old one stopped |
| One busy chat gets your bot muted for everyone (429) | Token buckets per user / per group / per bot stop it first |
| Rate limiting duplicated in every app | One source of truth at the gateway |

## Quickstart

```bash
sudo bash deploy/install.sh          # /opt/tg-gateway, external network, up
curl -H "Authorization: Bearer $GW_ADMIN_SECRET" \
     -H 'Content-Type: application/json' \
     -d '{"alias":"mybot","token":"123:ABC"}' \
     http://127.0.0.1:8080/admin/bots
```

Your app joins the network and swaps one URL:

```yaml
# your compose — JOIN only, never define the gateway here
services:
  mybot:
    networks: [tg-gateway-net]
networks:
  tg-gateway-net:
    external: true
```

```python
# aiogram 3.x — Bot has NO base_url kwarg; swap the API server object:
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
session = AiohttpSession(
    api=TelegramAPIServer.from_base("http://tg-gateway:8080", is_local=True))
bot = Bot(token, session=session)

# python-telegram-bot — base_url is the part BEFORE {token}; note the /bot:
app = (Application.builder()
       .token(t)
       .base_url("http://tg-gateway:8080/tgapi/bot")           # …/bot<token>/<method>
       .base_file_url("http://tg-gateway:8080/file/bot")       # …/file/bot<token>/<path>
       .build())

# grammY / Telegraf / raw HTTP: point the client's apiRoot at
# http://tg-gateway:8080/tgapi — same shape as api.telegram.org.
```

That's the whole integration. No SDK, no protocol change — the gateway
speaks the Bot API verbatim.

## Guarantees

- **Two lifecycles.** The gateway never lives in a consumer compose;
  `docker compose down` in your project cannot touch it (acceptance §14.1).
- **One poller per token.** The gateway is the only caller of
  `getUpdates` upstream; a stray app calling Telegram directly gets the
  standard `Conflict` (§14.10).
- **Rate Guard** per the FAQ: 1 msg/s per private chat (burst 3),
  20 msgs/min per group, 30 msgs/s per bot; answers to callbacks/inline
  queries ride a fast path; real 429s pause exactly the offending bucket
  for `retry_after` (§14.4–6). Paid broadcast ceiling only behind an
  explicit double opt-in (§14.7).
- **Update queue** survives app restarts — the consumer acks an internal
  offset; a two-minute app outage never reconnects Telegram (§14.8).
- **Offsets on disk** — a controlled gateway restart resumes without
  losing ownership (§14.9).

## API surface

| Route | Purpose |
|---|---|
| `POST /tgapi/bot<token>/<method>` | Drop-in `api.telegram.org` replacement |
| `POST /bots/<alias>/<method>` | Same, token-free + `Authorization: Bearer $GW_APP_SECRET` |
| `GET  /file/bot<token>/<path>` | Transparent file proxy |
| `GET  /healthz` · `GET /readyz` | Liveness / readiness |
| `POST /admin/bots` (+pause/resume/drain/delete/push) | Management, loopback-only |

## Configuration

All via environment (see `.env.example` / `gateway/config.py`):
`GW_ADMIN_SECRET`, `GW_APP_SECRET`, `GW_FERNET_KEY`, `GW_ON_LIMIT=queue|reject`,
bucket overrides (`GW_PRIVATE_RATE` …), `GW_UPDATE_QUEUE_MAX`.

## Status & metrics

`GET /admin/status` — per bot: session state, connection age, queue depth,
empty buckets, real vs synthetic 429 counters (spec §13).

## Known limitations (0.0.1)

- **Update queue is in-memory.** App restarts/disconnects lose nothing;
  a GATEWAY restart restores Telegram offsets from disk but drops
  queued-unacked updates (Telegram already considers them delivered).
  Mitigation: `deploy/safe-restart.sh` drains queues first. Persistence
  of the queue itself is a 0.2 target.
- **Single consumer per bot** (spec V1). Multiple consumers need the V2 lease.
- **No clustering** — one gateway process; spec caps a process at ~20–50 bots.
- Alerts (spec §13) surface as metrics on `/admin/status`; no notifier wired.

## MTProto Session Persistence (Pyrogram / pyrofork)

For **Pyrogram / pyrofork** bots (MTProto): the gateway includes a
transparent session-persistence layer. **Zero changes to application code.**

Your app creates `Client(name, ..., in_memory=True)` — the patch intercepts
this and converts it to file-based sessions on a persistent volume.
Pyrogram loads the saved `auth_key` on every boot, skips
`ImportBotAuthorization` entirely, and connects in 1-2 seconds. The
FloodWait (20-45 min per deploy) is eliminated.

### Integration (zero code changes)

```dockerfile
# Option 1 — use as base image + entrypoint shim
FROM ghcr.io/dovberkaplan/tg-session-gateway:latest
COPY your-app/ /app/
CMD ["python", "-m", "mtproto.entrypoint", "your_app.main"]

# Option 2 — sitecustomize (applies automatically)
COPY mtproto/sitecustomize.py /usr/local/lib/python3.12/site-packages/
CMD ["python", "-m", "your_app.main"]

# Option 3 — explicit (in your code)
# from mtproto import apply; apply()
```

| Variable | Default | Purpose |
|---|---|---|
| `PYROGRAM_SESSION_DIR` | `/data/sessions` | Session file directory |
| `PYROGRAM_PERSIST_SESSIONS` | `1` | Set `0` to disable |

### What it does NOT do

- Does not hold TCP connections across restarts (the MTProto TCP
  reconnect takes 1-2s — the FloodWait killer is the re-auth, not the TCP)
- Does not intercept network traffic (library patch, not a proxy)
- Works only with Pyrogram / pyrofork

## Scope

V1: multi-bot, pull + push to consumers, full rate guard, isolated
lifecycle. Out of scope (by spec): MTProto user sessions (Telethon /
Pyrogram / tdata), web panels, multi-replica token sharing.

Full specification: [docs/spec.md](docs/spec.md).

MTProto gateway plan: [docs/mtproto-gateway-plan.md](docs/mtproto-gateway-plan.md).

## License

MIT
