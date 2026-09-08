# TG Session Gateway

**Deploy your bot without dropping its Telegram session.**

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
# aiogram
Bot(token, base_url="http://tg-gateway:8080/tgapi")
# python-telegram-bot
Application.builder().token(t).base_url("http://tg-gateway:8080/tgapi").build()
# grammY / Telegraf / raw HTTP: same idea — point at the gateway
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

## Scope

V1: multi-bot, pull + push to consumers, full rate guard, isolated
lifecycle. Out of scope (by spec): MTProto user sessions (Telethon /
Pyrogram / tdata), web panels, multi-replica token sharing.

Full specification (Hebrew): [docs/spec.he.md](docs/spec.he.md).

## License

MIT
