# Telegram Session Gateway

> Extracted from production bot deployments that kept losing sessions. [Origin →](ORIGIN.md)

> **Bot API only.** Manages sessions for **bot tokens** — it is not a
> user-account client and has no user login (no phone/SMS/code flow
> anywhere). Not for hijacking sessions: it protects YOUR OWN bot's
> session across deploys, exactly like self-hosting Telegram's Local
> Bot API server.

[![CI](https://github.com/DovBerKaplan/tg-session-gateway/actions/workflows/ci.yml/badge.svg)](https://github.com/DovBerKaplan/tg-session-gateway/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue.svg)](pyproject.toml)

> **Your bot container should be disposable. Your Telegram session shouldn't be.**

**1 container · 1 URL change · zero deployment downtime.**

Self-hosted Bot API gateway with session persistence, rate limiting, FloodWait backoff, and a crash-safe update queue.

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

## Quick start

```bash
docker run -d --name tggw -p 8080:8080 \
  -e GW_ADMIN_SECRET=$(openssl rand -hex 16) \
  -e GW_APP_SECRET=$(openssl rand -hex 16) \
  -v tggw-data:/data \
  ghcr.io/dovberkaplan/tg-session-gateway:0.0.3
```

Then in your bot (aiogram example):

```python
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer

api = TelegramAPIServer.from_base("http://tggw:8080/tgapi")
session = AiohttpSession(api=api)
bot = Bot(token=TOKEN, session=session)
```

Or with python-telegram-bot:

```python
from telegram import Bot
from telegram.request import HTTPXRequest

request = HTTPXRequest(base_url="http://tggw:8080/tgapi/bot")
bot = Bot(token=TOKEN, request=request)
```

Verify it's alive:

```bash
curl http://localhost:8080/healthz
```

**⚠️ Don't expose to the public internet.** The `/tgapi` endpoint forwards Bot API calls without authentication — keep it on an internal Docker network only.

## What it solves

Every Telegram bot in Docker eventually hits this: deploy v2 kills the container, the Telegram session dies with it, and you get `FloodWait` or `AUTH_KEY_DUPLICATED`. This gateway separates the session lifecycle from the app lifecycle — deploy your bot as often as you want, the session stays.

| | Direct Bot API | Local Bot API | **Session Gateway** |
|---|---|---|---|
| Session survives deploys | ❌ | partial | ✅ |
| Zero-downtime bot deploys | ❌ | ❌ | ✅ |
| Rate guard + 429 isolation | ❌ | ❌ | ✅ |
| Update queue survives kill -9 | ❌ | ❌ | ✅ |

## How it works

```
        BEFORE                          AFTER
┌─────────────────┐           ┌──────────────────────┐
│  Bot container  │           │   Bot container(s)   │
│  ├ session      │           │  └ business logic    │
│  ├ update loop  │           └─────────┬────────────┘
│  ├ rate limits  │                     │ HTTP / WS
│  └ your code    │           ┌─────────▼────────────┐
└─────────────────┘           │  Session Gateway     │
                              │  ├ session (persisted)│
  deploy = all dies           │  ├ update loop        │
                              │  ├ rate guard         │
                              │  └ durable queue      │
                              └──────────────────────┘
                               deploy = bot only
```

Your bot speaks plain Bot API over HTTP — no SDK, no lock-in. The gateway holds the Telegram connection, buffers updates through restarts, and replays them when your bot reconnects.

## Features

- **Session persistence** — auth key survives container restarts, OSTs, and host reboots
- **Rate guard** — per-chat and global token-bucket, FloodWait exponential backoff
- **Durable update queue** — updates survive `kill -9`, replayed on reconnect
- **Multiple workers** — one poller, many consumers, no `AUTH_KEY_DUPLICATED`
- **Zero-downtime deploys** — `docker compose up -d` your bot freely
- **Internal Docker network** — Bot API forwarded verbatim, no TLS setup needed

## Using with Pyrogram/MTProto

If you're on Pyrogram/MTProto (not Bot API), there's a separate MTProto
sidecar — see [mtgateway/](mtgateway/). Early stage. **Also bots only**:
it registers bot tokens over MTProto; there is no user-account login
flow (no phone number, no login code) in the codebase.

## Docker Compose

```yaml
services:
  tggw:
    image: ghcr.io/dovberkaplan/tg-session-gateway:0.0.3
    environment:
      - GW_ADMIN_SECRET=${GW_ADMIN_SECRET}
      - GW_APP_SECRET=${GW_APP_SECRET}
    volumes:
      - tggw-data:/data
    networks:
      - bot-net

  your-bot:
    image: your-bot:latest
    environment:
      - API_BASE=http://tggw:8080/tgapi
    depends_on:
      - tggw
    networks:
      - bot-net

networks:
  bot-net:

volumes:
  tggw-data:
```

## Versions

| Tag | Status |
|---|---|
| `0.0.3` | **Stable** — use this |
| `0.1.0-rc1` | Pre-release — new features, less tested |
| `latest` | Tracks stable |

## License

MIT
