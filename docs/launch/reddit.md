# Reddit — r/Telegram, r/TelegramBots, r/selfhosted (paste-ready, adapt per sub)

**Title:** I was tired of Telegram bot deployments killing the bot's session, so I separated them — open source

**Text:**

The scenario: Telegram bot in Docker. Every `docker compose up -d`
replaces the container, the in-process Telegram session dies, and the
new container has to reconnect/re-auth. Do it too often and Telegram
serves you a FloodWait (mine said 3342 seconds). Run two workers on
one session and you risk AUTH_KEY_DUPLICATED — which can kill the
auth_key permanently.

So I split application lifecycle from connection lifecycle:

    Telegram
       ↑
    Session Gateway (always-on: session, update loop, rate guard, durable queue)
       ↑
    Bot v1 → v2 → v3 (stateless, HTTP/WS to the gateway)

Now deploying the bot doesn't touch Telegram at all. Restart proof
(real logs, not a mockup) is in the README: gateway killed with 4
sessions live → restarted → sessions restored from disk with zero
Telegram logins, queued updates replayed exactly once.

- Drop-in for aiogram / python-telegram-bot / grammY (change base URL)
- MTProto sidecar for Pyrogram bots
- Official FAQ rate buckets + real FloodWait parsing with per-peer backoff
- kill -9-safe: durable update queue, session persistence, singleton lock

Self-hosted, Python, MIT: https://github.com/DovBerKaplan/tg-session-gateway

Not asking for stars — asking people who run Telegram bots in Docker
to try it and tell me what breaks. Honest status: v0.0.x, one
production consumer so far, everything claimed is test-linked in the
README.
