# Show HN — paste-ready

**Title:** Show HN: I built a gateway that keeps Telegram bot sessions alive across Docker deployments

**Text:**

Every time I deployed a new version of my Telegram bot, the app
container restarted — and the Telegram session inside it died with it.
Reconnect, re-auth, and eventually Telegram answered with a FloodWait
of 3342 seconds. On one bot the auth-key got invalidated permanently
(AUTH_KEY_DUPLICATED — two processes had touched the same session).

So I separated the two lifecycles. The gateway is a small always-on
process that owns the Telegram session, the update loop, rate limiting
and a durable update queue. The bot itself is stateless and talks to
the gateway over HTTP/WebSocket. Deploying the bot is now just...
deploying the bot. Telegram never notices.

Two flavors: a drop-in api.telegram.org replacement (change one base
URL in aiogram/PTB/grammY), and an MTProto sidecar for Pyrogram bots.

The restart proof I care about: kill -9 the gateway with sessions
live and updates queued → restart → sessions restored from disk with
ZERO Telegram logins, unacked updates replayed exactly once. That
scenario is a chaos test in CI, not a claim.

It also implements the official Bot API rate buckets (1/s private,
20/min groups, 30/s global), parses real FloodWaits with per-peer
backoff, and a lock that makes a second process fail before it ever
opens a connection to Telegram.

Self-hosted, Python, MIT, single Docker compose:
https://github.com/DovBerKaplan/tg-session-gateway

Looking for feedback from people running Telegram bots in Docker:
does this match the failure mode you see? What's missing for it to
run in YOUR setup? (Honest scope: one production consumer so far,
v0.0.x — the README says exactly what is proven and what isn't.)
