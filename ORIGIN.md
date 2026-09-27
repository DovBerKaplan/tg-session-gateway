# Origin

This gateway was **extracted from production bot deployments**, not
designed on a whiteboard. The failure it solves hit us for real: every
container redeploy killed the Telegram session, and the next start ate
a FloodWait or an AUTH_KEY_DUPLICATED.

## What actually happened

Bot containers here deploy frequently. Each deploy used to cost the
session; the gateway moved session ownership out of the app lifecycle —
the bot container became disposable, the session didn't. The queue,
backoff, and lock semantics were hardened against the crashes we
actually saw in production, not synthetic ones.

## What stays closed

The bots themselves and the wider stack they live in. This repo is the
piece that stands alone: any aiogram/python-telegram-bot app pointing
at a Bot API base URL benefits the same way.
