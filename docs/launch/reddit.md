# Reddit — phase 1: find testers, not stars (r/TelegramBots, r/docker, r/selfhosted)

The launch research is clear: problem-first DISCUSSION posts recruit
real users; "check out my project" posts get ignored. Ask the question,
offer the tool as your answer, ask for testers explicitly.

## Post A — the question (r/TelegramBots, r/docker)

**Title:** How do you handle Telegram bot deployment restarts without tripping rate limits / dropping the session?

**Text:**

Every `docker compose up -d` on my bot used to mean: container
replaced → in-process Telegram session dies → reconnect/re-auth →
eventually Telegram served me a FloodWait of 3342 seconds. Two
workers on one session and you risk AUTH_KEY_DUPLICATED (permanent
auth_key loss).

I ended up separating the lifecycles: a small always-on gateway owns
the session + polling + rate guard + a crash-safe update queue; the
bot is a stateless container that talks to it over plain Bot API
(one base-URL change in aiogram/PTB). Deploying the bot doesn't touch
Telegram anymore — restart proof with real logs is in the README.

Open source, self-hosted, Python:
https://github.com/DovBerKaplan/tg-session-gateway

**Looking for testers** running Telegram bots in Docker (aiogram /
python-telegram-bot / grammY): does this match the failure mode you
see? What's missing for YOUR setup? Honest scope: v0.0.x, one
production consumer, everything claimed is test-linked.

## Post B — lighter variant (r/selfhosted)

**Title:** Self-hosting a Telegram session gateway so bot deploys stop killing the connection — looking for testers

Same body, emphasize the self-hosted/compose angle.

## Rules of engagement
- One subreddit per 2-3 days; adapt, never crosspaste identical text
- Answer every reply within the first hours (HN/Reddit algorithms weigh early engagement)
- Feedback → issues on GitHub → visible fixes = the real growth loop
