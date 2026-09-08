# Comparison & Alternatives

Honest positioning — including when you should NOT use this project.

## Bot API Gateway vs. the usual options

| | Direct polling (aiogram/PTB default) | Webhooks | **Bot API Gateway** |
|---|---|---|---|
| Deploy without dropping updates | ❌ poller dies with the process; gap until restart | ✅ Telegram retries | ✅ queue survives app restarts |
| Works without public HTTPS | ✅ | ❌ needs TLS endpoint | ✅ outbound-only |
| Multiple app replicas | ❌ one poller per token, ever | ✅ | ✅ replicas pull from the queue |
| Rate-limit protection | library-dependent | library-dependent | ✅ FAQ buckets + queueing at the gateway |
| Latency | ~long-poll (fast) | push (fastest) | ~long-poll (same as direct) |
| Extra moving part | none | none (but ingress) | one small service + volume |
| Migration effort | — | rewrite receive path | change `base_url`, that's it |

**Use webhooks if** you can run a public TLS endpoint and don't need
outbound-only networking. Webhooks are Telegram's intended production
receive path. This gateway is for when you can't or won't (private
networks, strict egress policies, many bots behind one egress IP,
consumers that expect polling semantics).

**Use direct polling if** you have one small bot, single instance, and
deploys are rare. The gateway is plumbing; not every bot needs it.

## MTProto Sidecar vs. the usual options

| | Pyrogram in-process | **MTProto sidecar** |
|---|---|---|
| Deploy without re-auth risk | ❌ every deploy reconnects; duplicate-connection risk during rolling overlap | ✅ sidecar owns the auth_key; app deploys are HTTP-level |
| Multiple replicas | ❌ `AUTH_KEY_DUPLICATED` | ✅ replicas share the sidecar |
| Rate limits | your problem | FAQ buckets + `FloodWait` parsing + per-peer backoff |
| App language | Python (Pyrogram) | anything that speaks HTTP/WS |
| Latency | in-process (fastest) | one HTTP hop |
| Migration effort | — | rewrite send/receive calls to HTTP |

**Keep Pyrogram in-process if** you have one replica and accept the
reconnect-on-deploy behavior. The sidecar earns its keep with multiple
replicas, rolling deploys, or polyglot consumers.

## vs. related projects

- **grammY runner / @grammyjs/runner** (JS): a distributed queue-scaling
  layer for grammY specifically. Same problem space for the receive
  side; tied to one framework. This gateway is library-agnostic on the
  HTTP side and additionally owns sessions and rate limiting.
- **Telethon/Pyrogram session servers / shared session DBs**: usually
  solve only session persistence, not ownership (who may connect) or
  rate limiting. The sidecar solves all three; the lock file is the
  ownership answer.
- **Homegrown "just persist the session file"**: necessary but not
  sufficient — the failure model has three layers (auth, session
  duplication, rate). See [architecture](architecture.md#failure-model-why-both-exist).

## What this project is NOT

- Not a bot framework — no handlers, no middleware, no state machines.
  Your library keeps doing that; this owns the Telegram connection.
- Not a proxy for *hiding* tokens from your own app (the `/tgapi`
  surface is token-routed, Bot API compatible, for zero-code-change
  migration; the `/bots/{alias}` surface with a shared secret is the
  hardening step).
- Not battle-tested at scale yet (v0.0.x). Rate parameters are from the
  official Bot API FAQ; real-world tuning feedback welcome.
