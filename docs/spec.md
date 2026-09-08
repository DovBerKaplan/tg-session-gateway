# TG Session Gateway — Specification

A session gateway for Telegram Bot API bots.
Requirements and specification document — not an implementation guide.

---

## 1. Purpose

A long-lived infrastructure service that holds the connection to Telegram
on behalf of multiple bots simultaneously.

Applications talk to Telegram only through the gateway. Deploying a new
version of an application does not disconnect the session from Telegram
and does not require a reconnect.

The gateway enforces Bot API limits to prevent 429 errors that would
temporarily disable the bot for all users.

---

## 2. Problems Solved

| Problem | Current Result | Target |
|---|---|---|
| `getUpdates` owned by the application | Every deploy causes Conflict / reconnect / delay | Connection ownership at the gateway only |
| One token, multiple processes | Random crashes | Single poller per token |
| Sending / answering too fast | 429, then the bot goes silent for everyone | Block or queue per official rate buckets |
| Gateway in the project's compose | `compose down` kills the session too | Completely separate lifecycle |

---

## 3. Scope

### In Scope

- Multiple bots (tokens) in one process
- Exclusive ownership of update reception from Telegram per token
- Internal update queue that survives application disconnects
- Bot API-compatible egress proxy
- Rate limiting per official FAQ rules, including per-user/per-chat
- Internal management interface
- Connection from any project via base URL replacement only

### Out of Scope

- Bot logic, FSM, LLM, user database
- MTProto user sessions (Telethon / Pyrogram / tdata)
- Rich web panel
- Replica clusters sharing the same token
- Publishing to the internet
- Management from consumer projects' compose files

---

## 4. Binding Principles

1. **Two lifecycles.** The gateway does not belong to any bot project.
   Deploying/stopping a consumer project does not affect the gateway
   process, its network, or its volume.
2. **One token = one owner.** No two pollers in the world may run
   `getUpdates` on the same token.
3. **The application never talks to `api.telegram.org`.** All calls go
   through the gateway.
4. **The gateway never intentionally triggers 429 from Telegram.**
   Violations are stopped or delayed internally.
5. **Single source of truth for rate.** No duplicate limiter required
   in the application.
6. **Webhook and getUpdates toward Telegram are XOR.** The gateway
   chooses one. The application does not set this against Telegram.

---

## 5. Operational Isolation

### 5.1 Infrastructure Identity

| Item | Value |
|---|---|
| Location | `/opt/tg-gateway` (or equivalent, not inside a consumer repo) |
| Docker project name | `tg-gateway` only |
| Container name | `tg-gateway` |
| Network | `tg-gateway-net`, external, created once |
| Management/API port | `127.0.0.1:8080` only, or internal port on the Docker network |
| Volume | Exclusive to the gateway, outside project directories |
| Required label | `tg.gateway=true` |

### 5.2 Prohibitions

- The gateway service appearing in a consumer project's compose
- `depends_on` from consumer to gateway
- Same `COMPOSE_PROJECT_NAME` as a consumer
- Cleanup scripts / Watchtower operating on all containers without label filtering
- Machine-level `docker compose down` that includes the gateway

### 5.3 Consumer Contract

The consumer may only:

- Join the `tg-gateway-net` network as `external: true`
- Configure a base URL to the gateway
- Pass a token / alias

The consumer does not create, stop, or update the gateway.

Gateway updates are a separate, controlled operation with queue draining.

---

## 6. Logical Architecture

```
Telegram Bot API
        |
        |  Single connection per token
        v
+-------------------------------------------+
|              TG Session Gateway            |
|                                            |
|  Control plane      Session plane          |
|  -------------      -------------         |
|  Bot registration   Poller per token      |
|  Status / metrics   Offset toward Telegram|
|                     Update Bus            |
|                                            |
|  Egress plane       Rate Guard            |
|  -------------      ----------            |
|  Bot API proxy      Global bucket per bot |
|  Send queue         Per-user bucket       |
|                     Per-group bucket      |
|                     429 pause             |
+-------------------------------------------+
        |                      |
        v                      v
   Project A              Project B
   (replaceable)         (replaceable)
```

### 6.1 Session Plane

`BotSession` entity per token:

- Stable identifier (`alias`) + encrypted token
- State: `connecting | live | paused | draining | error`
- Single poller or single webhook toward Telegram
- `telegram_offset` persisted to disk
- Internal update queue

After attach: the gateway calls `deleteWebhook` then starts polling,
unless webhook mode is explicitly configured (not the default).

The gateway does not call `logOut` routinely.

### 6.2 Update Bus

- Receive update from Telegram -> write to queue + advance offset ->
  only then ack to Telegram
- The application acknowledges a **separate internal** offset
- Application disconnect does not affect the Telegram connection
- Queue capacity is configured per bot; on overflow:
  `drop_oldest` or `reject_new` policy (default: `drop_oldest` with
  alert metric)

Product version 1: one active consumer per bot.
Product version 2 (not required for initial spec): multiple consumers
with lease.

### 6.3 Egress Plane

Bot API-compatible interface:

```
/tgapi/bot<token>/<method>
/tgapi/bot<token>/getUpdates
/file/bot<token>/<path>
```

Alternative without token in URL:

```
/bots/<alias>/<method>
Authorization: Bearer <app-secret>
```

The response is identical to Telegram's format (`ok`, `result`,
`error_code`, `parameters.retry_after`).

Every write request passes through the Rate Guard before egress.

### 6.4 Failure Handling

- A crash in bot A's session does not stop other bots.
- Gateway down is the only event that causes a Telegram reconnect.
  Applications wait for health; they do not fall back to
  `api.telegram.org`.

---

## 7. Multi-Bot Model

| Rule | Detail |
|---|---|
| Isolation unit | Token |
| Practical max per process | 20-50 bots (design target, not hard limit) |
| Above target | Separate gateway servers with disjoint token assignments |
| Logging | Alias only, never full token |
| Resources | Per-bot metrics: queue depth, 429s, connection state |

No sharing of pollers, queues, or buckets between tokens.

---

## 8. Rate Limiting

Normative source: [Telegram Bots FAQ](https://core.telegram.org/bots/faq)
and the `allow_paid_broadcast` parameter in the
[Bot API](https://core.telegram.org/bots/api).

Telegram does not publish a complete state machine. The system implements
the numbers that are published, and treats every real `retry_after` as
the highest authority.

### 8.1 Buckets

| Name | Official Rule | Implementation | Key |
|---|---|---|---|
| Per-user / single chat | No more than 1 message/second in a single chat; short burst allowed | 1 token/second, burst 3 | `bot_id + chat_id` in private chat |
| Per-group | No more than 20 messages/minute per group | 20 tokens / 60 seconds, burst <= 2 | `bot_id + chat_id` for group/supergroup/channel |
| Global broadcast | No more than ~30 messages/second across all chats | 30 tokens/second | `bot_id` |
| Paid broadcast | Up to 1000/second if enabled in BotFather; 0.1 Stars per message above 30 free; Stars balance and MAU threshold | 1000/second bucket only with double opt-in | `bot_id` |

"Per-user" = the private-chat bucket. In a private chat, `chat_id` is
the user's identifier.

### 8.2 Enforcement Rules

1. Write methods with `chat_id` are counted in buckets (not just
   `sendMessage`: including media, forward, copy, edit).
2. Read methods (`getMe`, `getChat`, ...) in a separate soft global
   bucket, not in the user bucket.
3. `answerCallbackQuery` and `answerInlineQuery` on a fast path,
   outside the broadcast bucket.
4. Chat classification via `getChat` cache. A negative `chat_id` is
   never private.
5. A 429 from Telegram associated with a chat -> pause that chat's
   bucket for `retry_after`.
6. A 429 without association -> pause the entire bot's egress for
   `retry_after`.
7. Never resend to the same target before the wait expires.
8. `allow_paid_broadcast` is honored only if the bot's gateway config
   also has `paid_broadcasts_enabled=true`.
9. The system never sends above the ceiling "to test Telegram".

### 8.3 Behavior When Bucket is Empty

Configured per bot:

- `queue` (default): the request waits in a per-chat queue. The client
  waits until a configured timeout or receives `202` with a queue ID.
- `reject`: immediate synthetic 429 with estimated `retry_after`.

Quality target: real 429s from Telegram ~= 0 during normal operation.

---

## 9. Deployment Behavior

### 9.1 Consumer Application Deploy

The gateway continues `getUpdates`. Updates accumulate in the internal
queue. The new application connects and pulls. No Telegram handshake,
no Conflict.

### 9.2 Gateway Deploy

Rare event. Requires drain, process replacement, offset restoration
from disk. This is the only case of a Telegram reconnect. Consumers
see unavailability and wait.

### 9.3 Unplanned Consumer Disconnect

The Telegram session stays `live`. The queue grows up to capacity.
After reconnection, the consumer continues from its internal offset.

---

## 10. Interfaces

### 10.1 Consumer — Bot API Compatible

The consumer replaces `https://api.telegram.org` with the gateway's
base URL. No method contract changes.

Update consumption modes:

| Mode | Who pulls from the gateway | Usage |
|---|---|---|
| Pull | The consumer calls `getUpdates` on the gateway | Existing polling libraries |
| Push | The gateway POSTs an internal webhook to the consumer | Existing webhook libraries |

In Push mode: health check to the consumer every 60 seconds. Failure ->
queue accumulation, not Telegram disconnect.

### 10.2 Management

Not exposed to the internet. Requires a management secret.

Minimum capabilities:

- Create / pause / drain / delete a bot
- Show connection state, queue depth, buckets, last 429
- Enable/disable paid broadcasts at the gateway level
- Queue policy selection (`queue` / `reject`)

No message sending on behalf of the bot via the management interface
beyond controlled debugging.

### 10.3 Health

- `/healthz` — process is alive
- `/readyz` — storage available; does not require all bots `live`

---

## 11. Integration with Existing Projects

The system is considered integrated into a project if and only if:

1. The consumer is attached to the external network or the gateway's
   loopback
2. All Bot API calls go to the gateway
3. No direct `getUpdates` / `setWebhook` toward Telegram from the consumer
4. The consumer's compose does not define the gateway service

Valid for python-telegram-bot, aiogram, grammY, Telegraf, LangBot, or
a raw HTTP client.

---

## 12. Data and Security

| Data | Requirement |
|---|---|
| Token | Encrypted at rest; only suffix in logs |
| Offset | On disk, survives gateway restart |
| Update content in queue | Short TTL; not a conversation store |
| Admin | localhost / internal network + secret |
| Files | Transparent proxy; no token leakage |

---

## 13. Metrics

Per bot:

- Session state
- Telegram connection age
- Update queue depth and max wait time
- Sends/second vs the 30/s ceiling
- Number of private chats / groups with empty buckets
- Real 429 count vs synthetic 429 count
- Time from consumer deploy to resumed processing

Alert if real 429 > 0 at a steady rate, or if the update queue crosses
a threshold.

---

## 14. Acceptance Criteria

1. `docker compose down` in a consumer directory does not stop the
   gateway container and does not delete the external network.
2. At least two tokens live simultaneously, isolated from each other.
3. During build/replacement of a consumer container, updates continue
   to be received at the gateway and are released for processing after
   startup, without Telegram Conflict.
4. Sending above 1/second to the same user is stopped or delayed at
   the gateway.
5. Sending above 20/minute to the same group is stopped or delayed at
   the gateway.
6. Broadcast above 30/second for a single bot is stopped or delayed at
   the gateway.
7. `allow_paid_broadcast` does not raise the ceiling without an explicit
   gateway-side flag.
8. Consumer disconnect for two minutes does not cause a Telegram
   reconnect.
9. A controlled gateway restart restores offset without losing token
   ownership.
10. A consumer calling `api.telegram.org` directly while the gateway is
    polling fails with Conflict. This confirms exclusive ownership.

---

## 15. Risks

| Risk | Impact | Mitigation Direction |
|---|---|---|
| Consumer bypasses the gateway | Conflict, session drop | Contract + acceptance criterion 10 |
| Gateway in the same compose as a consumer | Accidental shutdown | Prohibition in section 5 |
| Duplicate limiter | Artificial delay | Single source of truth |
| Incorrect chat classification | Wrong bucket, 429 | `getChat` cache |
| Frequent gateway restarts | The delay we wanted to avoid | Separate lifecycle |
| Unbounded queue | Memory | Cap + alert |
| Paid broadcasts on by default | Stars billing | Double opt-in |

---

## 16. Product Versions

**V1 — Required**
Multi-bot, internal pull+push, Rate Guard per section 8, isolation per
section 5, acceptance criteria 1-10.

**V2 — Does not block V1**
Multiple consumers per bot with lease, Local Bot API as upstream within
the gateway's lifecycle only, inter-machine gateway sharding.
