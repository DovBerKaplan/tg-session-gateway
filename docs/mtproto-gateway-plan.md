# MTProto Session Gateway — Engineering Plan

> Status: DRAFT — for review before implementation.
> Maps to the full specification (docs/spec.md) and to the
> [Telegram Bots FAQ](https://core.telegram.org/bots/faq).

---

## 0. Problem Statement

Telegram punishes three separate layers. A complete solution must cover
all three; solving only one still leaves production downtime.

| Layer | What Telegram sees | Symptom | Consequence |
|---|---|---|---|
| **Auth** | Repeated `importBotAuthorization` on the same token | FLOOD_WAIT 20-45 min per deploy | Bot offline for everyone |
| **Rolling** | Two replicas using the same auth_key simultaneously | AUTH_KEY_DUPLICATED or forced wait | Session invalidated, re-auth required |
| **Rate** | >1 msg/s to a chat, >20/min to a group, >30/s per bot | 429 / FLOOD_WAIT / FLOOD_PEER_WAIT | Bot muted for everyone |

Current approaches and their coverage:

| Approach | Auth | Rolling | Rate |
|---|---|---|---|
| `.session` file on a volume (in the app container) | solved | fails | fails |
| `in_memory=True → False` monkey-patch | solved | fails | fails |
| Bot API gateway (what we built) | n/a for MTProto | partial | partial |
| **MTProto Session Gateway (this plan)** | **solved** | **solved** | **solved** |

---

## 1. Architecture

### 1.1 Principle

A long-lived sidecar process, outside the application's compose, that
is the **sole MTProto client** for each bot session. The application
connects to it locally via HTTP/WebSocket. Deploys and rolling updates
never touch Telegram.

### 1.2 Topology

```
Telegram Data Centers
        |
        |  MTProto (auth_key, persistent TCP, per-session)
        v
+--------------------------------------------------+
|              MTProto Session Gateway              |
|                                                   |
|  +---------------+  +------------------------+   |
|  | Session Holder|  |     Rate Guard         |   |
|  |---------------|  |------------------------|   |
|  | Pyrogram      |  | FAQ buckets:           |   |
|  | Client per    |  |   1/s private (b3)    |   |
|  | bot token     |  |   20/min group         |   |
|  |               |  |   30/s global          |   |
|  | Session file  |  | FLOOD_WAIT per-peer    |   |
|  | on volume     |  | SLOWMODE               |   |
|  |               |  | Fast path: callbacks   |   |
|  +-------+-------+  +------------------------+   |
|          |                                        |
|  +-------v---------------------------------+     |
|  |         Internal Update Bus             |     |
|  |  Queue + per-consumer offset            |     |
|  +-------+---------------------------------+     |
|          |                                        |
|  +-------v---------------------------------+     |
|  |       Consumer API (HTTP/WS)           |     |
|  |  POST /v1/sessions/{alias}/{method}    |     |
|  |  WS   /v1/sessions/{alias}/updates     |     |
|  |  GET  /v1/sessions/{alias}/health      |     |
|  +----------------------------------------+     |
+--------------------------------------------------+
         |                          |
         v                          v
   Application A               Application B
   (freely deployable)        (freely deployable)
```

### 1.3 Key Design Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Language | Python 3.12 | Matches the target deployment; Pyrogram/pyrofork is the MTProto engine |
| MTProto library | pyrofork | Battle-tested fork of Pyrogram, already proven in production use |
| Consumer API | HTTP + WebSocket | HTTP for request/response; WS for push updates |
| Update delivery | Push (WS) primary, Pull (HTTP) fallback | WS = zero-latency; HTTP pull for compatibility |
| Rate limiting | In-memory token buckets | Sub-millisecond overhead; no external dependency |
| Persistence | SQLite WAL (offsets, session metadata) | Survives restarts; zero-config; no external DB |
| Session files | Docker volume (per-gateway, exclusive) | Never inside the application image |
| Lifecycle | Own compose project, external network | Same isolation pattern as the Bot API gateway |

---

## 2. Phase 1 — Session Core (MVP)

**Goal:** The gateway is the sole MTProto client. Applications deploy
freely without Telegram-visible changes.

**Duration estimate:** 2 weeks of focused work.

### 2.1 Deliverables

| ID | Component | Description | Spec Ref |
|---|---|---|---|
| P1.1 | Session Manager | Pyrogram Client per bot token, long-lived asyncio task | F1, F4, F5 |
| P1.2 | Session Persistence | `.session` files on a Docker volume; auth_key survives gateway restarts | F2, F3 |
| P1.3 | Update State | pts/qts/date/seq persisted alongside the session | F3 |
| P1.4 | HTTP API | `POST /v1/sessions/{alias}/{method}` — transparent proxy to Pyrogram methods | F10, F12 |
| P1.5 | WS Updates | `WS /v1/sessions/{alias}/updates` — push updates to the consumer | F11 |
| P1.6 | Pull Updates | `POST /v1/sessions/{alias}/getUpdates` — long-poll from internal queue | F11 |
| P1.7 | Lifecycle Isolation | Own compose project, external network, dedicated volume | F6 |
| P1.8 | Rolling Support | New replica connects; old replica disconnects; Telegram sees nothing | F7, F8 |
| P1.9 | Safe Restart | Drain → save session → restart → resume from same auth_key (≤3s) | F9 |
| P1.10 | Health Endpoints | `/healthz` (process alive) + `/readyz` (session connected to Telegram) | F27 |
| P1.11 | Admin API | Register/pause/resume/drain/delete sessions | Management interface |
| P1.12 | File Proxy | Transparent media download/upload through the gateway | F12 |

### 2.2 Consumer API Contract

```
Base: http://tg-mtproto:8080/v1

# Send a message (any Pyrogram method, same parameters)
POST /sessions/{alias}/send_message
  Authorization: Bearer <app-secret>
  Content-Type: application/json
  {"chat_id": 123456, "text": "Hello"}

Response (success):
  200 {"ok": true, "result": {"message_id": 42, ...}}

Response (rate limited):
  429 {"ok": false, "parameters": {"retry_after": 3}}

Response (session reconnecting):
  503 {"ok": false, "description": "session reconnecting", "retry_in": 2}

# Receive updates (WebSocket push)
WS /sessions/{alias}/updates
  → Server pushes each update as a JSON object

# Receive updates (HTTP pull, Bot-API-compatible)
POST /sessions/{alias}/getUpdates
  {"offset": 100, "timeout": 25, "limit": 100}
  → 200 {"ok": true, "result": [...]}

# Session health
GET /sessions/{alias}/health
  → 200 {"state": "live", "connection_age_s": 3600, "queue_depth": 0}

# Media download
GET /files/{alias}/{file_id}
  → binary stream

# Media upload
POST /sessions/{alias}/upload
  Content-Type: multipart/form-data
  → 200 {"ok": true, "result": {"file_id": "..."}}
```

### 2.3 Pyrogram Method Coverage (Priority Order)

Phase 1 must proxy these methods (the target deployment's actual usage):

| Priority | Method | Used by |
|---|---|---|
| P0 | `send_message` | All bots |
| P0 | `send_photo` | All bots |
| P0 | `send_video` | All bots |
| P0 | `send_document` | All bots |
| P0 | `edit_message_text` | All bots |
| P0 | `edit_message_media` | All bots |
| P0 | `delete_messages` | All bots |
| P0 | `get_messages` | All bots |
| P0 | `answer_inline_query` | Engine bots |
| P0 | `answer_callback_query` | All bots |
| P1 | `send_chat_action` | All bots |
| P1 | `forward_messages` | All bots |
| P1 | `copy_message` | All bots |
| P1 | `get_chat` | All bots |
| P1 | `get_me` | All bots |
| P2 | `send_audio` | Some bots |
| P2 | `send_animation` | Some bots |
| P2 | `get_chat_member` | Some bots |

Any unlisted method: the gateway will attempt it via generic proxy
(`POST /sessions/{alias}/call/{method_name}`) with a warning that
it's not explicitly tested.

### 2.4 Rolling Update Sequence

```
1. New app replica starts
2. New replica connects to gateway (WS or HTTP)
3. Gateway registers the new consumer
4. Gateway begins forwarding updates to the new consumer
5. Old replica receives drain signal (or timeout)
6. Old replica disconnects from gateway
7. Telegram observes: nothing happened

Critical timing: steps 2-6 take <1 second.
Telegram connection (step 0) was never touched.
```

### 2.5 Acceptance Criteria (Phase 1)

- [ ] 15 deploys in 20 minutes: zero `importBotAuthorization` after
      the first session creation; zero FLOOD_WAIT on auth.
- [ ] Rolling update with intentional 5-second overlap between replicas:
      zero `AUTH_KEY_DUPLICATED`; new application ready in <1s.
- [ ] Application down for 90 seconds while gateway is live:
      after restart, processing continues from offset without any
      Telegram-visible pause.
- [ ] `docker compose down` on the application project does not stop
      the gateway container.
- [ ] Application readiness = "connected to gateway + session ready",
      NOT "completed DC handshake".

---

## 3. Phase 2 — Rate Guard for MTProto

**Goal:** The gateway enforces FAQ limits and respects real FLOOD_WAIT.
One busy chat cannot silence the bot for everyone.

**Duration estimate:** 1 week.

### 3.1 Deliverables

| ID | Component | Description | Spec Ref |
|---|---|---|---|
| P2.1 | FAQ Buckets | 1/s private (burst 3), 20/min group, 30/s global per session | F15, F16 |
| P2.2 | Fast Path | `answer_callback_query` / `answer_inline_query` bypass broadcast bucket | F17 |
| P2.3 | Paid Broadcast | Double opt-in (gateway flag + request flag); 1000/s bucket | F18 |
| P2.4 | FLOOD_WAIT Parse | Extract wait time from Pyrogram exceptions; pause exact peer bucket | F19 |
| P2.5 | Policies | `queue` (default, per-chat waiting) or `reject` (synthetic 429) | F20 |
| P2.6 | Silent Methods | `resolve_username`, `get_participants` — separate soft bucket | F21 |
| P2.7 | Adaptive Backoff | After real FLOOD_WAIT: exponential backoff on same peer+method | F22 |

### 3.2 MTProto-Specific Rate Handling

Unlike Bot API (which returns JSON with `parameters.retry_after`),
Pyrogram raises exceptions with embedded wait times:

```python
# Pyrogram exception patterns the gateway must parse:
FloodWait(42)           # wait 42 seconds — general
FloodPremiumWait(10)    # wait 10 seconds — premium rate
SlowmodeWait(5)         # wait 5 seconds — group slow mode
PeerFloodInvalid()      # peer flood — back off from this peer entirely
```

The gateway catches these, applies per-peer backoff, and returns a
synthetic 429 to the consumer with the exact wait time.

### 3.3 Acceptance Criteria (Phase 2)

- [ ] 50 messages/second submitted to the gateway: output is capped at
      ~30/second; Telegram returns zero real 429s.
- [ ] 40 messages/minute to one group: excess is held in the group's
      bucket; other chats continue at normal speed.
- [ ] A real FLOOD_WAIT from Telegram: the gateway pauses only the
      affected peer's bucket for the exact `retry_after`; other peers
      are unaffected.

---

## 4. Phase 3 — Production Hardening

**Goal:** Production-grade reliability, observability, and durability.

### 4.1 Deliverables

| ID | Component | Description | Spec Ref |
|---|---|---|---|
| P3.1 | Queue Persistence | Update queue written to SQLite WAL; survives gateway restart | N6 |
| P3.2 | Per-Peer Egress Queue | One chat cannot starve others in the send queue | E1 |
| P3.3 | Idempotency Keys | Application-supplied key prevents duplicate sends after timeout | E2 |
| P3.4 | Explicit Drain | Clean shutdown: drain queues, save session, notify consumer | E3 |
| P3.5 | Pause Sending | "Receive but don't send" mode without disconnecting the session | E4 |
| P3.6 | Prometheus | `/metrics` with per-session gauges, counters, histograms | F26 |
| P3.7 | Deploy-to-First-Message | Metric measuring the actual rolling SLA | E7 |
| P3.8 | Queue Overflow Policy | `drop_oldest` / `reject` / backpressure, explicitly configured | E6 |
| P3.9 | Token Encryption at Rest | Fernet-encrypted tokens; only suffix in logs | F29, F28 |

### 4.2 Prometheus Metrics (per session)

```prometheus
# Connection
tg_gateway_session_state{alias="bot1"} 1  # 1=live, 0=disconnected
tg_gateway_session_connection_age_seconds{alias="bot1"} 3600

# Queue
tg_gateway_update_queue_depth{alias="bot1"} 0
tg_gateway_update_queue_max_wait_seconds{alias="bot1"} 0

# Rate
tg_gateway_sends_per_second{alias="bot1"} 12.5
tg_gateway_rate_limited_requests_total{alias="bot1"} 145
tg_gateway_real_flood_wait_total{alias="bot1"} 0
tg_gateway_synthetic_429_total{alias="bot1"} 145

# Buckets
tg_gateway_empty_buckets{alias="bot1",type="private"} 2
tg_gateway_empty_buckets{alias="bot1",type="group"} 0

# SLA
tg_gateway_deploy_to_first_message_seconds{alias="bot1"} 1.2
```

---

## 5. Phase 4 — Multi-Consumer (Deferred)

Not required for V1. Documented for future planning.

| Feature | Description |
|---|---|
| Lease mechanism | One active consumer per session; automatic failover |
| Replica support | Multiple application instances share one session via the gateway |
| Sharding | Multiple gateway processes with disjoint token assignments |

---

## 6. Telegram Constraints — Iron Rules

These are non-negotiable. Violation causes session loss or bot-wide
FloodWait.

1. **One auth_key = one primary connection to the home DC.** Exception:
   `tmp_sessions > 1`. Violation → `AUTH_KEY_DUPLICATED`, key may be
   invalidated. The gateway is the only process holding the connection.

2. **Repeated `importBotAuthorization` → long FLOOD_WAIT.** Never call
   this during a deploy. The gateway authenticates once, stores the
   auth_key, and never re-authenticates unless the key is invalidated.

3. **FAQ send limits as a bot:**
   - Private chat: ≤ 1 message/second (short burst allowed, then 429)
   - Group: ≤ 20 messages/minute
   - Broadcast: ~30 messages/second, unless paid broadcasts explicitly
     enabled

4. **MTProto additional limits:** `FLOOD_WAIT_X`, `FLOOD_PEER_WAIT_X`,
   `SLOWMODE_WAIT_X`. Respect every real `retry_after`; never send above it.

5. **The same `.session` file is never opened in two live processes.**
   The gateway is the single process; the application is a guest.

6. **Webhook and long-poll toward Telegram are XOR** — if touching
   Bot API at all.

---

## 7. Migration Path for the Consumer Project

### What changes in the consumer

The transport layer: Pyrogram client calls → HTTP calls to the gateway.

```python
# Before (direct Pyrogram):
await client.send_message(chat_id, "hello")

# After (via gateway):
async with httpx.AsyncClient(base_url="http://tg-mtproto:8080/v1") as http:
    resp = await http.post(
        f"/sessions/{alias}/send_message",
        json={"chat_id": chat_id, "text": "hello"},
        headers={"Authorization": f"Bearer {app_secret}"},
    )
```

A thin `GatewayClient` class can wrap this to maintain a Pyrogram-like
API surface, minimizing the diff in the consumer's handlers.

### What does NOT change in the consumer

- All handler logic
- All business logic
- All database interactions
- All SLM/moderation flow
- Rate limiting within handlers (the gateway's Rate Guard is the
  enforcement layer; handler-level limits become advisory)

### Migration steps

1. Deploy the gateway alongside the consumer (gateway is a new service)
2. Register the consumer's bot tokens with the gateway
3. Create a `GatewayClient` class in the consumer (thin HTTP wrapper)
4. Replace `client.send_message(...)` with `gateway.send_message(...)`
   throughout (mechanical, find-and-replace)
5. Replace update handlers: point them at the gateway's WS/push
6. Remove direct Pyrogram from the consumer's Docker image
7. Test: deploy the app 15 times in 20 minutes → zero FloodWait

---

## 8. Comparison with Alternatives

| Solution | Auth per deploy | Rolling overlap | FAQ under load | Protocol |
|---|---|---|---|---|
| `.session` on volume in the app | solved | fails | fails | any |
| `in_memory=True → False` patch | solved | fails | fails | Pyrogram |
| BotMux / Bot API gateway | n/a for MTProto | partial | partial | HTTP only |
| docker-telethon-plus | solved | partial | partial | Telethon HTTP |
| TelegramApiServer | solved | partial | no rate guard | Madeline HTTP |
| **This plan** | **solved** | **solved** | **solved** | Pyrogram HTTP/WS |

---

## 9. Testing Strategy

### Unit tests
- Rate Guard buckets (already 30+ tests from the Bot API gateway)
- FLOOD_WAIT parsing from Pyrogram exception strings
- Session lifecycle state machine
- Queue overflow policies

### Integration tests (mock Telegram)
- Session Manager against a mocked Pyrogram Client
- HTTP API contract tests (request/response shapes)
- WS push delivery
- Rolling update simulation (old consumer disconnects, new connects)

### End-to-end tests (real Telegram, CI)
- Register a test bot token
- Send/receive messages through the gateway
- Deploy simulation: kill the consumer, restart it, verify zero
  Telegram reconnection
- Rate limit verification: send above 1/s, verify the gateway throttles

### Load tests
- 30 messages/second sustained through the gateway
- 20 sessions concurrently
- Queue depth under burst load

---

## 10. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Pyrogram API changes in future versions | Gateway breaks | Pin pyrofork version; abstract method calls |
| Telegram changes MTProto behavior | Session drops | Monitor; the gateway reconnects from saved auth_key |
| Gateway becomes a single point of failure | All bots down | Health checks + autoheal; restart is ≤3s from saved session |
| Consumer sends malformed Pyrogram parameters | Gateway crash | Per-request validation; per-session error isolation |
| Memory growth from many sessions | OOM | Per-session resource limits; recommend sharding above 50 bots |
| Queue grows unbounded during app downtime | Memory | Configured cap + overflow policy + alert |

---

## 11. Timeline

| Phase | Duration | Deliverable |
|---|---|---|
| Phase 1: Session Core | 2 weeks | Deployable gateway; zero FloodWait on deploy |
| Phase 2: Rate Guard | 1 week | FAQ enforcement; one chat can't silence the bot |
| Phase 3: Hardening | 1 week | Production-grade: metrics, persistence, drain |
| Phase 4: Multi-consumer | deferred | Lease, replicas, sharding |

Phase 1 alone solves the two most critical acceptance criteria:
zero FloodWait on deploy and zero AUTH_KEY_DUPLICATED on rolling.
