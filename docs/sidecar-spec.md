# Telegram Session Sidecar

## Requirements Specification

**Status:** Draft  
**Scope:** MTProto bot/userbot sessions that must survive frequent deploys and user-driven load without Telegram-imposed downtime.

---

## 1. Problem

Three separate Telegram failure modes currently sit on the application’s critical path:

| Symptom | What Telegram sees | Effect on you |
|---|---|---|
| Too many deploys in a short window | Repeated `auth.importBotAuthorization` on the same bot token | `FLOOD_WAIT` of minutes to tens of minutes — not a one-second TCP handshake |
| Rolling update that should take ~1s | Old replica still holds the `auth_key` while the new one connects | `AUTH_KEY_DUPLICATED` and/or a delayed ready state even though every other service is already up |
| Burst of user traffic | More than 1 msg/s to a private chat, 20/min to a group, or ~30/s globally | Real `429` / `FLOOD_WAIT` / `FLOOD_PEER_WAIT`, sometimes stalling the **entire** bot, not just the hot chat |

The Bot API FAQ governs **outbound sending as a bot**. MTProto governs **the connection and authorization**. Both apply when the bot speaks MTProto (Pyrogram, Telethon, pyrofork, MadelineProto, etc.).

**Invariant:** the process that talks to Telegram must not live in the application’s rolling-update unit.

---

## 2. Goal

A long-lived infrastructure sidecar that:

1. Owns the MTProto session to Telegram.
2. Exposes a local, instant interface to the application.
3. Prevents the application from crossing Telegram limits — during deploys **and** under load.

**Success:** application deploy / rolling update is measured in one to two local seconds, with no Telegram handshake on the critical path, no auth `FLOOD_WAIT`, and no real send-side `429`.

---

## 3. Out of scope

- Bot business logic, FSM, LLM, user databases
- MTProxy for end-user Telegram clients
- A self-hosted Telegram server
- A rich web admin UI
- Multiple live replicas sharing one `auth_key`
- Exposing the management API to the public internet

---

## 4. Hard Telegram constraints (non-negotiable)

These are platform facts, not product preferences.

1. One `auth_key` may have only one **main** connection to the home DC unless the server advertises `tmp_sessions > 1`. Exceeding that yields `AUTH_KEY_DUPLICATED` and can invalidate the key.
2. Frequent `auth.importBotAuthorization` on the same bot token yields long `FLOOD_WAIT`. It must not run on deploy.
3. Bot sending limits (Telegram Bot FAQ):
   - Private chat: at most **1 message per second** (short bursts may pass, then `429`)
   - Group: at most **20 messages per minute**
   - Broadcast: about **30 messages per second**, unless paid broadcasts are explicitly enabled
4. MTProto additionally returns `FLOOD_WAIT_X`, `FLOOD_PEER_WAIT_X`, and `SLOWMODE_WAIT_X`. Real `retry_after` must be honored; the sidecar must not send through it.
5. The same session file must not be opened by two live processes.
6. Webhook and long-polling toward Telegram are XOR, if Bot API is touched at all.

---

## 5. Actors and boundaries

```
Telegram  ◄── single MTProto session per auth_key ──►  Sidecar
                                                       │
                                                       │  local HTTP / WebSocket
                                                       ▼
                                              Application replicas
                                              (deploy / roll freely)
```

- **Sidecar:** the only process that holds TCP to Telegram and the `auth_key`.
- **Application:** business logic only. Talks exclusively to the sidecar on an internal Docker network.
- **Operators:** manage sessions through a loopback/internal admin API.

The sidecar is **not** declared in the application Compose file. Application `docker compose down` must not stop it. There is no `depends_on` from app to sidecar.

---

## 6. Functional requirements

### 6.1 Session ownership

| ID | Requirement |
|---|---|
| F1 | Exactly one process (the sidecar) is the Telegram client for a given session. |
| F2 | The `auth_key` is loaded from a persistent volume. After the first successful login, deploys must not call `importBotAuthorization`. |
| F3 | Update state (`pts` / `qts` / `date` / `seq`, or the library equivalent) is persisted with the session. |
| F4 | The application does not open TCP to Telegram and does not load `.session` / string-session files. |
| F5 | Multiple sessions (bots or accounts) are isolated. `auth_key`s are never shared. |

### 6.2 Two lifecycles

| ID | Requirement |
|---|---|
| F6 | Sidecar lifecycle is independent of the application Compose project (`COMPOSE_PROJECT_NAME`, networks, `depends_on`). |
| F7 | On application rolling update, the new replica becomes ready by connecting to the sidecar over local HTTP/WS in **≤ 1 second**. No handshake with a Telegram DC is involved. |
| F8 | Overlap of old and new **application** replicas against the sidecar is allowed. Overlap of two processes against **Telegram** on the same `auth_key` is forbidden. |
| F9 | Controlled sidecar restart: drain → persist session → reconnect with the same `auth_key` and no re-auth. Target: ≤ 3 seconds of Telegram disconnect, no `FLOOD_WAIT`. |

### 6.3 Application contract

| ID | Requirement |
|---|---|
| F10 | The application speaks only to the sidecar on an internal network. |
| F11 | Inbound updates support both **pull** (internal long poll) and **push** (internal webhook or WebSocket). |
| F12 | All outbound Telegram calls (send, edit, answer callback, media, resolve, …) go through the sidecar. |
| F13 | Every outbound call returns immediately with success, a **synthetic** `429` plus `retry_after`, or an enqueued ack. No silent drop. |
| F14 | After deploy, attaching to a live sidecar session is immediate. The app must not wait for Telegram to “allow” a new session. |

### 6.4 Rate guard

| ID | Requirement |
|---|---|
| F15 | Token buckets at least at FAQ granularity: per private chat, per group, per session global. |
| F16 | Private-chat burst of up to 3, then 1/s sustained. |
| F17 | `answerCallbackQuery` / `answerInlineQuery` use a fast path and do not consume the send bucket. |
| F18 | Paid-broadcast ceiling is off by default and requires explicit double opt-in. |
| F19 | A real `FLOOD_WAIT` / `FLOOD_PEER_WAIT` / `SLOWMODE_WAIT` pauses **only** the offending bucket for `retry_after`. One hot group must not mute every other chat. |
| F20 | Overflow policy is configurable: `queue` (wait) or `reject` (synthetic 429). Default is `queue` with a hard cap. |
| F21 | Quiet but dangerous methods are limited too: `resolve_username`, participant fetches, join/leave — not only `sendMessage`. |
| F22 | After a real flood wait, apply adaptive backoff on that method+peer key. Immediate retry that lengthens the ban is a defect. |

### 6.5 Frequent deploys

| ID | Requirement |
|---|---|
| F23 | Fifteen deploys in twenty minutes produce zero `importBotAuthorization` calls after the first session exists, and zero auth `FLOOD_WAIT`. |
| F24 | An application crash loop must not become a connect/auth loop toward Telegram. |
| F25 | Application readiness means “connected to sidecar and session is ready”, **not** “finished DC handshake”. |

### 6.6 Observability

| ID | Requirement |
|---|---|
| F26 | Per session: connection age, queue depth, synthetic vs real 429 counts, empty buckets, active flood-wait remaining. |
| F27 | `/healthz` = process alive. `/readyz` = session connected to Telegram. |
| F28 | Logs never contain full tokens or `auth_key`s — suffix only. |

### 6.7 Security

| ID | Requirement |
|---|---|
| F29 | Tokens and session material encrypted at rest. |
| F30 | Admin API bound to localhost / internal network and authenticated with a secret. |
| F31 | Session volume has tight file permissions and is not baked into the image. |

---

## 7. Non-functional requirements

| ID | Requirement |
|---|---|
| N1 | Sidecar proxy overhead for a simple send: p99 < 50 ms on the Docker network, **excluding** Telegram RTT. |
| N2 | Time until a freshly deployed app receives updates, if the sidecar was already connected: < 1 s. |
| N3 | Design target: ≥ 20 sessions per sidecar process without dropping messages. Beyond that, run another sidecar with a disjoint session set. |
| N4 | Short Telegram network blip: automatic reconnect with the same `auth_key`, no login. |
| N5 | Application down for up to 2 minutes must not lose updates, provided the sidecar stayed up and the queue did not overflow. |
| N6 | Unclean sidecar restart: session remains valid (no re-auth). Updates that were only in memory may be lost. This must be documented; durable queue is a follow-on requirement. |

---

## 8. Rolling-update contract

This is the requirement that matches “all my services are up, Telegram is still stalling me.”

### Required sequence

1. New replica starts and connects to the sidecar; it becomes ready.
2. Old replica finishes in-flight work (drain ≤ 2 s) and disconnects from the sidecar.
3. Telegram observes neither step.

### Acceptance sentence

“All my services are up” **means** the application replica is ready. Telegram must not be on the rolling-update critical path.

### Rejected design

Mounting a `.session` file into the application container. That avoids re-auth, but during the overlap window two replicas still open the same `auth_key`. That is sufficient to trip `AUTH_KEY_DUPLICATED` or a delayed ready.

---

## 9. Follow-on requirements (“and more”)

Only items that appear as soon as the three original failures are fixed:

| ID | Requirement |
|---|---|
| E1 | Per-peer outbound queues so one chat cannot starve the rest. |
| E2 | Send idempotency keys from the app, so a new replica does not double-send after a timeout. |
| E3 | Explicit sidecar drain before stop (`safe-restart`). |
| E4 | “Pause sending / keep receiving” without dropping the Telegram session. |
| E5 | Either first-class adapters for Pyrogram **and** Telethon, or one stable HTTP/WS contract the app migrates to. A library monkey-patch alone does not satisfy rolling update. |
| E6 | Documented overflow behavior: drop-oldest, reject, or back-pressure. |
| E7 | SLO metric: time from deploy-complete to first message actually delivered to Telegram. |

---

## 10. Acceptance tests

1. **Auth flood.** 15 deploys in 20 minutes. After the first session exists: zero `importBotAuthorization`, zero auth `FLOOD_WAIT`.
2. **Rolling overlap.** Forced 5-second overlap of old and new app replicas. Zero `AUTH_KEY_DUPLICATED`. New replica ready in < 1 s.
3. **Global cap.** App attempts 50 sends/s. Sidecar clips to ~30/s or returns synthetic 429. Telegram itself does not return 429.
4. **Group isolation.** 40 messages/minute into one group. Excess waits in that group’s bucket. Other chats continue.
5. **App outage.** Sidecar stays up, app is down 90 s. On app restart, consumption resumes from persisted offset/pts with no “minute wait on Telegram.”
6. **Lifecycle isolation.** `docker compose down` in the application project does not stop the sidecar container.

---

## 11. What is insufficient

| Approach | Frequent deploys | Rolling overlap | FAQ under load |
|---|---|---|---|
| `.session` volume inside the app container | Fixed | Fails | Fails |
| Patch `in_memory=True` → file on disk | Fixed | Fails | Fails |
| Bot API gateway only (no MTProto owner) | Not applicable | Partial | Partial |
| In-process limiter in the app library | No | No | Partial |
| **Long-lived MTProto sidecar + rate guard + separate lifecycle** | Fixed | Fixed | Fixed |

Only the last row meets this spec.

---

## 12. One-paragraph product definition

A long-lived sidecar, outside the application Compose project, that is the sole MTProto client for each session. The application attaches to it locally and immediately. Deploys and rolling updates never touch Telegram. The sidecar enforces Bot FAQ limits and per-method/per-peer `FLOOD_WAIT`, and answers the application with a wait or a rejection — never with a process crash or a silent drop.
