# Gap analysis: `tg-session-gateway` vs the requested MTProto sidecar

**Subject:** [DovBerKaplan/tg-session-gateway](https://github.com/DovBerKaplan/tg-session-gateway)  
**Repo status at review:** `0.0.1` proof of concept (same-day commits, 2026-09-08)  
**Latest relevant commit:** `aaafe6b` — “feat: MTProto session persistence — zero app changes”  
**Purpose of this note:** map the repository against the original product requirement, list concrete gaps, and point at external projects worth studying before any redesign.

This is a requirements-fit review, not a line-by-line code audit.

---

## 1. What was actually requested

The target is a **long-lived infrastructure container** that is the *only* MTProto client for a bot (Pyrogram / Telethon / pyrofork — not `api.telegram.org`).

Hard constraints:

| # | Requirement |
|---|-------------|
| R1 | The container owns the Telegram TCP connection and the `auth_key`. |
| R2 | The application talks **only** to the container over local HTTP/WebSocket. |
| R3 | The application must not open TCP to Telegram and must not load a `.session` file. |
| R4 | App deploy / rolling update must **not** drop the Telegram connection and must **not** call `auth.importBotAuthorization` again. |
| R5 | After a version bump the app must attach to the already-running session in **under one second**. Telegram is not on the critical path of *ready*. |
| R6 | The container is **not** in the app’s Compose file. `docker compose down` on the bot must not stop it. |
| R7 | No two live replicas may face Telegram with the same key (`AUTH_KEY_DUPLICATED` is a design failure, not an ops surprise). |
| R8 | Enforce Telegram limits *before* they become real errors: Bot FAQ (1 msg/s private, 20/min group, ~30/s global) **and** MTProto `FLOOD_WAIT` / `FLOOD_PEER_WAIT` / `SLOWMODE`. |
| R9 | Pause only the relevant bucket; do not stall the whole bot. |
| R10 | Return a wait or a **synthetic 429** to the app. No silent drop. No real Telegram 429 leaking through as the primary control signal. |

Explicitly out of scope for the search (and still out of scope for a correct design):

- MTProxy for end users
- A patch that only persists `.session` inside the **application** container
- A Bot API–only gateway that does not own the MTProto connection

The pain being solved is operational: frequent deploys currently stall on multi-minute `FLOOD_WAIT` from re-auth, and a one-second rolling update stretches because Telegram will not accept an immediate second connection on the same key.

---

## 2. What the repository actually is

Two stacked products live in one repo. They do not implement the same contract.

### 2.1 Bot API gateway (`gateway/`) — the real V1 product

This is a long-lived process that sits in front of `api.telegram.org`:

- Drop-in Bot API surface: `POST /tgapi/bot<token>/<method>`
- Token-free alias route with `Authorization: Bearer`
- File proxy, health/ready, loopback admin
- Isolated lifecycle: own Compose project, external Docker network, install path outside consumer repos
- One upstream poller per token; consumer talks only to the gateway
- Rate guard aligned with the Bot FAQ
- Synthetic 429 + `retry_after` on the offending bucket
- In-memory update queue with disk-backed Telegram offsets
- Consumer integration is a `base_url` swap (aiogram / python-telegram-bot / grammY)

For **HTTP Bot API bots**, this matches R6, R8 (FAQ part), R9, R10, and the “app does not talk to Telegram” idea — *on the Bot API plane*.

The README states this clearly: the gateway “speaks the Bot API verbatim.”

### 2.2 MTProto folder (`mtproto/`) — a library patch, not a sidecar

Added in `aaafe6b`. Behaviour as documented by the repo itself:

- Intercepts `pyrogram.Client.__init__` when `in_memory=True`
- Rewrites that into a file-backed session on `PYROGRAM_SESSION_DIR` (default `/data/sessions`)
- First boot authenticates and writes the session; later boots load `auth_key` and skip `importBotAuthorization`
- Integration options: base-image entrypoint shim, `sitecustomize.py`, or `from mtproto import apply`

The same README section states what it does **not** do:

- Does not hold TCP across restarts
- Does not intercept network traffic
- Works only with Pyrogram / pyrofork
- The reconnect still takes 1–2 seconds; the FloodWait killer is skipping re-auth, not keeping the socket

The V1 scope line still says MTProto user sessions (Telethon / Pyrogram / tdata) are **out of scope**.

So the folder name and the original requirement share vocabulary. The implementation does not share architecture.

---

## 3. Requirement-by-requirement gaps

### R1 — Container owns TCP and `auth_key`

**Bot API plane:** the gateway owns *an HTTP session to Telegram’s Bot API*, not an MTProto `auth_key`. Ownership is “we are the only `getUpdates` / `sendMessage` caller,” which is the right invariant for Bot API and the wrong primitive for Pyrogram.

**MTProto plane:** `auth_key` lives in a `.session` file used by the **application process**. The gateway process is not the MTProto client.

**Gap:** no process in this repo is “the only client with the `auth_key`” while the app stays sessionless.

### R2 — App talks only HTTP/WS to the container

**Bot API plane:** yes, if the app is rewritten as a Bot API client pointed at the gateway.

**MTProto plane:** no. The app still constructs a Pyrogram `Client` and speaks MTProto itself. There is no HTTP/WS façade that stands in for `Client.send_message`, handlers, raw TL, or updates.

**Gap:** a Pyrogram app cannot drop the library and keep the same product. A Bot API app can.

### R3 — No TCP to Telegram, no `.session` in the app

The patch does the opposite of R3: it *creates* a `.session` in the app container’s volume and leaves TCP in the app.

`in_memory=True` is advertised as the app’s intent (“do not touch disk”). The patch silently reverses that intent.

**Gap:** this is exactly the excluded design (“a patch that only saves `.session` inside the application container”).

### R4 — Deploy must not drop Telegram and must not re-import bot authorization

**Bot API plane:** holds, because the poller never dies with the app.

**MTProto plane:** partial and brittle.

- If a single replica restarts and the volume is intact, `importBotAuthorization` is skipped. That removes the 20–45 minute FloodWait that comes from *re-auth*, which is real value.
- The MTProto TCP connection **does** drop. Pyrogram reconnects in ~1–2 s.
- A rolling update with overlap (old replica still connected, new replica loading the same session) is still `AUTH_KEY_DUPLICATED` territory. Skipping import does not make two live sockets on one key legal.

**Gap:** R4 as written requires the *connection* to survive the app deploy, not merely the *key file*.

### R5 — Attach in under one second; Telegram not on the ready path

**Bot API plane:** yes. Ready is “HTTP to gateway is up.”

**MTProto plane:** no. Ready still includes “Pyrogram connected to DC.” The patch claims 1–2 seconds after a cold app start. That is better than a FloodWait, and it is not “Telegram is off the critical path.”

Under one second is only achievable if the app attaches to a process that is *already* connected, via HTTP/WS, with no MTProto handshake in the app.

**Gap:** the 1–2 s reconnect is still a Telegram round-trip in the app container.

### R6 — Not in the app Compose file

**Bot API gateway:** designed correctly (`install.sh`, external network, consumer only *joins*).

**MTProto patch:** the session volume and the Pyrogram process live **in the app image**. Option 1 even says `FROM ghcr.io/dovberkaplan/tg-session-gateway:latest` and `COPY your-app/ /app/`. That inverts isolation: the app image *becomes* the gateway image, so `compose down` on the bot takes the session-holder with it.

**Gap:** MTProto persistence is coupled to the app lifecycle the spec tried to separate.

### R7 — No two replicas on one key

Nothing in the MTProto patch enforces a singleton.

- No flock / lease / “one consumer” lock around the `.session`
- No check that another process already holds the TCP connection
- A rolling deploy with `max_unavailable=0` or two Compose replicas is sufficient to burn the key

The Bot API side *does* encode “one poller per token.” That idea was not carried over to the session file.

**Gap:** `AUTH_KEY_DUPLICATED` remains an unhandled deploy mode, which was one of the original failure reports.

### R8 / R9 — Limit model

The gateway rate guard is FAQ-correct **for Bot API methods** proxied through it.

It does not wrap Pyrogram invokes. A patched Pyrogram app:

- Does not pass sends through those buckets
- Has no first-class `FLOOD_WAIT` / `FLOOD_PEER_WAIT` / `SLOWMODE` isolation at the infrastructure layer
- Relies on whatever the application or the library already does (often: sleep the whole client)

**Gap:** the best part of the repo (bucketized synthetic limits) is unreachable from the MTProto path.

### R10 — Synthetic 429 to the app

Exists on `/tgapi/...`. Does not exist for a Pyrogram call stack. The app either blocks inside the library or sees a real Telegram RPC error.

**Gap:** control-plane contract is Bot API JSON, not an MTProto-facing wait/`429`.

---

## 4. Consistency problems inside the repo

These are product-doc issues, not just missing features.

1. **Two architectures, one README.** The top of the README promises “your application talks only to the gateway.” The MTProto section then tells you to keep running Pyrogram inside the app. A reader can believe the patch delivers the diagram. It does not.

2. **Scope contradiction.** V1 scope excludes MTProto sessions. A feature named “MTProto session persistence” shipped into the same tree without changing scope or acceptance tests.

3. **Wrong success metric.** “FloodWait per deploy is eliminated” is true only for *re-auth FloodWait* on a **single** replica with a warm volume. It is not true for overlap, multi-replica, or “Telegram stays connected.”

4. **Zero-change claim is overstated.** Behaviour changes (`in_memory` is no longer in-memory; workdir is forced onto a volume; the process must be started through a shim or sitecustomize). Operational change is large: volume, image base, deploy topology. Source lines of the bot may stay the same; the contract does not.

5. **Security / tenancy.** Session files on a shared volume are the `auth_key`. The Bot API side encrypts tokens (`GW_FERNET_KEY`). The MTProto side, as documented, is a directory of session files. No lease, no encryption story in the public README, no “who may start a Client on this name” rule.

6. **Library coverage.** Requirement listed Telethon and pyrofork as first-class. Patch is Pyrogram `Client.__init__` only. Telethon session layout and client construction are different; pyrofork compatibility is assumed, not specified as a compatibility matrix.

7. **POC freshness.** Six commits, 0.0.1, in-memory update queue, no clustering, no notifier. Fine for a Bot API experiment. Too thin to present the MTProto folder as the answer to a production FloodWait / rolling-update incident.

---

## 5. Why this mismatch keeps happening

Three different problems get collapsed into one sentence (“keep the session across deploys”):

| Problem | What actually fixes it |
|---|---|
| `importBotAuthorization` FloodWait on every boot | Persist `auth_key` (file, string session, vault). The patch does this. |
| `AUTH_KEY_DUPLICATED` during rolling update | **One** live MTProto socket per key. Persist-on-disk does not do this. |
| App *ready* blocked on Telegram | Move the client into a process that outlives the app; app attaches over HTTP/WS. Neither the patch nor “faster reconnect” does this. |

The patch solves row 1 and leaves rows 2 and 3 in place. The Bot API gateway solves rows 2 and 3 — for a different protocol.

A correct MTProto sidecar is the Bot API gateway’s **lifecycle and rate-guard ideas**, applied to a process that *is* the Pyrogram/Telethon client.

---

## 6. Recommendation: study how other projects are built

Do not copy them wholesale. None of them is a drop-in of the full requirement. Use them as reference implementations for *individual* invariants.

### 6.1 Sidecar shape: one process owns Telethon, app speaks HTTP/WS

**[psyb0t/docker-telethon-plus](https://github.com/psyb0t/docker-telethon-plus)**

Study:

- Single Telethon client, single async lock, HTTP + WebSocket + MCP on top
- App never loads a session; the container does
- `restart: unless-stopped` as a separate Compose service
- Updates over `/ws/updates` so the consumer can be stateless
- Explicit throttle layers (global gap, per-chat interval, token buckets, adaptive backoff after `FLOOD_WAIT`)
- Health vs “account flood risk” as separate endpoints

Gaps vs the requirement (so they are not treated as the finished design): user `StringSession` rather than bot `importBotAuthorization`; sleeps inside the container instead of returning synthetic 429; buckets are userbot-shaped, not Bot FAQ; small / single-maintainer project.

### 6.2 Same shape on Pyrogram

**[zakirovdmr/gramgate](https://github.com/zakirovdmr/gramgate)**

Study:

- Pyrogram as the owned client
- REST + MCP from one process
- How they expose `send` / dialogs / updates without shipping the session to the caller

Gaps: agent-oriented API, not Bot FAQ + synthetic 429, not an isolated infra Compose story.

### 6.3 HTTP-MTProto bridge and bot-token auth

**[leshchenko1979/fast-mcp-telegram](https://github.com/leshchenko1979/fast-mcp-telegram)**

Study:

- One server holds MTProto; many HTTP/MCP consumers
- Bot token *and* user login paths
- Per-token session files and optional remote session storage
- Guardrails around raw TL invocation

Gaps: MCP product, not deploy isolation or FAQ buckets.

### 6.4 Session-sharing proxy (ownership of the key, many callers)

**[cjongseok/mtproto](https://github.com/cjongseok/mtproto)** (Go, old API layer)

Study the *idea* only: a proxy process holds the MTProto session; callers issue RPC over gRPC instead of opening a second DC socket. That is the correct split for R1–R3. The code itself is dated (layer 71) and should not be adopted as a runtime.

### 6.5 Singleton discipline around `AUTH_KEY_DUPLICATED`

**[chigwell/telegram-mcp](https://github.com/chigwell/telegram-mcp)**  
**[DimaPhil/telegram-proxy-api](https://github.com/DimaPhil/telegram-proxy-api)**

Study:

- Advisory file lock *before* `connect()`, so a second process never reaches Telegram
- Documented rule: one long-lived owner per session
- Optional pool of *distinct* session strings when multiple consumers are required (different keys, not two sockets on one key)

This is the missing piece of `mtproto/` in the current repo.

### 6.6 Flood isolation that does not freeze the whole client

**[gotd/contrib/middleware/floodwait](https://pkg.go.dev/github.com/gotd/contrib/middleware/floodwait)** (Go)

Study:

- Distinguishing method-scoped waits (`FLOOD_WAIT`) from peer-scoped waits (`SLOWMODE_WAIT`)
- Scheduling only the affected class of requests
- Clamping wait durations so a hostile value cannot pin the process forever

Pair this with the gateway’s existing Bot FAQ buckets rather than inventing a third limiter.

### 6.7 Official local Bot API server (contrast, not a substitute)

**[tdlib/telegram-bot-api](https://github.com/tdlib/telegram-bot-api)** and images such as `aiogram/telegram-bot-api`

Study the *ops* pattern: a long-lived container owns the Telegram connection (via tdlib), apps speak HTTP, deploys of the app do not touch Telegram.

It is still Bot API, not Pyrogram ownership of `auth_key`. Use it to steal lifecycle and health semantics, not as the MTProto answer.

### 6.8 What *not* to treat as a model

| Project / pattern | Why it is the wrong reference for this requirement |
|---|---|
| Session-file volume inside the app image | Same class as the current `mtproto/` patch |
| `pyromongo` / string session in env | Persistence only; no singleton, no HTTP façade |
| Official MTProxy / `telegrammessenger/proxy` | Transport for users, not a bot client |
| Local Bot API server as the *whole* solution | No Pyrogram/Telethon `auth_key` ownership |

---

## 7. Suggested design if the requirement still stands

Keep the Bot API gateway as V1. Do not grow `mtproto/` as a sitecustomize patch.

A V2 MTProto plane that actually matches R1–R10 looks like:

1. **Separate process, same isolation story as today’s gateway**  
   Own Compose project, external network, `restart: unless-stopped`. App Compose only joins the network.

2. **That process is the only Pyrogram or Telethon client**  
   It loads the session, holds TCP, runs the update loop. Session never leaves the volume of *this* container.

3. **Singleton lock before connect**  
   File lock or lease. A second start fails fast *without* calling Telegram.

4. **Consumer protocol is HTTP + WS (or a thin SDK that is only HTTP)**  
   Send / invoke / ack updates. App ready = “gateway `/readyz` and WS connected,” not “DC handshake done.”

5. **Reuse the existing rate guard**  
   FAQ buckets for bot-shaped sends; additional peer/method buckets for `FLOOD_WAIT`, `FLOOD_PEER_WAIT`, `SLOWMODE`. On limit: queue or synthetic 429. Never sleep the whole process unless the wait is global.

6. **Delete or quarantine the current patch**  
   As long as it ships next to the gateway README, it will be mistaken for the sidecar.

Closest codebases to read first, in order: `docker-telethon-plus` (façade + throttle), `telegram-mcp` (lock), this repo’s own `gateway/` rate guard (FAQ + synthetic 429), `gotd` floodwait middleware (bucket scope).

---

## 8. Bottom line

| Layer | Fit to the written requirement |
|---|---|
| Bot API gateway | Strong fit for Bot API bots; not what was asked for Pyrogram/Telethon |
| `mtproto/` persistence patch | Solves re-auth FloodWait on a single replica; violates R1–R3, R5–R7, R8–R10 |
| Combined repo as “the sidecar” | Does not meet the spec that motivated the search |

The missing product is not “save the session file more cleverly.” It is **move the MTProto client into the same lifecycle box the Bot API gateway already got right**, and put a local HTTP/WS contract in front of it.

Until that split exists, treat `tg-session-gateway` as a Bot API session gateway with an experimental Pyrogram helper — not as an MTProto sidecar.
