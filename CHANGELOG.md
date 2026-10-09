# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/),
adherence to [Semantic Versioning](https://semver.org/).

## [0.0.4] — 2026-10-09

First consumer-facing behavior on top of the hardened core: the gateway
now answers "is the bot alive?" visually, and honestly — the indicator
stops the moment the bot actually cannot respond.

### Added
- `genkey` container subcommand (`docker run --rm <image> genkey`):
  prints a fresh Fernet key — the documented way to create
  GW_FERNET_KEY without a local python+cryptography install.
- Container preflight (gateway/preflight.py): the image now follows the
  postgres contract instead of nginx — GW_FERNET_KEY is required
  (no more silently generating a key inside the container, which dies
  with it and leaves every stored token undecryptable), and /data must
  be a mounted volume (GW_ALLOW_EPHEMERAL_DATA=1 opts in for throwaway
  demos). deploy/install.sh now generates GW_FERNET_KEY into .env
  alongside the other secrets.
- Docker Hub mirror (dovberkaplan/tg-session-gateway{,-mtproto}) in the
  release workflow: same tags as GHCR, shorter pull command; GHCR stays
  the source of truth. Skipped cleanly until DOCKERHUB_USERNAME /
  DOCKERHUB_TOKEN repo secrets are configured.
- Auto typing indicator for hosted Bot API bots (spec §10.4): from the
  moment a user acts (message / edited_message / callback_query —
  private chats only by default) until the consumer answers, stops it
  explicitly, or a configurable timeout elapses. The loop re-sends
  `sendChatAction` without consuming Rate Guard tokens but stops on
  hard-paused chats (real 429 `retry_after` — no fake typing when the
  bot cannot answer). Timeout sends one English provider-issue notice,
  private chats only. Gateway extensions: `sendChatAction` with
  `action=cancel` (full stop) and `action=cancel_timeout` (suppress the
  notice, keep typing). Three-layer settings — env boot defaults,
  live gateway-wide `POST /admin/settings/typing`, per-bot
  `POST /admin/bots/{alias}/typing` overrides — resolved per beat, so
  changes apply to running sessions immediately. New metric
  `tg_gateway_typing_active{alias}`.
- Full-lifecycle tests for the above (tests/test_typing.py).

### Changed
- `latest` image tag moves only on stable version tags — never on
  pre-releases (v1.2.3-rc1) or manual workflow runs. README examples
  now pin the version (`0.0.4`) instead of `latest`.

### Fixed
- `POST /admin/bots/{alias}/paid` no longer skips the admin-secret
  check (it was the only admin route without one).

## [0.0.3] — 2026-09-09

The complete hardening charter (docs/hardening-charter.md), every box
closed with tests. Still honest about what this is: ONE production
consumer, ~24h of real traffic, zero external users. 0.1.0 comes when
the charter's promotion criteria hold AND someone else has run it.

### Added (since 0.0.2)
- Hardening charter + ADR-0001 (lock backends decision).
- Redis lease backend: SET NX PX, TTL auto-release on crash,
  heartbeat extension, monotonic fencing tokens (fakeredis-tested);
  file backend remains the zero-dependency default — behavior
  unchanged for single-host deployments.
- Chaos soak in CI: kill -9 with stranded locks → restart → zero
  AUTH_KEY_DUPLICATED, zero lost updates, exactly-once replay.
- Sidecar update-queue persistence: write-through on push, replay
  after restart, ack-on-pull (WS stays at-least-once).
- Persisted idempotency keys (no double sends across kill -9).
- Additive-only jitter on every retry/backoff wait (never below the
  quoted flood wait — property-tested).
- Monitoring runbook: metric catalog, Prometheus alert rules,
  on-call actions, lease counters.

### Fixed
- Non-dict raw updates crashed the sidecar handler on EVERY such
  update ('str does not support item assignment') — live finding on
  production deploy; now dropped cleanly with a regression test.

## [0.0.2] — 2026-09-09

First real-traffic milestone: the sidecar carried live MTProto sessions
(four bots) against production Telegram — sends, edits, inline
keyboards, and session-restore across restarts, verified end to end.

### Fixed
- **Update queue offset contract (Bot API gateway)** — `pull(offset)`
  acknowledged through `offset` itself, but every real Bot API library
  sends `offset = highest_seen + 1` per Telegram's docs: one UNSEEN
  update was silently dropped per ack. Now acks through `offset - 1`;
  contract test rewritten to true Telegram semantics with a regression
  case.
- `deleteWebhook` from consumers returned the gateway-owned 403 and
  killed aiogram's `start_polling` at boot — now a truthful no-op
  success (`setWebhook` stays owned).
- Sidecar update handler dropped dict updates (`hasattr(__dict__)`
  guard); `_serialize_update`'s dict branch was dead code.
- Sidecar `POST /v1/sessions/{alias}/getUpdates` was shadowed by the
  generic `{method}` route (FastAPI registration order) and 404'd —
  the pull endpoint was unreachable; the generic route now delegates.
- Fresh sidecar sessions failed first sends with "Peer id invalid" —
  peer cache is now warmed via `get_dialogs` after start (file
  sessions remember peers across restarts).
- `SessionLock.release()` dead code — the fd never closed and `_fd`
  never reset (found by mypy).
- Store connections used without a connect() guard — now a
  runtime-checked property with a clear error.
- docker-compose build path after the Dockerfile move to docker/.

### Added
- Sidecar: JSON → Pyrogram argument coercion (`mtgateway/coerce.py`)
  — InlineKeyboardMarkup, InputMedia*, reply keyboards round-trip
  over HTTP.
- Sidecar: token-routed surface `POST /v1/token/bot{token}/{method}`
  for consumers that only hold bot tokens (task pools).
- Admin audit trail in both products (register/pause/resume/drain/
  delete/push/paid, `GET /admin/audit`), append-only in the store.
- Prometheus `/metrics` on the Bot API gateway (same `tg_gateway_*`
  name family as the sidecar).
- Integration suites: full Bot API gateway lifecycle against mocked
  Telegram (incl. restart-with-unacked-updates replay and replica
  overlap — no losses, per-replica duplicate-free); sidecar lifecycle
  against a fake Pyrogram client (lock-before-connect, idempotency,
  E4 pause, per-chat synthetic 429, real FloodWait parsing, second
  sidecar refused with zero client.start() calls).
- Two-PROCESS lock tests (real subprocess holds the lock).
- Session hygiene: sidecar enforces 0700 session dir / 0600 files.
- Docs: architecture (planes, diagrams, failure model), migration
  guide (aiogram/PTB/Telegraf swap, Pyrogram→sidecar mapping,
  rollback), honest comparison incl. when NOT to use this;
  README rewritten — invariants each gated on a named test,
  per-product status, compose-first quickstart, alternatives table,
  single-loop rate-guard assumption.
- CI: mypy typecheck job (0 issues); release workflow publishing
  multi-arch images to GHCR on version tags.
- RabbitMQ 429-parking pattern for consumers (documented in the
  migration guide's reference deployment).

### Changed
- `RateConfig` deduplicated (mtgateway re-uses the gateway's
  canonical class — drift would have desynced rate limits).
- README "Guarantees" → test-gated Invariants; versioning honesty
  (working prototype, sidecar early but now carrying test-mode
  traffic).


## [0.0.1] — 2026-09-08

### Added
- Initial proof of concept: Bot API gateway with rate guard and update queue.
- MTProto Session Gateway (`mtgateway/`) and Bot API Gateway (`gateway/`)
  formally specified and implemented (docs/spec.md, docs/sidecar-spec.md).
- Singleton session locks, stale detection, idempotency keys (E2),
  pause-sending (E4), deploy-to-first-message SLO (E7).
- SECURITY.md, CONTRIBUTING.md, compatibility matrix, roadmap, gap
  analysis (preserved review).
- SQLite WAL persistence for the update queue (unacked updates
  survive gateway restarts; consumer offsets resume).

### Removed
- `mtproto/` (session persistence monkey-patch) — the rejected design;
  `mtgateway/` is the correct architecture.
