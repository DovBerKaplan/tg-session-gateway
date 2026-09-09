# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/),
adherence to [Semantic Versioning](https://semver.org/).

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
