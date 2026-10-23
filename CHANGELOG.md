# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/),
adherence to [Semantic Versioning](https://semver.org/).

## [Unreleased]

**The repository is now Go-only** — the Python implementation moved to
the [`legacy-python`](../../tree/legacy-python) branch (tagged v0.0.6
as its final state; rollback images ≤0.0.5 stay on GHCR). The Go
module moved from `go/` to the repo root: module path is now
`github.com/DovBerKaplan/tg-session-gateway`.

Go rewrite of both planes — single static binary `tggw`, native
Kubernetes, zero client-service dependencies. **Pre-release**: the
Python images remain the production path until the Go build soaks
against real Telegram. Reasoning: [ADR-0002](docs/adr/0002-go-rewrite.md).

### Added
- both planes at the repo root (`tggw botapi` / `tggw mtproto`) behind
  the same HTTP contract, `GW_*` env and metric names; modernc SQLite
  opens the existing `gateway.db`/`queue.db`/`mtgateway.db` in place;
  Redis removed (StatefulSet replicas:1 + ReadWriteOncePod PVC + file
  lock) — stages/commits: 7de9f17 (foundations), 52907ed (Bot API),
  73e437c (MTProto + `tggw migrate`, cutover gate `Telegram logins
  required: 0`), 61253f2 (Helm chart + install.yaml + distroless
  21.9MB image + compose), 45ea717 (CI: race/lint/conformance/image),
  2978157 (kind verification + multi-arch release), df7e0fc (golden
  files from the real Python sidecar, GW_ADMIN_PORT admin split with
  admin Services + preStop `tggw terminate`, real media
  upload/download via gotd uploader/downloader, chaos/soak black-box,
  automated kind acceptance `deploy/helm/verify-kind.sh`).
- Measured on kind v1.31: idle 1m CPU / 7Mi RAM per pod (Python
  baseline 80–150MB); pod-kill recovery ~4s with exactly-once replay;
  NetworkPolicy enforced both directions (label-gated ingress,
  CIDR-listed egress).
- CI jobs: go-test (race), go-lint (golangci-lint, 0 issues),
  go-conformance (real binary vs fake Telegram, 8/8), go-image,
  go-kind (full cluster acceptance).

### Fixed
- Pre-existing flake in the Python plane: same-millisecond backups
  collided on the filename (`backup_db` failed with "output file
  already exists"); the destination now bumps a counter.

## [0.0.5] — 2026-10-21

Full hardening pass over both planes (the verified external review +
an internal 30-item audit), in six committed phases:

### Added
- Session supervisor (mtgateway): one task per session owns it for
  life — reconnects reuse the session's OWN lock (the old reconnect
  path fought it and permanently dropped the session), retryable
  failures back off exponentially, auth floods are slept out, and
  revoked/duplicated keys quarantine instead of burning the session.
- DB durability kit (gateway/dbhealth.py): PRAGMAs everywhere
  (busy_timeout, GW_DURABILITY, wal_autocheckpoint), boot quarantine
  + newest-backup restore for corrupt DBs, visible persistence
  failures (StorageHealth degrades /readyz), rotating VACUUM INTO
  backups + Fernet-encrypted .session backups, hourly maintenance
  (integrity_check, WAL truncate, audit retention).
- FLOOD_WAIT backoff ledger: chat pauses survive restarts.
- mtgateway getUpdates offset mode (opt-in, Bot API semantics —
  delivery never acks); atomic per-(alias,key) idempotency with
  pending/done (a failed send releases its key); dead-letter capture
  for poison updates with admin inspect/clear; GW_UPDATE_QUEUE_MAX /
  GW_UPDATE_TTL_S finally wired; pts/qts state snapshots.
- Idempotency-Key header on Bot API writes (spec E2).
- Liveness vs readiness: /healthz proves the event loop ticks
  (LoopBeat), /readyz adds a storage write probe + session picture;
  active get_me pings catch half-open TCP; dead-man switch metric.
- Metrics pack + monitoring/alerts.yml (12 rules) + docs/alerting.md
  runbook; pip-audit unsilenced in CI.
- Crash-loop safe mode (boot-rate tracker, admin ack), verified
  safe-restart scripts (fail unless all sessions return live),
  autoheal labels + watchdog in install.sh, stop_grace_period 30s,
  harder preflight (disk space, write probe), boot-time Fernet token
  verification, staggered restores.
- Schema versioning (PRAGMA user_version) + migration runner with
  pre-migration backups; monotonic file-backend fencing tokens
  (audited at session start); docs/runbook.md (failure → commands).

### Fixed
- See phases 0-2 for the full bug list (lock self-refusal, un-awaited
  releases, redis TTL in seconds-vs-ms, ack-on-delivery data loss,
  idempotency races, silent persistence failures, the lone-key
  idempotency PK collision between bots).

## [0.0.4] — 2026-10-09

First consumer-facing behavior on top of the hardened core: the gateway
now answers "is the bot alive?" visually, and honestly — the indicator
stops the moment the bot actually cannot respond.

### Added
- Restart-semantics tests + README section (closes #19): kill -9
  mid-FloodWait backoff is pinned end to end — the pending retry is
  the consumer's (guard pause is in-memory and recomputed on boot,
  Telegram re-throttles as the authority), queued updates are durable
  and duplicate-free across the crash (persisted consumer offset
  floors delivery against redelivery races), and the session lock
  reclaims a dead holder immediately (PID check, not the 60s
  heartbeat window).
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
