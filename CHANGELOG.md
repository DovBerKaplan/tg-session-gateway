# Changelog

All notable changes to this project will be documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/),
adherence to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Removed
- `mtproto/` directory (session persistence monkey-patch) — it was the
  "rejected design" from the spec; `mtgateway/` is the correct architecture.
  See `docs/gap-analysis-report.md` §2.2 and `docs/design-decisions.md`.

### Added
- Singleton session locks (`mtgateway/lock.py`) — advisory file lock acquired
  BEFORE Pyrogram connect(); prevents AUTH_KEY_DUPLICATED at the process level.
- Stale lock detection (PID check + heartbeat timestamp).
- `SECURITY.md` — threat model, session file risks, reporting policy.
- `CONTRIBUTING.md` — code style, commit conventions, testing requirements.
- `docs/compatibility.md` — Pyrogram/pyrofork/Telethon support matrix.
- `docs/roadmap-10-of-10.md` — comprehensive improvement plan with gap
  analysis findings merged.
- `docs/gap-analysis-report.md` — external review (preserved for reference).
- Idempotency keys (E2) — `_idempotency_key` in outbound requests.
- Pause sending / keep receiving (E4) — `pause_sending`/`resume_sending` admin endpoints.
- Deploy-to-first-message SLO metric (E7) — tracked per-session, in Prometheus.

## [0.1.0] — 2026-09-08

### Added
- MTProto Session Gateway (`mtgateway/`) — long-lived Pyrogram sidecar with
  HTTP API, WebSocket push updates, FAQ rate guard, FLOOD_WAIT parsing with
  adaptive backoff, session persistence on volume, lifecycle isolation.
- Bot API Gateway (`gateway/`) — drop-in `api.telegram.org` replacement with
  poller ownership, update queue, rate guard, token-free alias routes.
- Formally specified both products (docs/spec.md, docs/sidecar-spec.md).
- 72 tests across rate guard, session management, proxy, and store.

## [0.0.1] — 2026-09-08

### Added
- Initial proof of concept: Bot API gateway with rate guard and update queue.
