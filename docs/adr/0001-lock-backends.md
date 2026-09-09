# ADR-0001: Session lock backends — file (single-host) + Redis lease (multi-host)

Status: accepted (2026-09-09) · Charter: goals 1+2

## Context

The product's first invariant is ONE OWNER PER SESSION: no process
may call connect() on a session before proving exclusive ownership
(prevents AUTH_KEY_DUPLICATED, which invalidates auth_keys
permanently). Production today runs every sidecar on a single Docker
host; the farm roadmap (docs/farm-architecture.md) adds multi-host.

## Decision

The lease becomes an interface with two backends, selected by env:

- `GW_LOCK_BACKEND=file` (default) — today's `SessionLock`:
  O_CREAT|O_EXCL on the session dir, PID + heartbeat, stale reclaim,
  SIGTERM release. Zero dependencies. Correct across containers on
  ONE host (proven in production: 4 sessions, deploy handovers ~2s).
- `GW_LOCK_BACKEND=redis` (+ `GW_REDIS_URL`) — `RedisSessionLease`:
  SET NX PX (TTL 15s) + heartbeat extension every 5s + fencing token
  (INCR per acquisition, exposed for farm-mode request tagging).
  Correct across hosts. Crash release is automatic (TTL expiry),
  faster and safer than file stale-detection.

## State Flow (charter checklist)

- Holder killed (SIGKILL): file → heartbeat stops, stale after 15s,
  next acquire reclaims. Redis → TTL lapses ≤15s, key vanishes, next
  acquire succeeds. No owner ever connects without the lease.
- Two sidecars race: both backends are atomic primitives
  (O_EXCL / SET NX) — exactly one wins; the loser refuses BEFORE any
  Telegram contact.
- Holder frozen (process alive, loop stuck): file heartbeat is an
  asyncio task on the same loop — a frozen loop stops touching, lock
  goes stale, reclaimed. Redis heartbeat likewise; fencing token
  lets farm mode reject a thawed zombie's late writes.
- 10 workers: workers never lock — only the session-owning sidecar
  process does. Workers are stateless HTTP clients by design.

## This component is STATEFUL

It guards THE stateful resource (the auth_key connection). The lock
itself is ephemeral coordination state; losing it (crash) is safe —
the worst case is a ≤15s delay before the next owner, never a double
owner.

## Consequences

+ Single-host users keep the zero-dependency default, bit-for-bit.
+ Multi-host HA becomes configuration, not code.
− Redis backend adds a runtime dependency (lazy import — absent
  Redis does not affect the file backend) and one more thing to
  monitor (lease metrics exported).
