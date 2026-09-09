# Hardening Charter — the working contract (v0.1.0)

Five disciplines, adopted. Each maps to a concrete Definition of Done
for this repo; claims stay honest until their test exists.

1. ARCHITECTURE FIRST — every new component starts as a one-page ADR
   (docs/adr/NNN-*.md): Stateful or Stateless; State Flow for TCP
   disconnect / kill -9 mid-operation / timeout; and "what happens if
   10 workers hit this resource simultaneously".
2. SINGLE WRITER PER RESOURCE — no shared local DBs across containers.
   SQLite stays single-writer-per-process (the sidecar IS the writer).
   Locks: file backend for single-host (documented ADR), Redis lease
   (TTL + heartbeat + fencing token) for multi-host.
3. HARDENING IS THE DEFAULT — exponential backoff WITH JITTER on every
   retry loop; rate errors isolate the offending peer/account only;
   Definition of Done includes a chaos test (kill mid-write, clean
   recovery, zero data loss) — not just green units.
4. NIH RESTRAINT — before any new infra wrapper: survey ≥2 existing
   OSS solutions (pros/cons documented) and re-read the platform spec
   (MTProto auth_key vs tmp_sessions, expiry semantics).
5. POLISH & FREEZE — 30 days, no new POCs. This repo reaches v0.1.0
   "production hardened": every known red/amber closed, soak green,
   monitoring documented, zero manual intervention required.

## v0.1.0 Definition of Done (tracked here)

- [x] Comparison survey of existing solutions (docs/comparison.md)
- [x] Per-peer rate isolation, tested
- [x] Lock-before-connect, tested two-process
- [x] Auth-flood retry discipline (wait + margin, capped attempts)
- [x] Persistent update queue (SQLite WAL, restart replay, tested)
- [x] Idempotency keys persisted (survive kill -9)
- [x] Jitter on every backoff/retry loop (additive-only, never below the required wait — tested)
- [x] Chaos soak in CI: 3 accounts, kill -9 with stranded locks,
      restart — zero AUTH_KEY_DUPLICATED, zero lost updates,
      exactly-once replay, ack releases (tests/test_chaos_soak.py)
- [x] Redis lease backend behind the lock interface (ADR-0001, fencing tokens, fakeredis-tested)
- [x] Monitoring runbook (docs/monitoring.md — catalog, alerts, on-call)
