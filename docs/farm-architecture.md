# Farm Architecture — distributed multi-account orchestration (roadmap)

The structural gap: no mature open-source Python system manages a
multi-account Telegram farm against a task queue with exclusive
session ownership. This repo already has the foundation (single-owner
sessions, long-lived connections, per-peer rate guards, persistent
update queue). This document is the plan for the three bricks that
turn it into the missing piece — WITHOUT breaking what production
already runs on.

## Brick 1 — Lock abstraction: file (today) + Redis leases (multi-host)

`mtgateway/lock.py` is single-HOST: correct across containers on one
machine (proven: heartbeat, stale reclaim, SIGTERM release), wrong
across machines. The lease becomes an interface with two backends:

- `FileSessionLock` — default, zero-dependency, exactly today's code.
- `RedisSessionLease` — SET NX PX + heartbeat extension + fencing
  token (monotonic counter written with the lease; every Telegram
  operation carries it, late writers abort). Acquire BEFORE connect(),
  same invariant.

Selection: `GW_LOCK_BACKEND=file|redis` (+ `GW_REDIS_URL`). Single-node
deployments keep today's behavior bit-for-bit.

## Brick 2 — Actor-per-Account (farm mode)

Each account becomes an independent actor: one asyncio loop (one
PROCESS at scale — see single-loop note below) consuming from its
dedicated stream `stream:account:{id}`, pushing results to
`stream:results`. The HTTP façade stays as the synchronous surface;
farm mode adds:

- `POST /v1/farm/tasks` `{account, method, args, idempotency_key}` →
  enqueued (Redis Stream), returns task id
- `GET  /v1/farm/tasks/{id}` → result/status (pending|running|done|
  failed|rate_limited)
- Actor loop per account: XAUTOGROUP consume → execute through the
  same coerce+rate-guard path as today → ack → publish result
- Workers (stateless, any language) only push tasks — they never hold
  sessions, never touch Telegram

The single-loop assumption DIES here the right way: one process per
account-actor (or a small pool of accounts per process), each with its
own in-memory guards — horizontal scale without shared-bucket math.

## Brick 3 — Per-account / per-peer FloodWait queue freeze

Today: guards are in-process and correct. Farm mode externalizes the
PAUSE state: a real FLOOD_WAIT on account X (or peer Y) writes
`pause:{account}:{peer} = until_ts` in Redis; the actor loop for X
defers that queue's tasks (visibility timeout — tasks return to the
stream, no loss) while every other account keeps working. Metrics:
`tg_farm_queue_paused{account,peer}`, `tg_farm_task_wait_seconds`.

## Phasing (each phase: tests + CI + docs, production untouched)

| Phase | Delivers | Risk to running consumers |
|---|---|---|
| 0 | this document; lock interface extraction (file backend, no behavior change) | none |
| 1 | Redis lease backend + two-backend lock tests | none (opt-in env) |
| 2 | farm mode skeleton: task API + one actor + Redis streams | none (new surface, sidecar mode unchanged) |
| 3 | FloodWait queue freeze + farm metrics | none (farm mode only) |
| 4 | soak test: N test accounts, kill -9 chaos, zero AUTH_KEY_DUPLICATED | none (test env) |

## Non-goals (deliberate)

- Not "every MadelineProto method over HTTP" — the façade stays
  minimal (send/updates/files + farm tasks).
- Not a UI/panel — TAO-style management is a different product.
- Not replacing the file backend — single-node users pay no Redis tax.
