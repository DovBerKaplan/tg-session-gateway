# Performance — measured numbers, and how to reproduce them

Every number below is from the black-box suites in
`tests/conformance_go/` running the REAL `tggw` binary against an
instant fake upstream, on the development machine (Linux 6.17, Go
1.25). The fake answers immediately, so what these measure is the
GATEWAY's added cost — in production, Telegram's own network RTT
(50-300ms) dominates every real send.

## Latency (the 1ms-class target)

| Path | p50 | p99 | What the path includes |
|---|---|---|---|
| Direct-to-fake baseline (no gateway) | 4.1ms | — | the Python measurement client itself: ~4ms of client cost |
| **send** through the gateway | 6.4ms | 27.6ms | HTTP in → auth → rate guard → typing bookkeeping → upstream forward → out. **Gateway's own cost: ~2.3ms** |
| **pull with offset ack** | 5.5ms | 14.6ms | auth + ack write (SQLite) + queue scan + response |
| **empty poll** | ~17ms mean | — | includes the 10ms no-data debounce BY DESIGN (`internalGetUpdates` clamps `timeout<=0` to 10ms) |
| **delivery** (update injected → poller → queue → parked long-poll wakes) | 7.1ms | 20.4ms | end-to-end push-into-queue-to-consumer |

Rate-guard policy is NOT performance: the default buckets are
1 msg/s per private chat and 30 msg/s per bot (the whole-bot global
write bucket). Benchmarks raise them via env (`GW_*_RATE`/`GW_*_BURST`)
so the limiter never gates the measurement. In production those
buckets are the feature, not overhead.

## Resource footprint (kind, resting)

- ~1m CPU, ~7Mi RSS per plane pod (distroless static binary).
- One process carries many bots: per-bot state is a small struct,
  one goroutine per poller; session files are per-alias SQLite
  databases on the volume.

## Reproduce

```bash
CGO_ENABLED=0 go build -o /tmp/tggw ./cmd/tggw
TGGW_BIN=/tmp/tggw .venv/bin/python -m pytest -q tests/conformance_go/test_latency.py -s
TGGW_BIN=/tmp/tggw .venv/bin/python -m pytest -q tests/conformance_go/test_load.py -s
```

The `-s` output prints the percentile table per path. The suites double
as regression gates: gross latency regressions (broken pooling, fsync
on the hot path, limiter misconfig) fail the build.

## Load behavior

`test_load.py` proves, black-box against the real binary:

- 240 sends across 8 chats by 8 concurrent workers: zero errors,
  exactly-once upstream delivery, ~140 sends/s sustained through the
  full gateway path (with policy buckets raised).
- Concurrent consumers lose nothing (at-least-once across racing
  cursors; exactly-once for a single advancing cursor).
- One `Idempotency-Key` under 6 concurrent sends reaches upstream
  exactly once.
- SIGKILL + restart: acked offsets survive (no redelivery), sends
  resume immediately.
