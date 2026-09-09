# Monitoring Runbook — v0.1.0 contract item

Both products export Prometheus text at `/metrics` (sidecar:
`:8081/metrics` loopback; Bot API gateway: `:8080/metrics`).

## Metric catalog

### MTProto sidecar (per `alias` label)

| Metric | Meaning | Healthy |
|---|---|---|
| `tg_gateway_session_state` | 1 = live | 1 |
| `tg_gateway_update_queue_depth` | unacked updates queued | near 0; sustained growth = stuck consumer |
| `tg_gateway_real_flood_wait_total` | REAL Telegram rate errors | **0** — growth means buckets mis-tuned |
| `tg_gateway_synthetic_429_total` | our own guard refused | low; spikes = a consumer flooding |
| `tg_gateway_consumers` | attached WS consumers | ≥ 1 in push mode |
| `tg_gateway_send_paused` | incident brake on | 0 |
| `tg_gateway_deploy_to_first_message_seconds` | E7 SLO | stable, not growing |

### Bot API gateway (per `alias`)

| Metric | Meaning | Healthy |
|---|---|---|
| `tg_gateway_session_state` | 1 = live poller | 1 |
| `tg_gateway_update_queue_depth` / dropped / expired | queue health | near 0 |
| `tg_gateway_queue_persistent` | SQLite WAL active | 1 |
| `tg_gateway_real_429_total` | real Telegram 429s | **0** |
| `tg_gateway_synthetic_429_total` | guard-triggered | low |

## Alert rules (Prometheus)

```yaml
groups:
  - name: tg-session-gateway
    rules:
      - alert: TGSessionDown
        expr: tg_gateway_session_state == 0
        for: 2m
        labels: {severity: critical}
        annotations:
          summary: "session {{ $labels.alias }} not live for 2m"
      - alert: TGRealRateErrors
        expr: increase(tg_gateway_real_flood_wait_total[5m]) > 0
          or increase(tg_gateway_real_429_total[5m]) > 0
        labels: {severity: warning}
        annotations:
          summary: "REAL Telegram rate errors — buckets are admitting too much"
      - alert: TGQueueStuck
        expr: deriv(tg_gateway_update_queue_depth[10m]) > 0
          and tg_gateway_update_queue_depth > 100
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: "unacked updates growing — consumer down or slow"
      - alert: TGSendPaused
        expr: tg_gateway_send_paused == 1
        labels: {severity: critical}
        annotations:
          summary: "incident brake is ON — someone paused sends"
```

## On-call actions

- **TGSessionDown**: `GET /v1/admin/status` → `last_error`. Auth flood →
  the auto-retry counts it down (state stays `connecting`, error shows
  the remaining wait) — do NOT re-register manually (a live session's
  re-register is a no-op; a failed one retries by itself).
- **TGRealRateErrors**: check which peer (`last_flood_wait` in admin
  status). One hot chat = consumer behavior; all peers = lower the
  bucket rates via env and restart.
- **TGQueueStuck**: consumer dead? `GET .../getUpdates` manually to
  confirm delivery; check consumer logs. The queue is bounded — it
  drops oldest past `GW_UPDATE_QUEUE_MAX`, never OOMs.
- **TGSendPaused**: deliberate incident brake — `POST resume_sending`
  only after understanding who paused and why.

## Lease metrics (Redis backend, farm/multi-host)

Lease acquire/refuse/refuse-stale counters per alias export as
`tg_gateway_lease_{acquired,refused,reclaimed}_total` — a growing
`refused` with no deploys = something else is trying to own the
session; investigate before it becomes AUTH_KEY_DUPLICATED.
