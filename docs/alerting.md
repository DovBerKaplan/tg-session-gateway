# Alerting

Prometheus rules live in [`monitoring/alerts.yml`](../monitoring/alerts.yml)
— identical metric names on both planes (`/metrics` on each service;
plane label `bot` / `mt` on the shared gauges). Point an existing
Prometheus at both endpoints and load the rule file. No built-in push
channel by design: the gateway stays dependency-free, alerts flow
through whatever Alertmanager you already run.

Each rule's `runbook` anchor below is the first response.

## session-not-live

`tg_gateway_session_state == 0` for >5m.

1. `GET /readyz` — session states + storage write probe.
2. `GET /v1/admin/status` (mt) / `GET /admin/status` (bot) — read
   `last_error` per session.
3. Supervisor restarts with backoff on its own; if it says `lock
   refused`, find the holder: `cat /data/sessions/<alias>.lock`
   (pid@host) — a same-host dead PID reclaims automatically.
4. Escalation rung 1: `POST /v1/admin/sessions/<alias>/restart`.

## session-quarantined

Terminal error (revoked/duplicated key). The supervisor stopped
retrying on purpose — retrying burns sessions. Re-auth the bot and
re-register (`DELETE /v1/admin/sessions/<alias>` then register).

## no-updates

Dead-man switch: connected but nothing received for 30m+. Check the
liveness ping (`tg_gateway_seconds_since_ping_ok`); if the ping is
fine the bot may simply be idle — tune the threshold per bot traffic.

## ping-stuck

Half-open TCP suspicion: the supervisor already forced a reconnect.
Persistent stuck pings → check network/NAT between the host and
Telegram DCs.

## storage-degraded / persist-failures

`readyz` is 503 while degraded. Causes: disk full (check `df`), DB
locked, or a quarantined DB (see the `.corrupt-*` file and the newest
backup restored into place). Persistence failures mean **unacked
updates now die with the process** — fix fast.

## corrupt-rows

Rows skipped during queue load (one bad row no longer skips the whole
backlog). Inspect `queue.db` / `mtgateway.db` with `sqlite3` and the
`.corrupt-*` forensics copy if present.

## queue-depth / queue-age

Consumer dead or slow. Pull mode: is the consumer polling? Push mode:
`push_healthy` in admin status. The queue TTL (default 1h) bounds the
damage; `drop_oldest` keeps the newest.

## flood-storm / reconnect-flapping

Sending pattern violates Telegram limits repeatedly or the session
can't hold a connection. Inspect `last_flood_wait` in admin status;
consider `pause_sending` while investigating.

## loop-wedged

Event-loop lag >5s: something blocks the loop (sync I/O, CPU spike).
Grab `py-spy dump --pid <pid>` before restarting.
