# Runbook — failure → symptom → exact commands

Every scenario lists the API surfaces for both planes:
`BOT` = Bot API gateway (`:8080`, `/admin/*`, Bearer GW_ADMIN_SECRET)
`MT`  = MTProto sidecar   (`:8081`, `/v1/admin/*`, same secret).

Docker paths assume the compose volumes (`tg-gateway-data` /
`tg-mtgateway-data`). Alert rules: [`monitoring/alerts.yml`](../monitoring/alerts.yml);
alert-level runbook: [`docs/alerting.md`](alerting.md).

> **Go build (`tggw`)**: the same scenarios apply — same routes, same
> secrets, same metric names. On Kubernetes the platform replaces the
> Docker mechanics: probes replace autoheal, `preStop` replaces
> `safe-restart.sh`, and the admin surface sits on `GW_ADMIN_PORT`
> (8081, NetworkPolicy-blocked — `kubectl port-forward`). See
> [`deploy/helm/tggw/README.md`](../deploy/helm/tggw/README.md).

## Session stuck in `error`

```bash
curl -s -H "Authorization: Bearer $GW_ADMIN_SECRET" localhost:8081/v1/admin/status | jq
# → look at last_error for the alias
```

- `lock refused: another process holds` → find the holder:
  `docker exec tg-mtgateway cat /data/sessions/<alias>.lock`
  (line 1 = pid, line 2 = host). A same-host dead pid is reclaimed
  automatically; a live foreign holder means a second sidecar is
  running — stop THAT, then:
  `curl -X POST -H "Authorization: Bearer $GW_ADMIN_SECRET" localhost:8081/v1/admin/sessions/<alias>/restart`
- `auth flood: retrying in Ns` → wait; the supervisor sleeps it out.
- `terminal (quarantined)` → the key is revoked/duplicated. Re-auth:
  `DELETE /v1/admin/sessions/<alias>` then re-register with a fresh
  token. A restart endpoint will refuse (by design).

## Session quarantined (revoked/duplicated key)

See above — `restart` returns 409 on purpose: retrying burns sessions.

## Corrupt DB

Symptom: boot log `DB corrupt (...) — quarantined to <path>.corrupt-*`,
`readyz` 503 with `storage.degraded_reason`, metric
`tg_gateway_quarantined_files_total`.

The newest healthy backup was already restored automatically. Verify:

```bash
docker exec tg-gateway ls /data/backups            # rotated VACUUM INTO snapshots
docker exec tg-gateway ls /data/*.corrupt-*        # forensics copy
```

Data written after the last backup is lost — the alert
(`TGStorageDegraded`) exists so you fix disk issues before this
happens. To start completely fresh: stop the container, move ALL
`*.db*` aside, start.

## Stuck lock

```bash
docker exec tg-mtgateway ls -la /data/sessions/*.lock
docker exec tg-mtgateway cat /data/sessions/<alias>.lock   # pid / host / boot_id
```

- Holder on another host → wait for the 60s heartbeat window or clear
  it ON THE HOLDER'S HOST.
- Holder dead on this host → the next acquire (or the boot janitor)
  reclaims it; to force: delete the file after verifying the pid is
  gone (`ps -p <pid>`).

## Disk full

`readyz` write-probe fails (503), `TGStorageDegraded` fires,
`persist_failures_total` climbs — unacked updates now live in memory
ONLY. Free space (`docker system prune`, move backups), no restart
needed: persistence recovers on the next batch.

## Crash-loop safe mode

`readyz` returns `{"safe_mode": true}` — nothing auto-connects (this
prevents an AUTH_KEY_DUPLICATED storm from overlapping restarts).
Docker/autoheal will keep restarting the container; that is expected
until you ack:

```bash
# fix the underlying crash FIRST (logs: docker logs tg-mtgateway), then:
curl -X POST -H "Authorization: Bearer $GW_ADMIN_SECRET" localhost:8081/v1/admin/safe-mode/ack
```

## Wrong GW_FERNET_KEY

Boot log names the exact aliases that cannot decrypt. Either restore
the original key (it is in the `.env` of the install dir and in
encrypted `.session` backups) or accept re-registering every bot.

## Poison updates (dead letters)

```bash
curl -s -H "Authorization: Bearer $GW_ADMIN_SECRET" \
  localhost:8081/v1/admin/sessions/<alias>/deadletters | jq
# clear after triage:
curl -X DELETE -H "Authorization: Bearer $GW_ADMIN_SECRET" \
  localhost:8081/v1/admin/sessions/<alias>/deadletters
```

## Dead/slow consumer (queue grows)

Pull mode: is the consumer polling? Push mode: `push_healthy` in
admin status; queue TTL (default 1h) bounds the damage. For a
permanently dead consumer, `drain` + fix the app, or lower
`GW_UPDATE_TTL_S`.

## Escalation ladder (cheapest first)

1. Reconnect ONE session: `POST .../sessions/<alias>/restart` (both
   planes where applicable).
2. Verified process restart (Compose): `docker restart tggw` and watch
   `/admin/status` until `live` — the flock + safe-mode machinery makes
   this safe. On Kubernetes `kubectl rollout restart statefulset/...`
   (preStop drains first).
3. Container/host restart — safe mode guards the boot storm.

## Soak gate (before flipping 1.0 defaults) — the cutover checklist

The Go build is functionally complete and kind-verified; this list is
what production must confirm BEFORE the README/image defaults stop
calling it a soak:

0. **STOP the Python sidecar before the Go binary mounts the same
   `/data`.** The transitional lock guard (a fresh Python-format lock
   file makes `Acquire` refuse) is a safety net, NOT a substitute —
   flock cannot see the Python sidecar's O_EXCL lock, and two live
   owners of one auth_key risk AUTH_KEY_DUPLICATED.
1. `tggw migrate --dry-run --from <copy of real /data>` ⇒
   `Telegram logins required: 0` (any non-zero is a stop).
2. Extract the REAL method inventory from the consumer's production
   logs (`POST /v1/sessions/...` lines) and diff against
   `tggw`'s allowlist (`501` responses are the loud miss).
3. `v0.1.0-rc.N` side-by-side against a test account: updates flow,
   send / getUpdates / offset-ack behave, WS push stays connected for
   hours.
4. Re-record the golden updates from real consumer traffic
   (`tests/conformance_go/record_golden_mt.py` records from the
   legacy sidecar; the production re-record replaces the corpus) and
   diff against `go test ./internal/mtproto/ -run Golden`.
5. `kubectl top pod` idle: < 20m CPU / < 32Mi RAM sustained.
6. Only after all green: tag v1.0.0, move `latest`, update the README
   quickstart to the promoted tags.

## Restoring an encrypted session backup

Backups: `/data/backups/sessions-<ts>.zip.enc` (Fernet, key =
GW_FERNET_KEY from the install `.env`).

```bash
docker run --rm -v tg-mtgateway-data:/data -v $PWD:/out \
  --entrypoint python3 tg-mtgateway:latest - <<'EOF'
import os, io, zipfile, sys
from cryptography.fernet import Fernet
key = os.environ["GW_FERNET_KEY"].encode()
blob = Fernet(key).decrypt(open("/data/backups/<FILE>.zip.enc","rb").read())
with zipfile.ZipFile(io.BytesIO(blob)) as z: z.extractall("/data/sessions")
EOF
# then a verified restart (stop the gateway first — never two owners)
```
