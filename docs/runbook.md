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

## User sessions: login, import, and the AUTH_KEY_DUPLICATED rule

User accounts (phone/code/2FA) live in the same MTProto plane as bots.
A user session that holds no authorization sits in `awaiting_login`
(metric `tg_gateway_session_awaiting_login`), holding its lock, dialing
nothing — the admin login flow drives it stepwise (the code and password
are never logged or audited):

```bash
curl -X POST .../v1/admin/sessions -d '{"alias":"acc1","kind":"user","conn_opts":"{\"ipv6\":\"2001:db8::2\"}"}'
curl -X POST .../v1/admin/sessions/acc1/login/start  -d '{"phone":"+972…"}'   # Telegram sends the code
curl -X POST .../v1/admin/sessions/acc1/login/code    -d '{"code":"…"}'        # 409 ⇒ 2FA:
curl -X POST .../v1/admin/sessions/acc1/login/password -d '{"password":"…"}'
```

No new login is needed when the account already has a session:

- **Telethon** — `scripts/telethon-session-export.py` turns the legacy
  `.session` SQLite into a StringSession; import it with
  `POST /v1/admin/sessions/{alias}/import {"telethon_string":"1…"}`.
- **gotd** (cine-scanner migration) — the scanner's sealed session is
  gotd-format JSON; import with `{"gotd_session_b64":"…"}`.

**The rule:** one auth key may be live in exactly one process. The old
holder (legacy Telethon process, scanner pod) MUST be stopped before
import — otherwise Telegram answers `AUTH_KEY_DUPLICATED` to both and
may burn the key. Import refuses to overwrite a stored session unless
`force:true`; `force` without stopping the old holder is how keys die.

`conn_opts` carries the account's anti-ban profile: `ipv6` (source
address bound for tcp6-only dials — no fallback) and `device`
(model/system_version/app_version) must match what the account used
before, or the fingerprint changes mid-life.

## Dead user session → re-login

A deactivated/revoked account (`USER_DEACTIVATED`, `USER_DEACTIVATED_BAN`,
`SESSION_REVOKED`, …) quarantines the session — retrying burns nothing,
by design. When the account is available again (un-banned, re-activated),
the SAME alias recovers with a fresh login: quarantine clears and the
supervisor reconnects automatically once `login/code` (or `password`)
succeeds. No unregister, no data loss:

```bash
curl -X POST .../v1/admin/sessions/acc1/login/start  -d '{"phone":"+972…"}'
curl -X POST .../v1/admin/sessions/acc1/login/code    -d '{"code":"…"}'
# 409 ⇒ 2FA: POST …/login/password {"password":"…"}
```

Alerts: `TGSessionQuarantined` (dead), `TGUserSessionAwaitingLogin`
(login pending >30m), `TGBulkLaneBacklog` (scan backlog, info).

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

**Status (2026-10-25): steps 0 and 1 PASSED IN PRODUCTION** — the
botapi plane runs v0.1.0-rc.1 against real traffic (offset continuity
860291301→860291344 across the restart, depth 0, zero errors/429s);
the MTProto plane stays on Python 0.0.5 until steps 2-5 pass for it.
Two production notes: volumes created by the PYTHON image belong to
UID 10001 with 0600 files — run the Go container as `user:
"10001:10001"` or chown to 65532 BEFORE first boot (the distroless
default is nonroot 65532 and it crashes on unreadable files); and a
partial cutover (Go botapi + Python sidecar on SEPARATE data dirs)
is valid — flock only guards same-volume double-ownership.

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

## Encryption at rest (0.1.0+)

Bot tokens AND gotd session blobs (which contain the auth_key) are
sealed with the volume's `GW_FERNET_KEY` — tokens as bare fernet
tokens (python-compatible), session blobs with a versioned `tggw1:`
envelope. Files: `gateway.db`, `mtgateway.db`, `sessions/*.db` are
chmod 0600 on open; the `.fernet_key` file is 0600.

- **Rotating GW_FERNET_KEY is data loss** for an existing volume
  unless you re-seal first: back up `/data`, run
  `tggw migrate --from /data` with the OLD key (re-seals everything),
  then switch the key. Without that step the next start fails loudly
  ("sealed blob failed to decrypt") instead of silently corrupting.
- Legacy pre-0.1.0 volumes load plaintext session blobs transparently
  and re-seal on the next write; `tggw migrate --from /data` re-seals
  everything immediately (the report's `reencrypted` count).
- Volume permissions: mount the data volume owned by the container
  user (65532). The image initializes `/data` with nonroot ownership;
  bind mounts must be pre-chowned (`chown 65532:65532 ./data`).

## Auto environment detection

Optional companion failover and TLS-aware upstream selection. Controlled
by `GW_ENV_DETECT=off` (default in Compose) or `auto`.

**CLI (read-only probe):**

```bash
GW_ENV_DETECT=auto GW_UPSTREAM_FALLBACK=http://telegram-bot-api:8081 \
  tggw doctor          # human summary
tggw doctor --json     # same facts + decision as JSON
```

**What gets probed:** cloud upstream TLS (full verification, optional
extra roots from `GW_UPSTREAM_CA_FILE`), certificate expiry vs local
clock skew, DNS (A/AAAA) for the upstream host, `HTTP(S)_PROXY` /
`NO_PROXY`, runtime hint (Docker / Kubernetes / bare metal), and
companion health at `GW_UPSTREAM_FALLBACK` (`GET …/healthz`) when that
URL is set.

**Hard rules (not configurable):**

- TLS certificate verification is **never** disabled — no
  `InsecureSkipVerify`, no “trust the inspection proxy” auto-import.
- Unknown corporate CA → set `GW_UPSTREAM_CA_FILE` to a PEM bundle.
- Automatic failover uses **only** a preconfigured
  `GW_UPSTREAM_FALLBACK` URL (e.g. internal `telegram-bot-api`); the
  gateway will not invent a companion address.

**Admin visibility (Bearer `GW_ADMIN_SECRET`):**

```bash
curl -s -H "Authorization: Bearer $GW_ADMIN_SECRET" localhost:8080/admin/env | jq
curl -s -H "Authorization: Bearer $GW_ADMIN_SECRET" localhost:8080/admin/upstream | jq
# manual switch (when supervisor enabled):
curl -X POST -H "Authorization: Bearer $GW_ADMIN_SECRET" localhost:8080/admin/upstream/failover
curl -X POST -H "Authorization: Bearer $GW_ADMIN_SECRET" localhost:8080/admin/upstream/failback
```

`/admin/env` returns probe facts, the decision engine output, TLS state,
and clock skew. `/admin/status` also embeds an `upstream` block when
failover is active.

**Alerts** ([`monitoring/alerts.yml`](../monitoring/alerts.yml)):

- `UpstreamFailoverActive` — traffic is on the local companion
  (`tg_gateway_upstream_failover_active == 1` for >2m). Confirm cloud
  TLS/path health; fail back when cloud is healthy
  (`POST /admin/upstream/failback` or fix `GW_UPSTREAM_CA_FILE`).
- `EnvDegraded` — cloud TLS probe is not `valid` but failover is **not**
  active for >5m. Typical fix: enable companion + `GW_UPSTREAM_FALLBACK`,
  or add `GW_UPSTREAM_CA_FILE`; never disable verify.

**Override envdetect with explicit `GW_*`:** anything you set in the
environment wins over recommendations (doctor prints which fields were
explicit):

| Decision | Override |
|---|---|
| Detection on/off | `GW_ENV_DETECT=off` \| `auto` |
| Primary upstream | `GW_UPSTREAM` |
| Companion URL | `GW_UPSTREAM_FALLBACK` |
| Failover policy | `GW_UPSTREAM_FAILOVER=off` \| `auto` \| `prefer_local` |
| Treat as local (multipart/time presets) | `GW_UPSTREAM_KIND=cloud` \| `local` \| `auto` |
| Extra trust anchors | `GW_UPSTREAM_CA_FILE` |
| Upstream HTTP timeout | `GW_UPSTREAM_TIMEOUT_S` |
| Large uploads | `GW_MAX_MULTIPART_MB` |
| Consumer URL prefix | `GW_PATH_PREFIX` (`none` = bare `/bot…`) |

Compose helper: [`scripts/ensure-local-botapi.sh`](../scripts/ensure-local-botapi.sh)
sets fallback + `GW_ENV_DETECT=auto` and starts the `local-botapi` profile.
Helm: `localBotApi.mode` `standby` \| `always` — see
[`deploy/helm/tggw/README.md`](../deploy/helm/tggw/README.md).
