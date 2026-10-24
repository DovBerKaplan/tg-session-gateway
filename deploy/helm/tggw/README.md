# tggw Helm chart

The Telegram Session Gateway for Kubernetes: Bot API gateway + MTProto
sidecar as one static Go binary, with **zero client-service
dependencies** — SQLite on a PVC (no Postgres), file locks (no Redis),
plain HTTP + Prometheus text exposition (no operator, no CRDs).

## Install

```sh
helm install tggw deploy/helm/tggw \
  --namespace tg-gateway --create-namespace \
  --set secrets.fernetKey=$(docker run --rm ghcr.io/dovberkaplan/tg-session-gateway genkey) \
  --set secrets.apiID=12345 --set secrets.apiHash=...

# one-time namespace hardening (the pods already satisfy restricted PSA)
kubectl label --overwrite ns tg-gateway pod-security.kubernetes.io/enforce=restricted
```

`GW_ADMIN_SECRET`/`GW_APP_SECRET` are generated once by the chart and
reused on every upgrade (helm `lookup`). `GW_FERNET_KEY` is **never**
generated: rotating it makes every stored bot token undecryptable, so
you provide it explicitly.

> **Prefer `existingSecret` in shared/production clusters**: any value
> passed via `--set secrets.fernetKey` is stored in Helm's release
> history (release Secret in the namespace). The chart's Secret carries
> `helm.sh/resource-policy: keep`, so it survives `helm uninstall` —
> deleting it manually is fatal to stored tokens.

## PDB: maxUnavailable 0 means the operator must be present

The default PodDisruptionBudget blocks ALL voluntary evictions: a
`kubectl drain` waits forever (until `--timeout`), and managed node
upgrades (GKE/EKS auto-upgrades) stall or force-kill after their
grace. That is deliberate — a mid-traffic eviction risks duplicate
sends and, on the MTProto plane, AUTH_KEY_DUPLICATED — but know the
levers:

```sh
# allow the drain to proceed past the PDB
kubectl drain <node> --disable-eviction --ignore-daemonsets

# or flip the default off for auto-upgrading clusters
helm upgrade tggw deploy/helm/tggw -n tg-gateway --reuse-values \
  --set podDisruptionBudget.enabled=false
```

No Helm? `deploy/helm/install.yaml` is the same chart rendered flat —
replace the placeholder secrets in it and `kubectl apply -f`.

## Single ownership (three layers, no Redis)

1. **StatefulSet, replicas pinned to 1** — Kubernetes creates no
   replacement pod until the old one is confirmed dead.
2. **ReadWriteOncePod PVC** (default; set `accessMode: ReadWriteOnce`
   on clusters older than 1.29) — the volume cannot attach twice.
3. **In-process file lock** (`O_EXCL` + PID/hostname + heartbeat on the
   PVC) — a second process refuses *before* any Telegram connect(),
   which would trip `AUTH_KEY_DUPLICATED` and can invalidate the key.

## NetworkPolicy

Default-deny both directions. Ingress on 8080 only from namespaces
labeled `tg-gateway.io/client=true`:

```sh
kubectl label ns my-consumer-ns tg-gateway.io/client=true
```

Egress only to cluster DNS and the Telegram CIDRs
(`149.154.160.0/20`, `91.108.4.0/22` — configurable; api.telegram.org
resolves inside the first range).

## Values highlights

| Key | Default | Meaning |
|---|---|---|
| `botapi.enabled` | `true` | Bot API plane StatefulSet |
| `mtproto.enabled` | `true` | MTProto plane (set `false` for Bot API only) |
| `*.storage.accessMode` | `ReadWriteOncePod` | fallback `ReadWriteOnce` pre-1.29 |
| `podDisruptionBudget.maxUnavailable` | `0` | voluntary drains wait for the operator |
| `monitoring.serviceMonitor.enabled` | `false` | rendered only if the CRD exists |
| `resourceQuota` / `limitRange` | on | namespace is gateway-only (`pods: 2`) |

## Deviation from the legacy Python compose

The binary serves API and admin on **one port** (`GW_PORT`, 8080) with
different bearer secrets — same as the Python product. The chart
exposes that single port via a ClusterIP Service; the NetworkPolicy
plus `GW_ADMIN_SECRET` are the isolation layers. There is no separate
admin Service because the product has no separate admin listener.

## Cutover from the Python sidecar

1. `kubectl cp` the old `/data` aside (backup), then mount it read-only
   into a job and run `tggw migrate --from /data` (writes gotd session
   blobs into `mtgateway.db`; idempotent; `--dry-run` supported).
2. Point the StatefulSet at the same PVC.
3. Verify `Telegram logins required: 0` in the migrate output — that
   is the cutover gate (no re-auth, no AUTH_FLOOD risk).
