# Scaling — running many bots across many instances

One gateway instance is single-owner by design (ADR-0003: flock +
one PVC per bot set). It is also SMALL: ~1m CPU and ~7Mi RSS resting
([performance.md](performance.md)), so a single instance comfortably
carries dozens of bots — measure before you shard.

## When to shard

| Signal | Action |
|---|---|
| One bot's traffic saturates the global rate budget (30 msg/s default, tunable) | shard the hot bot onto its own instance |
| Organizational isolation (per-team, per-customer fleets) | shard per owner |
| Very large fleets (100+ bots) | shard by bot ranges; 10-50 bots per instance is a sane band |

## The model: N instances, disjoint bot sets

```
              ┌─ instance A (shard-a): bots 1..20   → PVC A
router/mgmt ──┼─ instance B (shard-b): bots 21..40  → PVC B
              └─ instance C (shard-c): bots 41..60  → PVC C
```

- Each instance = one Helm release (or one StatefulSet pair) with its
  own `GW_DATA_DIR` volume and `shardName`.
- A bot token lives on EXACTLY ONE instance — the flock refuses a
  second live holder, and the refusal is the invariant working, not a
  bug (AUTH_KEY_DUPLICATED otherwise).
- Consumers address shards by base URL (or the alias-scoped
  `/bots/{alias}/` path behind your own router).

## Helm

```yaml
shardName: shard-a
podAntiAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    - labelSelector:
        matchExpressions:
          - key: app.kubernetes.io/name
            operator: In
            values: [tggw]
      topologyKey: kubernetes.io/hostname
```

`shardName` labels every pod (`tggw/shard=shard-a`) for routing and
ops; the anti-affinity keeps instances off each other's machines.
Replicas stay 1 per instance by invariant — scale by adding INSTANCES,
not replicas.

## Shared coordination (roadmap, 0.2)

Redis/etcd leases would let instances negotiate bot ownership
dynamically and share rate-guard budgets across processes. NOT in
0.1.x: it changes the single-owner invariant and adds a hard
dependency for a problem most fleets do not have. The sharding model
above is the supported path today; see the superseded
[farm-architecture.md](farm-architecture.md) for the original lease
design.
