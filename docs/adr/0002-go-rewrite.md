# ADR-0002: Rewrite both planes in Go as one static binary (`tggw`)

Status: accepted (2026-10-23) · Supersedes: none · Affects: both planes

## Context

v0.0.5 ships two Python products (~5,900 lines: FastAPI + aiosqlite,
plus pyrofork in the sidecar). They are correct and hardened, but:

- **Resource weight**: 80–150MB RSS per container. On small nodes and
  in dense Kubernetes clusters that is the cost floor, not the load.
- **Not Kubernetes-native**: Docker Compose + autoheal + shell restart
  scripts stand in for probes, preStop drains and StatefulSet
  semantics.
- **Redis for locking** (optional lease backend) pulls a second
  service into the deployment for something the platform itself can
  guarantee.
- The load is capped by Telegram's own rate limits (~30 msg/s per
  bot), so raw language speed is irrelevant — the wins available are
  footprint, startup, packaging and operational surface.

Meanwhile the hard requirement is behavioral parity: existing
consumers parse the exact JSON the Python plane emits, and existing
`gateway.db`/`queue.db`/`mtgateway.db` files must open **in place**.

## Decision

Rewrite both planes in Go 1.25 (gotd/td for MTProto,
modernc.org/sqlite pure-Go for storage) as a single static binary:

```
tggw botapi | mtproto | genkey | migrate | healthcheck | terminate
```

- Same `GW_*` env contract, same HTTP routes, same metric names.
- Redis is removed entirely: single ownership is guaranteed by
  StatefulSet `replicas: 1` + `ReadWriteOncePod` PVC + the ported file
  lock (ADR-0001's file backend) — three layers before any connect().
- MTProto method surface becomes an explicit adapter allowlist;
  unknown methods return 501 with the supported list. The Python
  getattr-everything behavior is intentionally NOT promised.
- Session migration without re-login: `tggw migrate` converts pyrofork
  `.session` files to gotd session blobs; `Telegram logins required: 0`
  is the cutover gate.
- The Python code stays on main until the Go build soaks in
  production; then it moves to `legacy-python`.

## Consequences

- **Measured** (kind, v1.31): idle 1m CPU / 7Mi RAM per pod;
  distroless image 21.9MB; pod-kill recovery ~4s with exactly-once
  replay; NetworkPolicy enforcement verified both directions.
- **Compatibility is pinned by tests, not hope**: golden update files
  recorded from the real Python sidecar, a black-box conformance suite
  against the real binary, and Python-created SQLite fixtures opened
  in place by the Go store.
- Go does not eliminate races by itself — `go test -race` in CI does
  the actual work (it caught three real bugs during the port).
- The one Python behavior we could not copy: the installed pyrofork
  has no `to_dict` on raw TL objects, so the Python sidecar's raw
  update path silently drops real Telegram updates. The Go serializer
  delivers them (Pyrogram dict shapes, pinned by goldens) — stricter
  parity than the original.
- Open until cutover: soak against real Telegram (the MTProto plane
  has never talked to a live server in tests), and re-recording the
  goldens from production traffic once it exists.
