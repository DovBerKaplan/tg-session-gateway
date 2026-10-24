# ADR-0003: Session locking moves to flock; Redis lease backend removed

Status: accepted (2026-10-24) · Supersedes: [ADR-0001](0001-lock-backends.md)

## Context

ADR-0001 defined the session lock as an interface with two backends:
file (`O_CREAT|O_EXCL` + PID + heartbeat) and an optional Redis lease.
The file backend's staleness rules relied on PID liveness — correct on
a single Docker host, wrong in Kubernetes:

- Every StatefulSet pod is **PID 1** with a **stable hostname**
  (`tggw-mtproto-0`). After a crash, the successor reads the old
  lock, `kill(1, 0)` succeeds against ITSELF, and the only remaining
  signal is the 60s heartbeat window — up to ~2 minutes of refused
  reconnects after every crash (60s staleness + 60s retry cooldown).
- Unlinking a lock file races an in-flight `open`+`O_EXCL` pair into
  two winners on two inodes — the janitor that "cleaned" stale locks
  could itself break single ownership.
- The Redis backend existed only for multi-host, which Kubernetes
  already prevents at the storage layer (ReadWriteOncePod PVC), and it
  dragged a whole service into the deployment.

## Decision

1. **flock is the lock.** `Acquire` opens the file (O_CREATE|O_RDWR)
   and takes `flock(LOCK_EX|LOCK_NB)`; the kernel releases it the
   moment the holder dies — no PID probing, no heartbeat window, no
   PID-namespace confusion. Crash → instant reacquire.
2. **Lock files are NEVER deleted.** flock binds to the inode:
   unlinking while held lets another process create and lock a fresh
   inode (two owners). `Release` closes the fd and leaves a 0-byte
   file; `SweepDir` is deleted outright.
3. **The file's content is diagnostics-only** (pid/host/boot_id/ts
   plus a `tggw` 5th-line marker) and never gates a decision.
   `StartHeartbeat` is a no-op kept for API compatibility;
   `tg_gateway_lock_reclaims_total` stays exported, pinned at 0, so
   existing dashboards keep scraping.
4. **Redis is gone.** Ownership layers: StatefulSet `replicas: 1` →
   ReadWriteOncePod PVC → this flock. NFS caveat: flock is
   per-client, which RWOP makes moot (single attachment).
5. **Transitional Python guard (0.1.x only):** the Python sidecar's
   lock was O_EXCL-based and INVISIBLE to flock. If `Acquire` finds a
   4-line Python-format holder file with a fresh (<60s) timestamp, it
   refuses — a live Python sidecar probably owns the volume. The Go
   format carries the `tggw` marker line so our own released locks
   never trip the guard. Removed after 0.1.x soaks.

## Consequences

- Crash recovery on the MTProto plane drops from up-to-2-minutes to
  immediate (`TestHolderDeathReleasesInstantly` pins it with a
  SIGKILLed subprocess).
- Cutover rule (runbook §soak-0): stop the Python sidecar before the
  Go binary mounts the same `/data`; the guard is a net, not a
  substitute.
- ADR-0001's interface/backends design is obsolete; this ADR replaces
  it. The historical file-lock semantics remain implemented (and
  documented) on the `legacy-python` branch.
