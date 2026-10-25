# Multi-account farms — SUPERSEDED

The Redis-lease design here predates the Go rewrite. Since
ADR-0003 (flock single-owner) the scaling model is SHARDING:
run N gateway instances, each owning a disjoint set of bots, per
instance capacity documented in [scaling.md](scaling.md). Redis
leases remain a roadmap item for 0.2 (see scaling.md's "shared
coordination" section) — this document is kept only as design history.
