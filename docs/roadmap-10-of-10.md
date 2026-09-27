# Roadmap: tg-session-gateway → 10/10

> Goal: the go-to open-source project for Telegram bot session persistence
> and rate-limit enforcement. When someone searches "telegram bot deploy
> without FloodWait" or "telegram session sidecar", this is the answer.
>
> This roadmap incorporates findings from the external gap analysis
> (docs/gap-analysis-report.md) and the sidecar requirements spec
> (docs/sidecar-spec.md).

---

## Current State (honest baseline, post-gap-analysis)

| Dimension | Score | Evidence |
|---|---|---|
| Architecture | 8/10 | mtgateway/ sidecar is the correct design (gap analysis §7 confirms); Bot API gateway solid; mtproto/ patch is the wrong design and must be removed |
| Code quality | 6/10 | Clean Python, but no mypy, no pre-commit, no type checking in CI, some files >400 lines |
| Testing | 5/10 | 72 unit tests; no integration tests, no E2E, no load tests, no coverage measurement |
| CI/CD | 4/10 | Basic pytest + docker build; no linting in CI, no publish, no release automation |
| Documentation | 4/10 | README confuses three products (gap analysis §4.1); scope contradiction (§4.2); no decision tree |
| Distribution | 2/10 | No published images, no releases, users must build from source |
| Community files | 2/10 | LICENSE only; no CONTRIBUTING, CHANGELOG, SECURITY, CODE_OF_CONDUCT, issue templates |
| Project structure | 4/10 | mtproto/ patch confuses readers (§7.6: "delete or quarantine"); .pytest_cache tracked; no Makefile |
| Operations | 3/10 | Prometheus endpoint exists; no Grafana dashboard, no alerting, no runbook |
| Battle-testing | 1/10 | Zero production deployments; unit tests only |
| Security | 5/10 | Tokens encrypted (Fernet); session files NOT encrypted (§4.5); no singleton lock (§6.5) |
| Library coverage | 4/10 | Pyrogram only; no Telethon, no pyrofork compatibility matrix (§4.6) |

---

## Phase 0: Architectural Cleanup (CRITICAL — do first)

**Source: gap analysis §2.2, §3 (R1-R10), §4.1-4.3, §7.6**

> The report's strongest finding: `mtproto/` is the wrong architecture.
> It is explicitly the "rejected design" from the spec (§8: "Mounting
> a .session file into the application container"). Keeping it alongside
> the correct solution (`mtgateway/`) confuses every reader.

### 0.1 Remove the mtproto/ patch

- [ ] **Delete the `mtproto/` directory entirely** (gap analysis §7.6:
      "As long as it ships next to the gateway README, it will be
      mistaken for the sidecar")
- [ ] Remove `mtproto/` references from `Dockerfile` (`COPY mtproto/ mtproto/`)
- [ ] Remove `mtproto/` references from `.dockerignore`
- [ ] Remove `tests/test_mtproto_patch.py`
- [ ] Remove `docs/roadmap-10-of-10.md` Phase 1 reference to `mtproto/`
- [ ] Add a `docs/design-decisions.md` explaining WHY the patch was
      removed (educational: three problems it couldn't solve)

**Rationale (from gap analysis §3):**
- R1 violation: auth_key lives in the APP process, not a sidecar
- R3 violation: patch CREATES a .session in the app container (the exact excluded design)
- R5 violation: app ready still includes "Pyrogram connected to DC" (1-2s Telegram RTT)
- R6 violation: session volume is inside the app image (coupled lifecycle)
- R7 violation: no singleton enforcement → AUTH_KEY_DUPLICATED on rolling update
- R8-R10 violation: rate guard unreachable from Pyrogram call stack

### 0.2 Fix the README scope contradiction

- [ ] Update README to describe exactly TWO products (not three):
  - `gateway/` — Bot API gateway (for aiogram/PTB/grammY bots)
  - `mtgateway/` — MTProto sidecar (for Pyrogram/pyrofork bots)
- [ ] Remove the "MTProto Session Persistence (Pyrogram / pyrofork)"
      section that describes the deleted patch
- [ ] Add a decision tree at the top (see Phase 4.1 below)
- [ ] Update V1 scope line: remove "MTProto sessions out of scope"
      (mtgateway/ IS the MTProto solution now)
- [ ] Fix the "zero code changes" claim — it was true for the patch,
      but the patch is gone; mtgateway/ requires a transport-layer
      change (which is the correct architecture)

### 0.3 Singleton session enforcement

**Source: gap analysis §6.5 (chigwell/telegram-mcp, DimaPhil/telegram-proxy-api)**

- [ ] Add advisory file lock BEFORE Pyrogram `connect()` in
      `mtgateway/sessions.py`:
      ```python
      # Before client.start():
      lock_path = os.path.join(session_dir, f"{alias}.lock")
      lock_fd = os.open(lock_path, os.O_CREAT | os.O_EXCL)
      # If FileExistsError → another sidecar owns this session → fail fast
      # WITHOUT calling Telegram (R7: no second socket ever reaches Telegram)
      ```
- [ ] Release lock on clean shutdown
- [ ] Handle stale locks (PID check + heartbeat timestamp)
- [ ] Log prominently when a lock is refused: "another process holds this session"
- [ ] Test: start sidecar A with session X, try to start sidecar B with
      session X → B fails at the lock, never reaches Telegram

### 0.4 Session file encryption at rest

**Source: gap analysis §4.5**

- [ ] Document that `.session` files contain the raw `auth_key`
- [ ] Add `docs/security.md` covering session file risks
- [ ] Investigate: encrypt session files with Fernet (GW_FERNET_KEY)
      → decrypt in memory on load → never write plaintext to disk
- [ ] Volume permissions: ensure `chmod 700` on session directory,
      `chmod 600` on individual `.session` files
- [ ] Document the threat model: anyone with volume access has the auth_key

### 0.5 Library compatibility matrix

**Source: gap analysis §4.6**

- [ ] Create `docs/compatibility.md`:
  | Library | Version tested | Status | Notes |
  |---|---|---|---|
  | Pyrogram | 2.0.106 | tested | Primary target |
  | pyrofork | latest | needs testing | Assumed compatible, not verified |
  | Telethon | — | not supported | Different architecture (see below) |
- [ ] Add pyrofork to CI test dependencies and verify
- [ ] Document Telethon as a V2 goal (different session format, different
      client construction — not a drop-in)
- [ ] Add a `TROUBLESHOOTING.md` section for library-specific issues

---

## Phase 1: Project Hygiene (Day 1)

**Goal: Make the repo look like a project people trust at first glance.**

### 1.1 Clean the repo

- [ ] Remove `.pytest_cache/` from git tracking (add to `.gitignore`)
- [ ] Move root `Dockerfile` → `docker/Dockerfile.gateway` (consistency with `Dockerfile.mtgateway`)
- [ ] Remove `gateway/admin.py` dead code (models already there, routes duplicated in `app.py`)
- [ ] Add `tests/__init__.py` and `tests/conftest.py` (shared fixtures)
- [ ] Rename deploy scripts: `deploy/gateway-restart.sh`, `deploy/mtgateway-restart.sh`
- [ ] Remove `pyproject.toml` `[tool.setuptools]` section or make properly installable
- [ ] Save the gap analysis report as `docs/gap-analysis-report.md` (reference)

### 1.2 Add missing community files

- [ ] `CONTRIBUTING.md` — how to submit PRs, code style, test requirements, commit convention
- [ ] `CHANGELOG.md` — Keep a Changelog format; retroactively document v0.0.1 → current
- [ ] `SECURITY.md` — how to report vulnerabilities, security contact, disclosure policy
- [ ] `CODE_OF_CONDUCT.md` — Contributor Covenant v2.1
- [ ] `.github/ISSUE_TEMPLATE/bug_report.md`
- [ ] `.github/ISSUE_TEMPLATE/feature_request.md`
- [ ] `.github/PULL_REQUEST_TEMPLATE.md`
- [ ] `.github/dependabot.yml`

### 1.3 Developer experience

- [ ] `Makefile`: `make test`, `make lint`, `make format`, `make typecheck`, `make docker-build`, `make docker-up`, `make clean`
- [ ] `.pre-commit-config.yaml` — ruff (lint+format), mypy, end-of-file-fixer
- [ ] `ruff.toml` — line-length 100, target py312, import sorting
- [ ] `mypy.ini` — strict mode for `gateway/` and `mtgateway/`

---

## Phase 2: Code Quality (Day 1-2)

### 2.1 Type hints & strictness

- [ ] Add type hints to all public functions in `gateway/` and `mtgateway/`
- [ ] Add `py.typed` marker files
- [ ] `mypy --strict gateway/ mtgateway/` → 0 errors
- [ ] Add `Protocol` types for the consumer interface

### 2.2 Error handling

- [ ] Define exception hierarchy: `GatewayError`, `SessionNotFoundError`, `RateLimitError`, `UpstreamError`, `AuthError`, `LockNotAcquiredError`
- [ ] Replace bare `except Exception` with specific exceptions
- [ ] Add structured logging with correlation IDs
- [ ] Add `X-Request-ID` header tracing

### 2.3 Code organization

- [ ] Split `mtgateway/app.py` (430 lines) → `routes/session.py`, `routes/admin.py`, `routes/health.py`, `routes/metrics.py`
- [ ] Split `mtgateway/sessions.py` (370 lines) → `session.py`, `manager.py`, `consumer.py`
- [ ] Split `gateway/app.py` (238 lines) similarly
- [ ] Extract `mtgateway/serialization.py` (shared helpers)
- [ ] Create `gateway/types.py` (Pydantic models for API shapes)

### 2.4 Linting & formatting baseline

- [ ] `ruff check gateway/ mtgateway/ tests/ --fix` → 0 errors
- [ ] `ruff format gateway/ mtgateway/ tests/` → consistent style
- [ ] `mypy --strict gateway/ mtgateway/` → 0 errors
- [ ] All checks in CI (fail the build)

---

## Phase 3: CI/CD Pipeline (Day 2)

### 3.1 Enhanced CI

```yaml
jobs:
  lint:      # ruff check + ruff format --check
  typecheck: # mypy --strict
  test:      # pytest with coverage → Codecov
  security:  # pip-audit, Trivy container scan
  build:     # docker build both images
  publish:   # on tag: push to GHCR
```

### 3.2 Release workflow

- [ ] `.github/workflows/release.yml` — triggered by `v*` tags
- [ ] Multi-arch images (amd64 + arm64)
- [ ] Publishes to GHCR
- [ ] Creates GitHub Release with changelog

### 3.3 Branch protection

- [ ] Require PR reviews (1 approval)
- [ ] Require status checks: lint, typecheck, test
- [ ] Require up-to-date branches

---

## Phase 4: Documentation (Day 2-3)

### 4.1 README restructure (TWO products, not three)

```markdown
# TG Session Gateway

## Which one do you need?

| Your bot uses... | Use... | Directory |
|---|---|---|
| Bot API (aiogram, PTB, grammY) | Bot API Gateway | `gateway/` |
| Pyrogram / pyrofork (MTProto) | MTProto Sidecar | `mtgateway/` |

[Quick start →] [Architecture →] [Comparison →]
```

### 4.2 Additional documentation

- [ ] `docs/architecture.md` — sidecar pattern, data flow, rate guard
- [ ] `docs/api-reference.md` — every endpoint (from OpenAPI/FastAPI)
- [ ] `docs/migration-guide.md` — "I have a Pyrogram bot, how do I move it?"
- [ ] `docs/rate-limits.md` — FAQ limits, bucket diagram, tuning
- [ ] `docs/comparison.md` — vs BotMux, TelegramApiServer, docker-telethon-plus, gramgate
- [ ] `docs/deployment.md` — production guide
- [ ] `docs/security.md` — session file risks, encryption, threat model
- [ ] `docs/compatibility.md` — Pyrogram/pyrofork/Telethon support matrix
- [ ] `docs/design-decisions.md` — why the patch was removed, why the sidecar
- [ ] `docs/grafana-dashboard.json` — importable dashboard

### 4.3 README badges

- [ ] CI status, Codecov, Docker pulls, License, Python version, Code style

---

## Phase 5: Testing (Day 3-5)

### 5.1 Integration tests (mock Telegram)

- [ ] `tests/integration/conftest.py` — spins up mock Telegram + gateway
- [ ] Test: full lifecycle (register → send → receive → pause → drain)
- [ ] Test: rate limiting (50/s → clipped to 30/s)
- [ ] Test: rolling update (A connects, B connects, A disconnects)
- [ ] Test: restart (stop gateway → restart → session restored)
- [ ] Test: FLOOD_WAIT (mock 429 → per-peer backoff)
- [ ] Test: singleton lock (two sidecars → second fails at lock, not at Telegram)

### 5.2 E2E tests (real Telegram)

- [ ] `tests/e2e/` — requires real bot token (GitHub Secrets)
- [ ] Test: register, send, receive
- [ ] Test: kill consumer, restart, updates resume
- [ ] Test: 15 deploys in 20 min, zero re-auth
- [ ] Weekly scheduled workflow

### 5.3 Load tests

- [ ] `tests/load/` with `locust` or `k6`
- [ ] Benchmark: 30 msgs/s sustained
- [ ] Benchmark: 20 concurrent sessions
- [ ] Benchmark: proxy overhead p99 <50ms (N1)
- [ ] Publish `docs/benchmarks.md`

### 5.4 Coverage

- [ ] `pytest-cov` with `--cov=gateway --cov=mtgateway`
- [ ] Upload to Codecov
- [ ] Target: >80% on both packages

---

## Phase 6: Distribution (Day 5)

- [ ] GHCR publishing: both images, multi-arch
- [ ] `examples/basic/` — single bot, Bot API gateway
- [ ] `examples/pyrogram/` — single bot, MTProto sidecar + consumer code
- [ ] `examples/monitoring/` — gateway + Prometheus + Grafana
- [ ] Quick start script (curl | bash)

---

## Phase 7: Operations (Day 5-7)

- [ ] `docs/grafana-dashboard.json`
- [ ] `docs/alerting.md` — Prometheus alert rules
- [ ] `docs/runbook.md` — what to do when things break
- [ ] Docker healthchecks + autoheal labels
- [ ] Watchtower-compatible labels

---

## Phase 8: Community & Adoption (Week 2+)

- [ ] GitHub topics, awesome lists, blog post, demo GIF
- [ ] `good first issue` labels, GitHub Discussions
- [ ] PyPI consumer client library (`tg-gateway-client`)
- [ ] Helm chart / Terraform module (if demand)

---

## Phase 9: Battle-Testing (Week 2-4)

- [x] Deploy Bot API gateway for a real consumer (aiogram moderation bot)
- [ ] Deploy MTProto sidecar in shadow mode alongside the consumer
- [ ] Run for 1 week, collect metrics
- [ ] 20+ deploys in a day → zero FloodWait
- [ ] Publish case study

---

## Phase 10: v1.0 Release (Week 4+)

- [ ] All Phase 0-8 complete
- [ ] 7+ days production usage
- [ ] 80%+ coverage
- [ ] 3+ contributors (or 10+ stars)
- [ ] Zero open security vulnerabilities
- [ ] Documentation complete
- [ ] Docker images published
- [ ] CHANGELOG v0.0.1 → v1.0

---

## Source Reports Incorporated

| Report | Issues merged |
|---|---|
| `gap-analysis-report.md` §2.2 | mtproto/ is the wrong architecture → Phase 0.1 (delete) |
| §3 R1-R3 | App still owns TCP/auth_key → Phase 0.1 (delete patch, keep sidecar) |
| §3 R5 | Ready includes DC handshake → Phase 0.1 (sidecar solves this) |
| §3 R6 | Lifecycle coupled → Phase 0.1 (sidecar has own compose) |
| §3 R7 | No singleton → Phase 0.3 (file lock before connect) |
| §3 R8-R10 | Rate guard unreachable from Pyrogram → Phase 0.1 (sidecar wraps Pyrogram) |
| §4.1 | Two architectures, one README → Phase 4.1 (README restructure) |
| §4.2 | Scope contradiction → Phase 0.2 (update scope) |
| §4.3 | Wrong success metric → Phase 0.2 (fix claims) |
| §4.4 | Zero-change claim overstated → Phase 0.2 (remove claim) |
| §4.5 | Session files unencrypted → Phase 0.4 (encryption) |
| §4.6 | Library coverage unclear → Phase 0.5 (compatibility matrix) |
| §4.7 | POC freshness → Phases 1-9 (full roadmap) |
| §5 | Three-problem analysis → docs/design-decisions.md |
| §6.1-6.7 | External project references → Phase 4.2 (comparison docs) |
| §6.5 | Singleton lock (telegram-mcp) → Phase 0.3 |
| §7 | Design recommendation → mtgateway/ (already built) + Phase 0 cleanup |

---

## Success Metrics

| Metric | Current | v1.0 Target |
|---|---|---|
| Test count | 72 | 200+ |
| Test coverage | unknown | 80%+ |
| CI jobs | 2 | 6 |
| Documentation pages | 3 | 12+ |
| Docker pulls | 0 | 100+ |
| GitHub stars | 0 | 50+ |
| Contributors | 1 | 3+ |
| Production deployments | 0 | 1 (closed-source consumer, test mode) |
| Auth-key-duplicate incidents | untested | 0 (lock enforced) |
| Deploy-to-ready time | untested | <1s (no Telegram on path) |
