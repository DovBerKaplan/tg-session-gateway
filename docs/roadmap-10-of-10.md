# Roadmap: tg-session-gateway → 10/10

> Goal: the go-to open-source project for Telegram bot session persistence
> and rate-limit enforcement. When someone searches "telegram bot deploy
> without FloodWait" or "telegram session sidecar", this is the answer.

---

## Current State (honest baseline)

| Dimension | Score | Evidence |
|---|---|---|
| Architecture | 8/10 | Correct: sidecar pattern, FAQ rate guard, lifecycle isolation, 44/44 spec requirements |
| Code quality | 6/10 | Clean Python, but no mypy, no pre-commit, no type checking in CI, some files >400 lines |
| Testing | 6/10 | 72 unit tests; no integration tests against real Telegram, no E2E, no load tests |
| CI/CD | 4/10 | Basic pytest + docker build; no linting in CI, no publish, no release automation |
| Documentation | 5/10 | Good README; no API reference, no architecture diagram, no migration guide, three products unexplained |
| Distribution | 2/10 | No published images, no releases, users must build from source |
| Community files | 2/10 | LICENSE only; no CONTRIBUTING, CHANGELOG, SECURITY, CODE_OF_CONDUCT, issue templates |
| Project structure | 5/10 | Reasonable tree; .pytest_cache tracked, Dockerfile location inconsistent, no Makefile |
| Operations | 3/10 | Prometheus endpoint exists; no Grafana dashboard, no alerting, no runbook |
| Battle-testing | 1/10 | Zero production deployments; unit tests only |

---

## Phase 1: Project Hygiene (Day 1)

**Goal: Make the repo look like a project people trust at first glance.**

### 1.1 Clean the repo

- [ ] Remove `.pytest_cache/` from git tracking (add to `.gitignore` if not already)
- [ ] Move root `Dockerfile` → `docker/Dockerfile.gateway` (consistency with `Dockerfile.mtgateway`)
- [ ] Remove `gateway/admin.py` dead code (models already there, routes duplicated in `app.py`)
- [ ] Add `tests/__init__.py` and `tests/conftest.py` (shared fixtures)
- [ ] Consolidate `deploy/safe-restart.sh` and `deploy/mtgateway-safe-restart.sh` into `deploy/` with clear naming: `deploy/gateway-restart.sh`, `deploy/mtgateway-restart.sh`
- [ ] Delete `pyproject.toml` `[tool.setuptools]` section (not installable as a package; remove confusion) or make it properly installable

### 1.2 Add missing community files

- [ ] `CONTRIBUTING.md` — how to submit PRs, code style, test requirements, commit message convention
- [ ] `CHANGELOG.md` — Keep a Changelog format; retroactively document v0.0.1 → current
- [ ] `SECURITY.md` — how to report vulnerabilities, security contact, disclosure policy
- [ ] `CODE_OF_CONDUCT.md` — Contributor Covenant v2.1
- [ ] `.github/ISSUE_TEMPLATE/bug_report.md` — structured bug reports
- [ ] `.github/ISSUE_TEMPLATE/feature_request.md` — structured feature requests
- [ ] `.github/PULL_REQUEST_TEMPLATE.md` — checklist for PRs (tests, docs, changelog)
- [ ] `.github/FUNDING.yml` — optional GitHub Sponsors
- [ ] `.github/dependabot.yml` — automated dependency updates (weekly, pip + docker)

### 1.3 Developer experience

- [ ] `Makefile` with targets: `make test`, `make lint`, `make format`, `make typecheck`, `make docker-build`, `make docker-up`, `make docker-down`, `make clean`
- [ ] `.pre-commit-config.yaml` — ruff (lint+format), mypy, end-of-file-fixer, trailing-whitespace
- [ ] `ruff.toml` or `[tool.ruff]` in pyproject — line-length 100, target py312, import sorting
- [ ] `mypy.ini` or `[tool.mypy]` — strict mode for `gateway/` and `mtgateway/`, ignore `mtproto/` (monkey-patching)
- [ ] `justfile` as alternative to Makefile (optional, younger audience prefers it)

---

## Phase 2: Code Quality (Day 1-2)

**Goal: Every file passes strict linting, type checking, and formatting.**

### 2.1 Type hints

- [ ] Add type hints to all public functions in `gateway/` and `mtgateway/`
- [ ] Add `py.typed` marker files to both packages
- [ ] Fix mypy errors (target: `mypy --strict gateway/ mtgateway/` → 0 errors)
- [ ] Add `Protocol` types for the consumer interface (what the app implements)

### 2.2 Error handling

- [ ] Define custom exception hierarchy: `GatewayError`, `SessionNotFoundError`, `RateLimitError`, `UpstreamError`, `AuthError`
- [ ] Replace bare `except Exception` with specific exceptions where possible
- [ ] Add structured logging (JSON format option) with correlation IDs
- [ ] Add request tracing: each HTTP request gets an `X-Request-ID` that flows through logs

### 2.3 Code organization

- [ ] Split `mtgateway/app.py` (430 lines) into: `routes/session.py`, `routes/admin.py`, `routes/health.py`, `routes/metrics.py`
- [ ] Split `mtgateway/sessions.py` (370 lines) into: `session.py` (single session), `manager.py` (lifecycle), `consumer.py` (consumer tracking)
- [ ] Split `gateway/app.py` (238 lines) similarly
- [ ] Extract serialization helpers (`_serialize`, `_json_body`) into `mtgateway/serialization.py` (shared)
- [ ] Create `gateway/types.py` with Pydantic models for API request/response shapes

### 2.4 Linting & formatting baseline

- [ ] `ruff check gateway/ mtgateway/ mtproto/ tests/ --fix` → 0 errors
- [ ] `ruff format gateway/ mtgateway/ mtproto/ tests/` → consistent style
- [ ] `mypy --strict gateway/ mtgateway/` → 0 errors
- [ ] All checks in CI (fail the build on any error)

---

## Phase 3: CI/CD Pipeline (Day 2)

**Goal: Push to main → tests run → lint runs → type check runs → Docker image builds → image publishes → release tag creates a GitHub Release.**

### 3.1 Enhanced CI workflow

```yaml
# .github/workflows/ci.yml
jobs:
  lint:      # ruff check + ruff format --check
  typecheck: # mypy --strict
  test:      # pytest with coverage → upload to Codecov
  security:  # pip-audit, Trivy container scan
  build:     # docker build both images
  publish:   # on tag: push to GHCR (main branch: push :latest, tags: push :vX.Y.Z)
```

### 3.2 Release workflow

- [ ] `.github/workflows/release.yml` — triggered by `v*` tags
- [ ] Builds multi-arch images (linux/amd64 + linux/arm64)
- [ ] Publishes to `ghcr.io/dovberkaplan/tg-session-gateway` and `ghcr.io/dovberkaplan/tg-mtgateway`
- [ ] Creates GitHub Release with auto-generated changelog
- [ ] Signs images with cosign (optional but professional)

### 3.3 Branch protection

- [ ] Require PR reviews (1 approval minimum)
- [ ] Require status checks: lint, typecheck, test
- [ ] Require up-to-date branches before merge
- [ ] Enable auto-merge for approved PRs with green checks

---

## Phase 4: Documentation (Day 2-3)

**Goal: A newcomer understands what this is, which product they need, and how to use it — in under 60 seconds.**

### 4.1 README restructure

The current README describes the Bot API gateway. With three products, the README needs to be a decision tree:

```markdown
# TG Session Gateway

## Which one do you need?

| Your bot uses... | Use... | Directory |
|---|---|---|
| Bot API (aiogram, PTB, grammY) | Bot API Gateway | `gateway/` |
| Pyrogram / pyrofork (MTProto) | MTProto Sidecar | `mtgateway/` |
| Pyrogram but don't want to change code yet | Session Patch | `mtproto/` |

[Quick start for each →]
[Architecture diagram →]
[Comparison vs BotMux / TelegramApiServer →]
```

### 4.2 Additional documentation

- [ ] `docs/architecture.md` — ASCII/c4 diagram of the sidecar pattern, data flow, rate guard internals
- [ ] `docs/api-reference.md` — every HTTP endpoint, request/response shapes, error codes (generated from OpenAPI/FastAPI)
- [ ] `docs/migration-guide.md` — "I have a Pyrogram bot, how do I move it to the sidecar?" step by step
- [ ] `docs/rate-limits.md` — how the FAQ limits are implemented, bucket diagram, tuning parameters
- [ ] `docs/comparison.md` — honest table vs. BotMux, TelegramApiServer, docker-telethon-plus, pyrogram-bridge
- [ ] `docs/deployment.md` — production deployment guide (volume sizing, monitoring, scaling)
- [ ] Inline `# API Documentation` comments → auto-generate docs with `mkdocs` or `sphinx`
- [ ] `docs/grafana-dashboard.json` — importable dashboard with the Prometheus metrics

### 4.3 README badges

- [ ] CI status badge
- [ ] Codecov coverage badge
- [ ] Docker pulls badge (after GHCR publishing)
- [ ] License badge
- [ ] Python version badge
- [ ] Code style: ruff badge

---

## Phase 5: Testing (Day 3-5)

**Goal: Confidence that this works against real Telegram, not just mocks.**

### 5.1 Integration tests (mock Telegram server)

- [ ] `tests/integration/` directory with `conftest.py` that spins up:
  - A mock Telegram Bot API server (aiohttp that returns canned responses)
  - The gateway pointing at the mock
  - A test consumer that makes HTTP calls
- [ ] Test: full lifecycle — register session → send message → receive update → pause → resume → drain → unregister
- [ ] Test: rate limiting — send 50 msgs/s, verify only 30/s pass, verify synthetic 429
- [ ] Test: rolling update — consumer A connects, consumer B connects, A disconnects, B still receives
- [ ] Test: restart — stop gateway, restart, verify session restored from disk
- [ ] Test: FLOOD_WAIT — mock returns 429, verify per-peer backoff engages

### 5.2 E2E tests (real Telegram, optional CI)

- [ ] `tests/e2e/` — requires a real bot token (via GitHub Secrets)
- [ ] Test: register a test bot, send a message to a known chat, verify delivery
- [ ] Test: kill the consumer process, restart it, verify updates resume without reconnect
- [ ] Test: 15 deploys (container restarts) in 20 minutes, verify zero re-auth
- [ ] Run weekly (not per-commit) via scheduled workflow to avoid rate limits

### 5.3 Load tests

- [ ] `tests/load/` with `locust` or `k6`
- [ ] Benchmark: 30 msgs/s sustained through the sidecar
- [ ] Benchmark: 20 concurrent sessions
- [ ] Benchmark: proxy overhead p50/p99 (must be <50ms per N1)
- [ ] Publish results in `docs/benchmarks.md`

### 5.4 Coverage

- [ ] Add `pytest-cov` with `--cov=gateway --cov=mtgateway`
- [ ] Upload to Codecov (free for OSS)
- [ ] Target: >80% line coverage on `gateway/` and `mtgateway/`
- [ ] Coverage badge in README

---

## Phase 6: Distribution (Day 5)

**Goal: `docker pull` works. Users don't build from source.**

### 6.1 GHCR publishing

- [ ] `ghcr.io/dovberkaplan/tg-session-gateway:latest` — Bot API gateway
- [ ] `ghcr.io/dovberkaplan/tg-mtgateway:latest` — MTProto sidecar
- [ ] Version tags: `:v0.2.0`, `:v0.2.0-alpine` (smaller)
- [ ] Multi-arch: amd64 + arm64
- [ ] OCI labels: source, license, description, maintainer

### 6.2 Docker Compose examples

- [ ] `examples/basic/` — single bot, Bot API gateway, docker-compose.yml + README
- [ ] `examples/pyrogram/` — single bot, MTProto sidecar, docker-compose.yml + consumer client code
- [ ] `examples/monitoring/` — gateway + Prometheus + Grafana, docker-compose.yml
- [ ] Each example: `docker compose up` → working in <2 minutes

### 6.3 Quick start script

- [ ] `curl -fsSL https://raw.githubusercontent.com/.../quickstart.sh | bash`
- [ ] Detects Docker, creates network, starts gateway, registers a bot interactively

---

## Phase 7: Operations (Day 5-7)

**Goal: Running this in production is safe and observable.**

### 7.1 Monitoring

- [ ] `docs/grafana-dashboard.json` — pre-built dashboard showing:
  - Session states over time
  - Queue depths
  - Rate-limited vs real 429 rates
  - Connection ages
  - Deploy-to-first-message SLO
- [ ] `docs/alerting.md` — recommended Prometheus alert rules:
  - `tg_gateway_real_flood_wait_total > 0 for 5m` → alert
  - `tg_gateway_update_queue_depth > 1000` → alert
  - `tg_gateway_session_state == 0 for 1m` → page

### 7.2 Runbook

- [ ] `docs/runbook.md` — what to do when:
  - Gateway won't start (check volume permissions, check env vars)
  - Session stuck in `connecting` (check Telegram connectivity, check API credentials)
  - Queue depth growing (check consumer health, check if app is down)
  - Real 429s occurring (check if rate limits are misconfigured, check for hot peers)

### 7.3 Health checks & autoheal

- [ ] Docker healthcheck on both images (already present, verify)
- [ ] `autoheal` container label for automatic restart on unhealthy
- [ ] Watchtower-compatible labels for image updates (with `tg.gateway=true` filter to avoid cross-project restarts)

---

## Phase 8: Community & Adoption (Week 2+)

**Goal: People find this, trust it, and contribute.**

### 8.1 Discovery

- [ ] Add topics to GitHub repo: `telegram`, `bot`, `session`, `rate-limiting`, `pyrogram`, `aiogram`, `sidecar`, `infrastructure`
- [ ] Submit to: Awesome Telegram Bots list, Python Awesome, r/Telegram, r/Python
- [ ] Write a blog post: "How I eliminated FloodWait from my Telegram bot deploys"
- [ ] Create a demo GIF/video showing rolling update with zero Telegram disconnection

### 8.2 Contribution readiness

- [ ] `good first issue` labels on 5+ approachable tasks
- [ ] `help wanted` labels on specific feature requests
- [ ] GitHub Discussions enabled (Q&A category)
- [ ] GitHub Projects board for roadmap visibility
- [ ] Pin an issue: "Roadmap to v1.0 — what's missing?"

### 8.3 Ecosystem integration

- [ ] PyPI package for the consumer client library (`tg-gateway-client`) — thin HTTP wrapper that mimics Pyrogram's API surface
- [ ] Helm chart for Kubernetes deployment (if there's demand)
- [ ] Terraform module (if there's demand)

---

## Phase 9: Battle-Testing (Week 2-4)

**Goal: Proof that this works in production.**

- [ ] Deploy the Bot API gateway to kot's server for modbot (aiogram)
- [ ] Deploy the MTProto sidecar alongside kot (but not yet routing traffic) — observe
- [ ] Run kot's modbot through the Bot API gateway for 1 week — collect metrics
- [ ] Run the MTProto sidecar in shadow mode (connected but not serving traffic) for 1 week
- [ ] Perform 20+ deploys in a day — verify zero FloodWait
- [ ] Document findings: actual p99 proxy overhead, actual queue depths, actual reconnect times
- [ ] Publish case study in `docs/case-study.md`

---

## Phase 10: v1.0 Release (Week 4+)

**Criteria for v1.0:**

- [ ] All Phase 1-8 complete
- [ ] 7+ days of production usage without critical issues
- [ ] 80%+ test coverage
- [ ] 3+ contributors (or 10+ stars as proxy for interest)
- [ ] Zero open security vulnerabilities
- [ ] Documentation complete (README, API ref, migration guide, runbook)
- [ ] Docker images published and pulling correctly
- [ ] CHANGELOG documents all changes from v0.0.1 to v1.0

---

## Success Metrics

| Metric | Current | v1.0 Target |
|---|---|---|
| Test count | 72 | 200+ |
| Test coverage | unknown | 80%+ |
| CI jobs | 2 (test, build) | 6 (lint, typecheck, test, security, build, publish) |
| Documentation pages | 3 | 10+ |
| Docker pulls | 0 | 100+ |
| GitHub stars | 0 | 50+ |
| Contributors | 1 | 3+ |
| Production deployments | 0 | 1 (kot) |
| Real Telegram tests | 0 | Weekly E2E suite |

---

## Priority Order (if time is limited)

1. **Phase 1** (hygiene) — 2 hours, immediate credibility
2. **Phase 4.1** (README restructure) — 1 hour, most impactful single change
3. **Phase 3** (CI/CD) — 4 hours, professional pipeline
4. **Phase 5.1** (integration tests) — 1 day, confidence
5. **Phase 6** (distribution) — 2 hours, usability
6. Everything else in order
