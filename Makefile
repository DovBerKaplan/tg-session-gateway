.PHONY: test lint format typecheck docker-build docker-up docker-down clean

test:
	python -m pytest -q --tb=short

lint:
	ruff check gateway/ mtgateway/ tests/ --output-format=concise

format:
	ruff format gateway/ mtgateway/ tests/

typecheck:
	mypy gateway/ mtgateway/

docker-build:
	docker build -f docker/Dockerfile.gateway -t tg-session-gateway:dev .
	docker build -f docker/Dockerfile.mtgateway -t tg-mtgateway:dev .

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache

# ── Go build (tggw) ────────────────────────────────────────────────
GO ?= go

go-build:
	cd go && CGO_ENABLED=0 $(GO) build -o /tmp/tggw ./cmd/tggw

go-test:
	cd go && $(GO) test -race -count=1 ./...

go-lint:
	cd go && golangci-lint run --timeout 5m

go-conformance: go-build
	TGGW_BIN=/tmp/tggw .venv/bin/python -m pytest -q tests/conformance_go

go-kind:
	deploy/helm/verify-kind.sh

.PHONY: go-build go-test go-lint go-conformance go-kind
