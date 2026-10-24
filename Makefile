.PHONY: test go-build go-test go-lint go-conformance go-kind docker-build docker-up docker-down clean

# default target: the product's own tests (Go). Black-box conformance
# lives under go-conformance (needs the venv's httpx+pytest).
test: go-test

go-build:
	CGO_ENABLED=0 $(GO) go build -o /tmp/tggw ./cmd/tggw

go-test:
	$(GO) go test -race -count=1 ./...

go-lint:
	golangci-lint run --timeout 5m

go-conformance: go-build
	TGGW_BIN=/tmp/tggw .venv/bin/python -m pytest -q tests/conformance_go

go-kind:
	deploy/helm/verify-kind.sh

docker-build:
	docker build -f docker/Dockerfile.tggw -t tggw:dev .

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	python3 -c "import shutil,glob; [shutil.rmtree(p, ignore_errors=True) for p in glob.glob('**/__pycache__', recursive=True)]"
