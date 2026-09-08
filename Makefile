.PHONY: test lint format typecheck docker-build docker-up docker-down clean

test:
	python -m pytest -q --tb=short

lint:
	ruff check gateway/ mtgateway/ tests/ --output-format=concise

format:
	ruff format gateway/ mtgateway/ tests/

typecheck:
	mypy --strict gateway/ mtgateway/ || true

docker-build:
	docker build -t tg-session-gateway:dev .
	docker build -f docker/Dockerfile.mtgateway -t tg-mtgateway:dev .

docker-up:
	docker compose up -d

docker-down:
	docker compose down

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
