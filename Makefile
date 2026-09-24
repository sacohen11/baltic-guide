.PHONY: install test build up down logs
install:
	uv sync --frozen --extra dev
	cd frontend && npm ci
test:
	uv run ruff check backend scripts
	uv run pytest -q
	cd frontend && npm test
build:
	cd frontend && npm run build
up:
	docker compose up --build -d
down:
	docker compose down
logs:
	docker compose logs -f api worker ingestor
