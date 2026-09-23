.PHONY: install ui demo serve test lint integration
install:
	uv sync --extra dev
	cd frontend && npm ci
ui:
	cd frontend && npm run build
demo: ui
	uv run baltic demo
	uv run baltic serve
serve:
	uv run baltic serve
test:
	uv run pytest -q
lint:
	uv run ruff check backend
	cd frontend && npm run build
integration:
	uv run pytest -q backend/tests/test_integration.py
