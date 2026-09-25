.PHONY: check test unit integration lint type fmt up down stack
check: lint type test
test:            ## unit + integration (integration tests skip themselves if services are down)
	uv run pytest -q
unit:
	uv run pytest -q tests/unit
integration: up
	uv run pytest -q tests/integration
lint:
	uv run ruff check . && uv run ruff format --check .
type:
	uv run mypy
fmt:
	uv run ruff format . && uv run ruff check --fix .
up:
	docker compose up -d --wait redis postgres
down:
	docker compose down
