# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy PYTHONUNBUFFERED=1
WORKDIR /app

# dependencies first: this layer is cached until uv.lock changes
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY examples ./examples
RUN uv sync --frozen --no-dev

RUN useradd --create-home relay
USER relay
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 18082
CMD ["relay", "api", "--app", "examples.jobs:app"]
