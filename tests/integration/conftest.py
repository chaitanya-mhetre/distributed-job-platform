"""Integration fixtures: a real Redis + Postgres (docker compose, see `make up`).

Each test gets a random Redis namespace, and the jobs tables are truncated, so tests never see
each other's data. If the services aren't reachable, integration tests are skipped (not faked).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from relay import Relay, Settings

REDIS_URL = os.environ.get("RELAY_TEST_REDIS_URL", "redis://:relaydev@localhost:56384/0")
# A separate database, so tests never touch the data of a stack running on the same Postgres.
DB_URL = os.environ.get(
    "RELAY_TEST_DATABASE_URL", "postgresql+asyncpg://relay:relay@localhost:55437/relay_test"
)
_db_ready = False


async def ensure_test_database() -> None:
    global _db_ready
    if _db_ready:
        return
    base, _, name = DB_URL.rpartition("/")
    admin = create_async_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": name}
            )
            if not exists:
                await conn.execute(text(f'CREATE DATABASE "{name}"'))
    finally:
        await admin.dispose()
    _db_ready = True


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        if "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)


def make_settings(**overrides: Any) -> Settings:
    return Settings(
        redis_url=REDIS_URL,
        database_url=DB_URL,
        namespace=f"test-{uuid.uuid4().hex[:8]}",
        api_key="",
        **overrides,
    )


@pytest.fixture
async def relay() -> AsyncIterator[Relay]:
    app = Relay(make_settings())
    try:
        await asyncio.wait_for(app.broker.redis.ping(), 2)
        await asyncio.wait_for(ensure_test_database(), 5)
        async with app.store.engine.connect() as conn:
            await asyncio.wait_for(conn.execute(text("SELECT 1")), 5)
    except (OSError, TimeoutError, RedisConnectionError) as exc:
        await app.close()
        pytest.skip(f"Redis/Postgres not reachable ({type(exc).__name__}); run `make up`")
    await app.setup()  # real errors here fail the test instead of skipping it
    async with app.store.engine.begin() as conn:
        rows = await conn.execute(
            text(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
                " AND tablename <> 'schema_migrations'"
            )
        )
        tables: list[str] = list(rows.scalars())
        await conn.execute(text(f"TRUNCATE {', '.join(tables)} CASCADE"))
    yield app
    keys = [k async for k in app.broker.redis.scan_iter(f"{app.settings.namespace}:*")]
    if keys:
        await app.broker.redis.delete(*keys)
    await app.close()


async def wait_for(
    predicate: Callable[[], Awaitable[bool]], within: float = 20, interval: float = 0.05
) -> None:
    deadline = asyncio.get_running_loop().time() + within
    while not await predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(interval)
