"""Database engine + a tiny versioned-SQL migration runner.

Migrations are plain `.sql` files in `relay/migrations`, applied in filename order and recorded
in `schema_migrations`. A Postgres advisory lock makes concurrent `migrate()` calls (several
containers starting at once) safe.
"""

from __future__ import annotations

from importlib import resources

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

_MIGRATION_LOCK_ID = 7_331_001


def make_engine(database_url: str, pool_size: int = 10) -> AsyncEngine:
    return create_async_engine(database_url, pool_size=pool_size, max_overflow=pool_size)


def _migration_files() -> list[tuple[str, str]]:
    folder = resources.files("relay") / "migrations"
    files = [p for p in folder.iterdir() if p.name.endswith(".sql")]
    return sorted((f.name, f.read_text()) for f in files)


async def migrate(engine: AsyncEngine) -> list[str]:
    """Apply pending migrations. Returns the names that were applied."""
    applied: list[str] = []
    async with engine.begin() as conn:
        await conn.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": _MIGRATION_LOCK_ID})
        await conn.execute(
            text(
                "CREATE TABLE IF NOT EXISTS schema_migrations ("
                " name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
        )
        rows = await conn.execute(text("SELECT name FROM schema_migrations"))
        done: set[str] = set(rows.scalars())
        for name, sql in _migration_files():
            if name in done:
                continue
            # asyncpg's plain execute() (no parameters) uses Postgres' simple query protocol,
            # which accepts several statements in one string. Same connection + transaction.
            raw = await conn.get_raw_connection()
            await raw.driver_connection.execute(sql)  # type: ignore[union-attr]
            await conn.execute(
                text("INSERT INTO schema_migrations (name) VALUES (:n)"), {"n": name}
            )
            applied.append(name)
    return applied
