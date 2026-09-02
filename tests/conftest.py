import pytest
import pytest_asyncio
from sqlalchemy import text

from app.db import Base, engine


async def _db_reachable() -> bool:
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


@pytest_asyncio.fixture
async def clean_db():
    """Explicit opt-in fixture for tests that hit a real Postgres instance.
    Skips (rather than fails) when no DB is reachable, e.g. Postgres/Redis
    haven't been started with `docker compose up -d postgres redis`."""
    if not await _db_reachable():
        pytest.skip(
            "Postgres not reachable at DATABASE_URL — start it with "
            "`docker compose up -d postgres redis` before running integration tests."
        )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(text("TRUNCATE TABLE chunks, files RESTART IDENTITY CASCADE"))
    yield
