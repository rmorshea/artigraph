import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from artigraph.db import engine_context
from artigraph.orm.base import Base


@pytest.fixture
def engine():
    # Need to do this in a sync fixture so context var is properly copied:
    # See: https://github.com/pytest-dev/pytest-asyncio/issues/127
    with engine_context(create_async_engine("sqlite+aiosqlite:///:memory:")) as eng:
        yield eng


@pytest.fixture(autouse=True)
async def create_tables(engine: AsyncEngine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
