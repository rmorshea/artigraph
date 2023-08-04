import moto
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from artigraph.db import engine_context


@pytest.fixture(autouse=True)
def engine():
    # Need to do this in a sync fixture so context var is properly copied:
    # See: https://github.com/pytest-dev/pytest-asyncio/issues/127
    with engine_context(
        create_async_engine("sqlite+aiosqlite:///:memory:"),
        create_tables=True,
    ) as eng:
        yield eng


@pytest.fixture(autouse=True)
def mock_aws():
    with moto.mock_s3():
        yield
