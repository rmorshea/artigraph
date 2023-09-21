import moto
import pytest
from sqlalchemy import event

from artigraph.core.db import current_engine


@pytest.fixture(autouse=True)
def engine():
    # Need to do this in a sync fixture so context var is properly copied:
    # See: https://github.com/pytest-dev/pytest-asyncio/issues/127
    with current_engine(
        "sqlite+aiosqlite:///:memory:",
        create_tables=True,
    ) as eng:
        # enforce foreign key constraints
        event.listen(
            eng.sync_engine,
            "connect",
            lambda c, _: c.execute("pragma foreign_keys=on"),
        )
        yield eng


@pytest.fixture(autouse=True)
def mock_aws():
    with moto.mock_s3():
        yield
