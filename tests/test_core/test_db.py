from uuid import uuid1

import pytest
from sqlalchemy import select

from artigraph.core.db import current_session
from artigraph.core.orm.node import OrmNode


async def test_current_session_auto_rollback():
    with pytest.raises(RuntimeError):
        async with current_session() as session:
            node = OrmNode(id=uuid1())
            session.add(node)
            await session.flush()
            result = await session.execute(select(OrmNode))
            result.scalar_one()
            msg = "This should trigger a rollback"
            raise RuntimeError(msg)

    async with current_session() as session:
        result = await session.execute(select(OrmNode))
        assert result.scalar_one_or_none() is None
