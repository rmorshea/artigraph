import asyncio

import pytest
from sqlalchemy import select

from artigraph.db import current_session, get_engine
from artigraph.orm.node import Node


async def test_current_session_auto_rollback():
    with pytest.raises(RuntimeError):
        async with current_session() as session:
            node = Node(node_parent_id=None)
            session.add(node)
            await session.flush()
            result = await session.execute(select(Node))
            result.scalar_one()
            msg = "This should trigger a rollback"
            raise RuntimeError(msg)

    async with current_session() as session:
        result = await session.execute(select(Node))
        assert result.scalar_one_or_none() is None


def test_get_engine_create_tables():
    asyncio.run(get_engine(create_tables=True))
