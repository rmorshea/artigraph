from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from artigraph.core.api.filter import NodeLinkFilter
from artigraph.core.api.funcs import write, write_one
from artigraph.core.api.link import NodeLink
from artigraph.core.api.node import Node
from tests.common.check import check_can_read_write_delete_one


async def test_write_read_delete_node_link():
    node1 = Node()
    node2 = Node()
    await write([node1, node2])
    node_link = NodeLink(parent_id=node1.node_id, child_id=node2.node_id)
    node_link_filter = NodeLinkFilter(link_id=node_link.link_id)
    await check_can_read_write_delete_one(node_link, self_filter=node_link_filter)


async def test_write_order_does_not_matter_for_fk_constraint():
    node1 = Node()
    node2 = Node()
    node_link = NodeLink(parent_id=node1.node_id, child_id=node2.node_id)
    await write([node_link, node1, node2])


async def test_cannot_create_node_link_for_missing_nodes():
    node_link = NodeLink(parent_id=uuid4(), child_id=uuid4())
    with pytest.raises(IntegrityError):
        await write_one(node_link)
