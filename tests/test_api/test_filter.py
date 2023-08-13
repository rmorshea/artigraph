from datetime import datetime, timezone

from artigraph.api.filter import NodeFilter
from artigraph.api.node import new_node, read_node, write_node


async def test_filter_by_node_created_and_update_at():
    node = new_node()
    created_at = node.node_created_at = datetime(1993, 2, 6, tzinfo=timezone.utc)
    updated_at = node.node_updated_at = datetime(1995, 7, 25, tzinfo=timezone.utc)

    await write_node(node)

    assert (await read_node(NodeFilter(created_at=created_at))).node_id == node.node_id
    assert (await read_node(NodeFilter(updated_at=updated_at))).node_id == node.node_id
