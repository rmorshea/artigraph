from datetime import datetime, timezone

from artigraph.api.filter import MultiFilter, NodeFilter, ValueFilter
from artigraph.api.node import read_node, write_node
from artigraph.orm.node import Node


async def test_filter_by_node_created_and_update_at():
    node = Node()
    created_at = node.node_created_at = datetime(1993, 2, 6, tzinfo=timezone.utc)
    updated_at = node.node_updated_at = datetime(1995, 7, 25, tzinfo=timezone.utc)

    await write_node(node)

    assert (await read_node(NodeFilter(created_at=created_at))).node_id == node.node_id
    assert (await read_node(NodeFilter(updated_at=updated_at))).node_id == node.node_id


def test_multi_and_filter():
    vf1 = ValueFilter(gt=1).use(Node.node_id)
    vf2 = ValueFilter(lt=3).use(Node.node_id)
    vf3 = ValueFilter(eq=2).use(Node.node_id)

    multi_filter = vf1 & vf2 & vf3
    assert multi_filter == MultiFilter(op="and", filters=(vf1, vf2, vf3))

    assert (
        multi_filter.create().compile().string
        == (vf1.create() & vf2.create() & vf3.create()).compile().string
    )


def test_multi_or_filter():
    vf1 = ValueFilter(gt=1).use(Node.node_id)
    vf2 = ValueFilter(lt=3).use(Node.node_id)
    vf3 = ValueFilter(eq=2).use(Node.node_id)

    multi_filter = vf1 | vf2 | vf3
    assert multi_filter == MultiFilter(op="or", filters=(vf1, vf2, vf3))

    assert (
        multi_filter.create().compile().string
        == (vf1.create() | vf2.create() | vf3.create()).compile().string
    )


def test_multi_and_or_filter():
    vf1 = ValueFilter(gt=1).use(Node.node_id)
    vf2 = ValueFilter(lt=3).use(Node.node_id)
    vf3 = ValueFilter(eq=2).use(Node.node_id)
    vf4 = ValueFilter(eq=4).use(Node.node_id)

    multi_filter = (vf1 & vf2) | (vf3 & vf4)
    assert multi_filter == MultiFilter(
        op="or",
        filters=(
            MultiFilter(op="and", filters=(vf1, vf2)),
            MultiFilter(op="and", filters=(vf3, vf4)),
        ),
    )

    assert (
        multi_filter.create().compile().string
        == (vf1.create() & vf2.create() | vf3.create() & vf4.create()).compile().string
    )


def test_multi_filter_flattens_if_two_with_same_op():
    vf1 = ValueFilter(gt=1).use(Node.node_id)
    vf2 = ValueFilter(lt=3).use(Node.node_id)
    vf3 = ValueFilter(eq=2).use(Node.node_id)
    vf4 = ValueFilter(eq=4).use(Node.node_id)

    multi_filter = (vf1 & vf2) & (vf3 & vf4)
    assert multi_filter == MultiFilter(
        op="and",
        filters=(vf1, vf2, vf3, vf4),
    )

    assert (
        multi_filter.create().compile().string
        == (vf1.create() & vf2.create() & vf3.create() & vf4.create()).compile().string
    )
