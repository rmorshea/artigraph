from datetime import datetime, timezone
from uuid import uuid1, uuid4

from artigraph.core.api.filter import MultiFilter, NodeFilter, ValueFilter
from artigraph.core.api.funcs import orm_read_one_or_none, orm_write, read_one, write_many
from artigraph.core.api.link import Link
from artigraph.core.api.node import Node
from artigraph.core.orm.node import OrmNode


async def test_filter_by_node_created_and_update_at():
    node = OrmNode(id=uuid4())
    created_at = node.created_at = datetime(1993, 2, 6, tzinfo=timezone.utc)
    updated_at = node.updated_at = datetime(1995, 7, 25, tzinfo=timezone.utc)

    await orm_write([node])

    created_at_read = await orm_read_one_or_none(OrmNode, NodeFilter(created_at=created_at))
    assert created_at_read is not None
    assert created_at_read.id == node.id

    updated_at_read = await orm_read_one_or_none(OrmNode, NodeFilter(updated_at=updated_at))
    assert updated_at_read is not None
    assert updated_at_read.id == node.id


def test_multi_and_filter():
    vf1 = ValueFilter(gt=uuid1()).against(OrmNode.id)
    vf2 = ValueFilter(lt=uuid1()).against(OrmNode.id)
    vf3 = ValueFilter(eq=uuid1()).against(OrmNode.id)

    multi_filter = vf1 & vf2 & vf3
    assert multi_filter == MultiFilter(op="and", filters=(vf1, vf2, vf3))

    assert (
        multi_filter.create().compile().string
        == (vf1.create() & vf2.create() & vf3.create()).compile().string
    )


def test_multi_or_filter():
    vf1 = ValueFilter(gt=uuid1()).against(OrmNode.id)
    vf2 = ValueFilter(lt=uuid1()).against(OrmNode.id)
    vf3 = ValueFilter(eq=uuid1()).against(OrmNode.id)

    multi_filter = vf1 | vf2 | vf3
    assert multi_filter == MultiFilter(op="or", filters=(vf1, vf2, vf3))

    assert (
        multi_filter.create().compile().string
        == (vf1.create() | vf2.create() | vf3.create()).compile().string
    )


def test_multi_and_or_filter():
    vf1 = ValueFilter(gt=uuid1()).against(OrmNode.id)
    vf2 = ValueFilter(lt=uuid1()).against(OrmNode.id)
    vf3 = ValueFilter(eq=uuid1()).against(OrmNode.id)
    vf4 = ValueFilter(eq=uuid1()).against(OrmNode.id)

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
    vf1 = ValueFilter(gt=uuid1()).against(OrmNode.id)
    vf2 = ValueFilter(lt=uuid1()).against(OrmNode.id)
    vf3 = ValueFilter(eq=uuid1()).against(OrmNode.id)
    vf4 = ValueFilter(eq=uuid1()).against(OrmNode.id)

    multi_filter = (vf1 & vf2) & (vf3 & vf4)
    assert multi_filter == MultiFilter(
        op="and",
        filters=(vf1, vf2, vf3, vf4),
    )

    assert (
        multi_filter.create().compile().string
        == (vf1.create() & vf2.create() & vf3.create() & vf4.create()).compile().string
    )


async def test_node_filter_by_label():
    parent = Node()
    child1 = Node()
    child2 = Node()

    parent_c1 = Link(source_id=parent.graph_id, target_id=child1.graph_id, label="c1")
    parent_c2 = Link(source_id=parent.graph_id, target_id=child2.graph_id, label="c2")

    await write_many.a([parent, child1, child2, parent_c1, parent_c2])

    assert await read_one.a(Node, NodeFilter(label="c1")) == child1
