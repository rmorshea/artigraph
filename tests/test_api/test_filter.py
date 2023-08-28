from datetime import datetime, timezone

from artigraph.api.filter import MultiFilter, NodeFilter, ValueFilter
from artigraph.api.func import orm_read_one, orm_write
from artigraph.orm.node import OrmNode


async def test_filter_by_node_created_and_update_at():
    node = OrmNode(node_id="test")
    created_at = node.created_at = datetime(1993, 2, 6, tzinfo=timezone.utc)
    updated_at = node.updated_at = datetime(1995, 7, 25, tzinfo=timezone.utc)

    await orm_write([node])

    assert (await orm_read_one(OrmNode, NodeFilter(created_at=created_at))).node_id == node.node_id
    assert (await orm_read_one(OrmNode, NodeFilter(updated_at=updated_at))).node_id == node.node_id


def test_multi_and_filter():
    vf1 = ValueFilter(gt=1).against(OrmNode.node_id)
    vf2 = ValueFilter(lt=3).against(OrmNode.node_id)
    vf3 = ValueFilter(eq=2).against(OrmNode.node_id)

    multi_filter = vf1 & vf2 & vf3
    assert multi_filter == MultiFilter(op="and", filters=(vf1, vf2, vf3))

    assert (
        multi_filter.create().compile().string
        == (vf1.create() & vf2.create() & vf3.create()).compile().string
    )


def test_multi_or_filter():
    vf1 = ValueFilter(gt=1).against(OrmNode.node_id)
    vf2 = ValueFilter(lt=3).against(OrmNode.node_id)
    vf3 = ValueFilter(eq=2).against(OrmNode.node_id)

    multi_filter = vf1 | vf2 | vf3
    assert multi_filter == MultiFilter(op="or", filters=(vf1, vf2, vf3))

    assert (
        multi_filter.create().compile().string
        == (vf1.create() | vf2.create() | vf3.create()).compile().string
    )


def test_multi_and_or_filter():
    vf1 = ValueFilter(gt=1).against(OrmNode.node_id)
    vf2 = ValueFilter(lt=3).against(OrmNode.node_id)
    vf3 = ValueFilter(eq=2).against(OrmNode.node_id)
    vf4 = ValueFilter(eq=4).against(OrmNode.node_id)

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
    vf1 = ValueFilter(gt=1).against(OrmNode.node_id)
    vf2 = ValueFilter(lt=3).against(OrmNode.node_id)
    vf3 = ValueFilter(eq=2).against(OrmNode.node_id)
    vf4 = ValueFilter(eq=4).against(OrmNode.node_id)

    multi_filter = (vf1 & vf2) & (vf3 & vf4)
    assert multi_filter == MultiFilter(
        op="and",
        filters=(vf1, vf2, vf3, vf4),
    )

    assert (
        multi_filter.create().compile().string
        == (vf1.create() & vf2.create() & vf3.create() & vf4.create()).compile().string
    )
