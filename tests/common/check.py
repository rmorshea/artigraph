import asyncio
from typing import Sequence

from artigraph.core.api.filter import Filter
from artigraph.core.api.funcs import delete_one, exists, read_one, write_one
from artigraph.core.api.proto import GraphLike


async def check_can_read_write_delete_one(
    value: GraphLike,
    *,
    self_filter: Filter,
    related_filters: Sequence[tuple[type[GraphLike], Filter]] = (),
):
    await write_one(value)

    db_value = await read_one(type(value), self_filter)
    assert db_value == value

    # check existance after since read gives a better error
    await check_exists(*related_filters)

    await delete_one(value)
    await check_not_exists((type(value), self_filter), *related_filters)


async def check_not_exists(*filters: tuple[type[GraphLike], Filter]) -> None:
    assert not any(await asyncio.gather(*[exists(t, f) for t, f in filters]))


async def check_exists(*filters: tuple[type[GraphLike], Filter]) -> None:
    assert all(await asyncio.gather(*[exists(t, f) for t, f in filters]))
