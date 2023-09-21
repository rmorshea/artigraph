from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection
from dataclasses import fields
from typing import Any, Sequence, TypeVar, cast

from sqlalchemy import Row, RowMapping, select
from sqlalchemy import delete as sql_delete

from artigraph.core.api.base import GraphObject
from artigraph.core.api.filter import Filter, MultiFilter
from artigraph.core.db import current_session
from artigraph.core.orm.base import (
    OrmBase,
    get_fk_dependency_rank,
    get_poly_graph_orm_type,
)
from artigraph.core.utils.anysync import anysync
from artigraph.core.utils.misc import TaskBatch

S = TypeVar("S", bound=OrmBase)
R = TypeVar("R", bound=OrmBase)
G = TypeVar("G", bound=GraphObject)


@anysync
async def exists(cls: type[GraphObject], where: Filter) -> bool:
    """Check if records exist."""
    return await orm_exists(cls.graph_orm_type, where)


@anysync
async def read_one(cls: type[G], where: Filter) -> G:
    """Read a record that matches the given filter."""
    one = await read_one_or_none.a(cls, where)
    if one is None:
        msg = f"No record found matching filter {where}"
        raise ValueError(msg)
    return one


@anysync
async def read_one_or_none(cls: type[G], where: Filter) -> G | None:
    """Read a record that matches the given filter or None if no record is found."""
    record = await orm_read_one_or_none(cls.graph_orm_type, where)
    if record is None:
        return None
    related_records = {
        graph_orm_type: await orm_read(graph_orm_type, related_filter)
        for graph_orm_type, related_filter in cls.graph_filter_related(where).items()
    }
    return cast(G, (await cls.graph_load([record], related_records))[0])


@anysync
async def read(cls: type[G], where: Filter) -> Sequence[G]:
    """Read records that match the given filter."""
    records = await orm_read(cls.graph_orm_type, where)
    related_records = {
        graph_orm_type: await orm_read(graph_orm_type, api_filter)
        for graph_orm_type, api_filter in cls.graph_filter_related(where).items()
    }
    return await cls.graph_load(records, related_records)


@anysync
async def delete_one(obj: GraphObject) -> None:
    """Delete a record."""
    return await delete_many.a([obj])


@anysync
async def delete_many(objs: Sequence[GraphObject]) -> None:
    """Delete records."""
    filters_by_type: defaultdict[type[GraphObject], list[Filter]] = defaultdict(list)
    for o in objs:
        filters_by_type[type(o)].append(o.graph_filter_self())

    async with current_session() as session:
        for o_type, o_filters in filters_by_type.items():
            where = o_filters[0] if len(o_filters) == 1 else MultiFilter(op="or", filters=o_filters)
            await delete.a(o_type, where)
        await session.commit()


@anysync
async def delete(cls: type[GraphObject], where: Filter) -> None:
    """Delete records matching the given filter."""
    related_filters = cls.graph_filter_related(where)
    async with current_session():
        for o_type in sorted(related_filters, key=get_fk_dependency_rank, reverse=True):
            await orm_delete(o_type, related_filters[o_type])
        # must delete this last since the related deletion queries may depend on it
        await orm_delete(cls.graph_orm_type, where)


@anysync
async def write_one(obj: GraphObject) -> None:
    """Create a record."""
    return await write_many.a([obj])


@anysync
async def write_many(objs: Collection[GraphObject]) -> None:
    """Create records and, if given, refresh their attributes."""
    await orm_write(await dump(objs))


async def dump_one(obj: GraphObject[S, R, Any]) -> tuple[S, Sequence[R]]:
    first, *rest = await dump_one_flat(obj)
    return first, rest  # type: ignore


async def dump_one_flat(obj: GraphObject[S, R, Any]) -> Sequence[S | R]:
    return await dump([obj])


async def dump(objs: Collection[GraphObject[S, R, Filter]]) -> Sequence[S | R]:
    """Dump objects into ORM records."""
    dump_self_records: TaskBatch[OrmBase] = TaskBatch()
    for o in objs:
        dump_self_records.add(o.graph_dump_self)

    dump_all_records: TaskBatch[Sequence[OrmBase]] = TaskBatch()

    # add self records first so that related records can depend on them if needed
    dump_all_records.add(dump_self_records.gather)

    for o in objs:
        dump_all_records.add(o.graph_dump_related)

    records_seqs = await dump_all_records.gather()
    return [cast(S | R, r) for rs in records_seqs for r in rs]


async def orm_exists(graph_orm_type: type[S], where: Filter) -> bool:
    """Check if ORM records exist."""
    async with current_session() as session:
        return bool((await session.execute(select(graph_orm_type).where(where.create()))).first())


async def orm_read_one_or_none(graph_orm_type: type[S], where: Filter) -> S | None:
    """Read an ORM record that matches the given filter or None if no record is found."""
    cmd = select(graph_orm_type.__table__).where(where.create())
    async with current_session() as session:
        orm = (await session.execute(cmd)).one_or_none()
    if orm is None:
        return None
    return load_orm_from_row(graph_orm_type, orm)


async def orm_read(graph_orm_type: type[S], where: Filter) -> Sequence[S]:
    """Read ORM records that match the given filter."""
    cmd = select(graph_orm_type.__table__).where(where.create())
    async with current_session() as session:
        rows = (await session.execute(cmd)).all()
    return load_orms_from_rows(graph_orm_type, rows)


async def orm_delete(graph_orm_type: type[S], where: Filter) -> None:
    """Delete ORM records that match the given filter."""
    cmd = sql_delete(graph_orm_type).where(where.create())
    async with current_session() as session:
        await session.execute(cmd)
        await session.flush()


async def orm_write(orm_objs: Collection[S]) -> None:
    """Create ORM records and, if given, refresh their attributes."""
    async with current_session() as session:
        for objs in _order_records_by_dependency_rank(orm_objs):
            session.add_all(objs)
            await session.flush()


def load_orm_from_row(graph_orm_type: type[S], row: Row) -> S:
    """Load the appropriate ORM instance given a SQLAlchemy row."""
    return load_orms_from_rows(graph_orm_type, [row])[0]


def load_orms_from_rows(graph_orm_type: type[S], rows: Sequence[Row]) -> Sequence[S]:
    """Load the appropriate ORM instances given a sequence of SQLAlchemy rows."""
    poly_on = None
    for cls in graph_orm_type.mro():
        if (poly_on := cls.__dict__.get("__mapper_args__", {}).get("polymorphic_on")) is not None:
            break

    if not poly_on:
        init_field_names = {f.name for f in fields(graph_orm_type) if f.init}
        return [_make_non_poly_obj(graph_orm_type, init_field_names, row._mapping) for row in rows]
    else:
        keys_by_graph_orm_type: dict[type[OrmBase], set[str]] = {}
        return [
            _make_poly_obj(graph_orm_type, poly_on, keys_by_graph_orm_type, row._mapping)
            for row in rows
        ]


def _make_poly_obj(
    graph_orm_type: type[S],
    poly_on: str,
    keys_by_graph_orm_type: dict[type[OrmBase], set[str]],
    row_mapping: RowMapping,
) -> S:
    poly_id = row_mapping[poly_on]
    specific_graph_orm_type = get_poly_graph_orm_type(graph_orm_type.__tablename__, poly_id)
    if specific_graph_orm_type not in keys_by_graph_orm_type:
        keys = keys_by_graph_orm_type[specific_graph_orm_type] = _get_init_field_names(
            specific_graph_orm_type
        )
    else:
        keys = keys_by_graph_orm_type[specific_graph_orm_type]
    return cast(S, _make_non_poly_obj(specific_graph_orm_type, keys, row_mapping))


def _get_init_field_names(graph_orm_type: type[S]) -> set[str]:
    """Get the names of the fields that should be initialized."""
    return {f.name for f in fields(graph_orm_type) if f.init}


def _make_non_poly_obj(
    graph_orm_type: type[S],
    keys: set[str],
    row_mapping: RowMapping,
) -> S:
    """Create an ORM object from a SQLAlchemy row."""
    kwargs = {k: row_mapping[k] for k in keys}
    return graph_orm_type(**kwargs)


def _order_records_by_dependency_rank(records: Collection[OrmBase]) -> Sequence[Sequence[OrmBase]]:
    """Order records by dependency rank in O(N)"""
    rank_by_graph_orm_type = {r.__tablename__: get_fk_dependency_rank(type(r)) for r in records}
    max_rank = max(rank_by_graph_orm_type.values() or [0])
    records_by_rank: list[list[OrmBase]] = [[] for _ in range(max_rank + 1)]
    for r in records:
        records_by_rank[rank_by_graph_orm_type[r.__tablename__]].append(r)
    return [records for records in records_by_rank if records]
