from __future__ import annotations

from collections import defaultdict
from collections.abc import Collection
from dataclasses import fields
from typing import Any, ClassVar, Mapping, Sequence, TypeVar, runtime_checkable

from sqlalchemy import Row, select
from sqlalchemy import delete as sql_delete
from typing_extensions import Protocol, Self

from artigraph.api.filter import Filter, MultiFilter
from artigraph.db import current_session
from artigraph.orm.base import OrmBase, get_poly_graph_orm_type
from artigraph.utils.misc import TaskBatch

O = TypeVar("O", bound=OrmBase)  # noqa: E741
R = TypeVar("R", bound=OrmBase)
G = TypeVar("G", bound="GraphType")
F = TypeVar("F", bound=Filter)


@runtime_checkable
class GraphType(Protocol[O, F, R]):
    """Protocol for objects that can be converted to and from Artigraph ORM records."""

    graph_orm_type: ClassVar[type[O]]
    """The ORM type that represents this object."""

    def graph_filter_self(self) -> F:
        """Get the filter for records of the ORM type that represent the object."""

    @classmethod
    def graph_filter_related(cls, where: F, /) -> Mapping[type[R], Filter]:
        """Get the filters for records of related ORM records required to construct this object."""

    async def graph_dump(self) -> Sequence[OrmBase]:
        """Dump the object to ORM records."""

    @classmethod
    async def graph_load(
        cls,
        records: Sequence[O],
        related_records: dict[type[R], Sequence[R]],
        /,
    ) -> Sequence[Self]:
        """Load ORM records into objects."""


async def exists(cls: type[GraphType], where: Filter) -> bool:
    """Check if records exist."""
    return await orm_exists(cls.graph_orm_type, where)


async def read_one(cls: type[G], where: Filter) -> G:
    """Read a record that matches the given filter."""
    one = await read_one_or_none(cls, where)
    if one is None:
        msg = f"No record found matching filter {where}"
        raise ValueError(msg)
    return one


async def read_one_or_none(cls: type[G], where: Filter) -> G | None:
    """Read a record that matches the given filter or None if no record is found."""
    record = await orm_read_one_or_none(cls.graph_orm_type, where)
    if record is None:
        return None
    related_records = {
        graph_orm_type: await orm_read(graph_orm_type, related_filter)
        for graph_orm_type, related_filter in cls.graph_filter_related(where).items()
    }
    return (await cls.graph_load([record], related_records))[0]


async def read(cls: type[G], where: Filter) -> Sequence[G]:
    """Read records that match the given filter."""
    records = await orm_read(cls.graph_orm_type, where)
    related_records = {
        graph_orm_type: await orm_read(graph_orm_type, api_filter)
        for graph_orm_type, api_filter in cls.graph_filter_related(where).items()
    }
    return await cls.graph_load(records, related_records)


async def delete_one(obj: GraphType) -> None:
    """Delete a record."""
    return await delete_many([obj])


async def delete_many(objs: Sequence[GraphType]) -> None:
    """Delete records."""
    if not objs:
        return

    filters_by_type: defaultdict[type[GraphType], list[Filter]] = defaultdict(list)
    for o in objs:
        filters_by_type[type(o)].append(o.graph_filter_self())

    async with current_session() as session:
        for o_type, o_filters in filters_by_type.items():
            where = o_filters[0] if len(o_filters) == 1 else MultiFilter(op="or", filters=o_filters)
            await delete(o_type, where)
        session.commit()


async def delete(cls: type[GraphType], where: Filter) -> None:
    """Delete records matching the given filter."""
    related_filters = cls.graph_filter_related(where)
    async with current_session():
        for o_type, o_where in related_filters.items():
            await orm_delete(o_type, o_where)
        # must delete this last since the related deletion queries may depend on it
        await orm_delete(cls.graph_orm_type, where)


async def write_one(obj: GraphType) -> None:
    """Create a record."""
    return await write([obj])


async def write(objs: Collection[GraphType]) -> None:
    """Create records and, if given, refresh their attributes."""
    records: TaskBatch[O] = TaskBatch()
    for o in objs:
        records.add(o.graph_dump)
    await orm_write([r for rs in await records.gather() for r in rs])


async def orm_exists(graph_orm_type: type[O], where: Filter) -> bool:
    """Check if ORM records exist."""
    async with current_session() as session:
        return bool((await session.execute(select(graph_orm_type).where(where.create()))).first())


async def orm_read_one_or_none(graph_orm_type: type[O], where: Filter) -> O | None:
    """Read an ORM record that matches the given filter or None if no record is found."""
    cmd = select(graph_orm_type.__table__).where(where.create())
    async with current_session() as session:
        orm = (await session.execute(cmd)).one_or_none()
    if orm is None:
        return None
    return load_orm_from_row(graph_orm_type, orm)


async def orm_read(graph_orm_type: type[O], where: Filter) -> Sequence[O]:
    """Read ORM records that match the given filter."""
    cmd = select(graph_orm_type.__table__).where(where.create())
    async with current_session() as session:
        rows = (await session.execute(cmd)).all()
    return load_orms_from_rows(graph_orm_type, rows)


async def orm_delete(graph_orm_type: type[O], where: Filter) -> None:
    """Delete ORM records that match the given filter."""
    cmd = sql_delete(graph_orm_type).where(where.create())
    async with current_session() as session:
        await session.execute(cmd)


async def orm_write(orm_objs: Collection[O]) -> None:
    """Create ORM records and, if given, refresh their attributes."""
    async with current_session() as session:
        session.add_all(orm_objs)
        await session.flush()


def load_orm_from_row(graph_orm_type: type[O], row: Row) -> O:
    """Load the appropriate ORM instance given a SQLAlchemy row."""
    return load_orms_from_rows(graph_orm_type, [row])[0]


def load_orms_from_rows(graph_orm_type: type[O], rows: Sequence[Row]) -> Sequence[O]:
    """Load the appropriate ORM instances given a sequence of SQLAlchemy rows."""
    poly_on = None
    for cls in graph_orm_type.mro():
        if (poly_on := cls.__dict__.get("__mapper_args__", {}).get("polymorphic_on")) is not None:
            break

    if not poly_on:
        init_field_names = {f.name for f in fields(graph_orm_type) if f.init}
        return [_make_non_poly_obj(graph_orm_type, init_field_names, row._mapping) for row in rows]
    else:
        table = graph_orm_type.__tablename__
        keys_by_graph_orm_type: dict[type[OrmBase], set[str]] = {}
        return [
            _make_poly_obj(table, poly_on, keys_by_graph_orm_type, row._mapping) for row in rows
        ]


def _make_poly_obj(
    table: str,
    poly_on: str,
    keys_by_graph_orm_type: dict[str, set[str]],
    row_mapping: dict[str, Any],
) -> O:
    poly_id = row_mapping[poly_on]
    graph_orm_type = get_poly_graph_orm_type(table, poly_id)
    if graph_orm_type not in keys_by_graph_orm_type:
        keys = keys_by_graph_orm_type[graph_orm_type] = _get_init_field_names(graph_orm_type)
    else:
        keys = keys_by_graph_orm_type[graph_orm_type]
    return _make_non_poly_obj(graph_orm_type, keys, row_mapping)


def _get_init_field_names(graph_orm_type: type[O]) -> set[str]:
    """Get the names of the fields that should be initialized."""
    return {f.name for f in fields(graph_orm_type) if f.init}


def _make_non_poly_obj(
    graph_orm_type: type[O],
    keys: set[str],
    row_mapping: dict[str, Any],
) -> O:
    """Create an ORM object from a SQLAlchemy row."""
    kwargs = {k: row_mapping[k] for k in keys}
    return graph_orm_type(**kwargs)
