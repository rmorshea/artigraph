from __future__ import annotations

from abc import abstractclassmethod, abstractmethod
from collections import defaultdict
from collections.abc import Collection
from dataclasses import InitVar, field, fields
from datetime import datetime
from typing import (
    Any,
    ClassVar,
    Generic,
    Sequence,
    TypeVar,
)

from sqlalchemy import Row, select
from sqlalchemy import delete as sql_delete
from typing_extensions import ParamSpec, Self

from artigraph.api.filter import Filter, MultiFilter
from artigraph.db import current_session
from artigraph.orm.base import OrmBase, get_poly_orm_type
from artigraph.utils import Dataclass, TaskBatch

P = ParamSpec("P")
T = TypeVar("T")
O = TypeVar("O", bound=OrmBase)  # noqa: E741
A = TypeVar("A", bound="Api")


class Api(Generic[O], Dataclass):
    """An interface for saving and loading database records."""

    orm_type: ClassVar[type[O]] = OrmBase
    """The ORM type for this API object."""

    orm: InitVar[O | None] = None
    """The object relational mapping."""

    created_at: datetime = field(init=False, compare=False)
    """The time at which the underlying record was created."""

    updated_at: datetime = field(init=False, compare=False)
    """The time at which the underlying record was last updated."""

    def __post_init__(self, orm: O | None) -> None:
        if orm is not None:
            self.created_at = orm.created_at
            self.updated_at = orm.updated_at
        self._frozen = True

    @abstractmethod
    def filters(self) -> dict[type[OrmBase], Filter]:
        """Create filters for the database records this API object represents."""
        raise NotImplementedError()

    @abstractmethod
    async def to_orms(self) -> Sequence[OrmBase]:
        raise NotImplementedError()

    @abstractclassmethod
    async def from_orm(cls, orm_obj: O, /) -> Self:
        raise NotImplementedError()

    def __setattr__(self, name: str, value: Any) -> None:
        if self._frozen:
            msg = f"Cannot set {name!r} - {self.__class__.__name__} is immutable"
            raise TypeError(msg)
        super().__setattr__(name, value)


# Don't want this to show up as a field in the dataclass
Api._frozen = False


async def exists(api_type: type[Api], api_filter: Filter) -> bool:
    """Check if records exist."""
    return await orm_exists(api_type.orm_type, api_filter)


async def read_one(api_type: type[A], api_filter: Filter) -> A:
    """Read a record that matches the given filter."""
    return await api_type.from_orm(await orm_read_one(api_type.orm_type, api_filter))


async def read_one_or_none(api_type: type[A], api_filter: Filter) -> O | None:
    """Read a record that matches the given filter or None if no record is found."""
    orm = await orm_read_one_or_none(api_filter)
    return None if orm is None else await api_type.from_orm(orm)


async def read(api_type: type[A], api_filter: Filter) -> Sequence[A]:
    """Read records that match the given filter."""
    orms = await orm_read(api_type.orm_type, api_filter)
    return await TaskBatch().map(api_type.from_orm, orms).gather()


async def delete_one(api_obj: Api) -> None:
    """Delete a record."""
    await delete([api_obj])


async def delete(api_objs: Sequence[Api]) -> None:
    """Delete records matching the given filter."""
    filters_by_type: defaultdict[type[OrmBase], list[Filter]] = defaultdict(list)
    for obj in api_objs:
        for orm_type, api_filter in obj.filters().items():
            filters_by_type[orm_type].append(api_filter)
    filter_by_type: dict[type[OrmBase], Filter] = {
        orm_type: MultiFilter(op="or", filters=filters)
        for orm_type, filters in filters_by_type.items()
    }
    async with current_session() as session:
        for orm_type, api_filter in filter_by_type.items():
            cmd = sql_delete(orm_type).where(api_filter.create())
            await session.execute(cmd)


async def write_one(api_obj: Api) -> None:
    """Create a record."""
    return await write([api_obj])


async def write(api_objs: Collection[Api[O]]) -> None:
    """Create records and, if given, refresh their attributes."""
    orms: TaskBatch[O] = TaskBatch()
    for obj in api_objs:
        orms.add(obj.to_orms)
    await orm_write([o for os in await orms.gather() for o in os])


async def orm_exists(orm_type: type[O], api_filter: Filter) -> bool:
    """Check if ORM records exist."""
    async with current_session() as session:
        return bool((await session.execute(select(orm_type).where(api_filter.create()))).first())


async def orm_read_one(orm_type: type[O], api_filter: Filter) -> O:
    """Read an ORM records that matches the given filter."""
    record = await orm_read_one_or_none(orm_type, api_filter)
    if record is None:
        msg = f"No records found for filter {api_filter!r}"
        raise ValueError(msg)
    return record


async def orm_read_one_or_none(orm_type: type[O], api_filter: Filter) -> O | None:
    """Read an ORM record that matches the given filter or None if no record is found."""
    cmd = select(orm_type.__table__).where(api_filter.create())
    async with current_session() as session:
        orm = (await session.execute(cmd)).one_or_none()
    if orm is None:
        return None
    return load_orm_from_row(orm_type, orm)


async def orm_read(orm_type: type[O], api_filter: Filter) -> Sequence[O]:
    """Read ORM records that match the given filter."""
    cmd = select(orm_type.__table__).where(api_filter.create())
    async with current_session() as session:
        rows = (await session.execute(cmd)).all()
    return load_orms_from_rows(orm_type, rows)


async def orm_delete(orm_type: type[O], api_filter: Filter) -> None:
    """Delete ORM records that match the given filter."""
    cmd = sql_delete(orm_type).where(api_filter.create())
    async with current_session() as session:
        await session.execute(cmd)


async def orm_write(orm_objs: Collection[O]) -> None:
    """Create ORM records and, if given, refresh their attributes."""
    async with current_session() as session:
        session.add_all(orm_objs)
        await session.flush()


def load_orm_from_row(orm_type: type[O], row: Row) -> O:
    """Load the appropriate ORM instance given a SQLAlchemy row."""
    return load_orms_from_rows(orm_type, [row])[0]


def load_orms_from_rows(orm_type: type[O], rows: Sequence[Row]) -> Sequence[O]:
    """Load the appropriate ORM instances given a sequence of SQLAlchemy rows."""
    poly_on = None
    for cls in orm_type.mro():
        if (poly_on := cls.__mapper_args__.get("polymorphic_on")) is not None:
            break

    if not poly_on:
        init_field_names = {f.name for f in fields(orm_type) if f.init}
        return [_make_non_poly_obj(orm_type, init_field_names, row._mapping) for row in rows]
    else:
        table = orm_type.__tablename__
        keys_by_orm_type: dict[type[OrmBase], set[str]] = {}
        return [_make_poly_obj(table, poly_on, keys_by_orm_type, row._mapping) for row in rows]


def _make_poly_obj(
    table: str,
    poly_on: str,
    keys_by_orm_type: dict[str, set[str]],
    row_mapping: dict[str, Any],
) -> O:
    poly_id = row_mapping[poly_on]
    orm_type = get_poly_orm_type(table, poly_id)
    if orm_type not in keys_by_orm_type:
        keys = keys_by_orm_type[orm_type] = _get_init_field_names(orm_type)
    else:
        keys = keys_by_orm_type[orm_type]
    return _make_non_poly_obj(orm_type, keys, row_mapping)


def _get_init_field_names(orm_type: type[O]) -> set[str]:
    """Get the names of the fields that should be initialized."""
    return {f.name for f in fields(orm_type) if f.init}


def _make_non_poly_obj(
    orm_type: type[O],
    keys: set[str],
    row_mapping: dict[str, Any],
) -> O:
    """Create an ORM object from a SQLAlchemy row."""
    kwargs = {k: row_mapping[k] for k in keys}
    return orm_type(**kwargs)
