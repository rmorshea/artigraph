from __future__ import annotations

import abc
from typing import ClassVar, Generic, Mapping, Sequence, TypeVar
from uuid import UUID

from typing_extensions import Self

from artigraph.core.api.filter import Filter
from artigraph.core.orm.base import OrmBase

S = TypeVar("S", bound=OrmBase)
R = TypeVar("R", bound=OrmBase)
F = TypeVar("F", bound=Filter)


class GraphObject(abc.ABC, Generic[S, R, F]):
    """Base for objects that can be converted to and from Artigraph ORM records."""

    graph_id: UUID
    """The ID of the object."""

    graph_orm_type: ClassVar[type[OrmBase]]
    """The ORM type that represents this object."""

    @abc.abstractmethod
    def graph_filter_self(self) -> F:
        """Get the filter for records of the ORM type that represent the object."""
        raise NotImplementedError()

    @abc.abstractmethod
    async def graph_dump_self(self) -> S:
        """Dump the object into an ORM record."""
        raise NotImplementedError()

    @abc.abstractmethod
    async def graph_dump_related(self) -> Sequence[R]:
        """Dump all other related objects into ORM records."""
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def graph_filter_related(cls, self_filter: Filter, /) -> Mapping[type[R], Filter]:
        """Get the filters for records of related ORM records required to construct this object."""
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    async def graph_load(
        cls,
        self_records: Sequence[S],
        related_records: dict[type[R], Sequence[R]],
        /,
    ) -> Sequence[Self]:
        """Load ORM records into objects."""
        raise NotImplementedError()
