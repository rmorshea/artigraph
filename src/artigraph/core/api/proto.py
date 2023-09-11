from __future__ import annotations

from typing import ClassVar, Mapping, Sequence, TypeVar, runtime_checkable

from typing_extensions import Protocol, Self

from artigraph.core.api.filter import Filter
from artigraph.core.orm.base import OrmBase

S = TypeVar("S", bound=OrmBase)
R = TypeVar("R", bound=OrmBase)
G = TypeVar("G", bound="GraphLike")
F = TypeVar("F", bound=Filter)


@runtime_checkable
class GraphLike(Protocol[S, R, F]):
    """Protocol for objects that can be converted to and from Artigraph ORM records."""

    graph_orm_type: ClassVar[type[S]]
    """The ORM type that represents this object."""

    def graph_filter_self(self) -> F:
        """Get the filter for records of the ORM type that represent the object."""

    @classmethod
    def graph_filter_related(cls, self_filter: F, /) -> Mapping[type[R], Filter]:
        """Get the filters for records of related ORM records required to construct this object."""

    async def graph_dump_self(self) -> S:
        """Dump the object into an ORM record."""

    async def graph_dump_related(self) -> Sequence[R]:
        """Dump all other related objects into ORM records."""

    @classmethod
    async def graph_load(
        cls,
        self_records: Sequence[S],
        related_records: dict[type[R], Sequence[R]],
        /,
    ) -> Sequence[Self]:
        """Load ORM records into objects."""
