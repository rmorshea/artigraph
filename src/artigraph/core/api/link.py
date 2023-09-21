from __future__ import annotations

from dataclasses import field
from typing import Any, ClassVar, Sequence, TypeVar
from uuid import UUID, uuid1

from typing_extensions import Self

from artigraph.core.api.base import GraphObject
from artigraph.core.api.filter import LinkFilter
from artigraph.core.orm.link import OrmLink
from artigraph.core.utils.misc import FrozenDataclass

L = TypeVar("L", bound=OrmLink)


class Link(FrozenDataclass, GraphObject[L, OrmLink, LinkFilter]):
    """A wrapper around an ORM node link record."""

    graph_orm_type: ClassVar[type[OrmLink]] = OrmLink
    """The ORM type for this node."""

    source_id: UUID
    """The ID of the parent node."""
    target_id: UUID
    """The ID of the child node."""
    label: str | None = None
    """A label for the link."""
    graph_id: UUID = field(default_factory=uuid1)
    """The unique ID of this link"""

    def graph_filter_self(self) -> LinkFilter:
        return LinkFilter(id=self.graph_id)

    @classmethod
    def graph_filter_related(cls, _: LinkFilter) -> dict:
        return {}

    async def graph_dump_self(self) -> OrmLink:
        return OrmLink(
            id=self.graph_id,
            target_id=self.target_id,
            source_id=self.source_id,
            label=self.label,
        )

    async def graph_dump_related(self) -> Sequence[Any]:
        return []

    @classmethod
    async def graph_load(cls, self_records: Sequence[OrmLink], _: dict) -> Sequence[Self]:
        return [
            cls(
                graph_id=r.id,
                target_id=r.target_id,
                source_id=r.source_id,
                label=r.label,
            )
            for r in self_records
        ]
