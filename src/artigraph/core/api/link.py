from __future__ import annotations

from dataclasses import field
from typing import ClassVar, Sequence, TypeVar
from uuid import UUID, uuid1

from artigraph.core.api.filter import NodeLinkFilter
from artigraph.core.orm.link import OrmNodeLink
from artigraph.core.utils.misc import Dataclass

L = TypeVar("L", bound=OrmNodeLink)


class NodeLink(Dataclass):
    """A wrapper around an ORM node link record."""

    graph_orm_type: ClassVar[type[L]] = OrmNodeLink
    """The ORM type for this node."""

    child_id: UUID | None = None
    """The ID of the child node."""

    parent_id: UUID | None = None
    """The ID of the parent node."""

    label: str | None = None
    """A label for the link."""

    link_id: UUID = field(default_factory=uuid1)
    """The unique ID of this link"""

    def graph_filter_self(self) -> NodeLinkFilter:
        return NodeLinkFilter(link_id=self.link_id)

    @classmethod
    def graph_filter_related(cls, _: NodeLinkFilter) -> dict:
        return {}

    async def graph_dump_self(self) -> OrmNodeLink:
        return OrmNodeLink(
            link_id=self.link_id,
            child_id=self.child_id,
            parent_id=self.parent_id,
            label=self.label,
        )

    async def graph_dump_related(self) -> Sequence[OrmNodeLink]:
        return []

    @classmethod
    async def graph_load(cls, self_records: Sequence[OrmNodeLink], _: dict) -> NodeLink:
        return [
            cls(
                link_id=r.link_id,
                child_id=r.child_id,
                parent_id=r.parent_id,
                label=r.label,
            )
            for r in self_records
        ]
