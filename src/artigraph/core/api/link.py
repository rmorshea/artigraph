from __future__ import annotations

from dataclasses import field
from typing import Any, ClassVar, Sequence, TypeVar
from uuid import UUID, uuid1

from typing_extensions import Self

from artigraph.core.api.base import GraphBase
from artigraph.core.api.filter import NodeLinkFilter
from artigraph.core.orm.link import OrmNodeLink
from artigraph.core.utils.misc import FrozenDataclass

L = TypeVar("L", bound=OrmNodeLink)


class NodeLink(FrozenDataclass, GraphBase[L, OrmNodeLink, NodeLinkFilter]):
    """A wrapper around an ORM node link record."""

    graph_orm_type: ClassVar[type[OrmNodeLink]] = OrmNodeLink
    """The ORM type for this node."""

    parent_id: UUID
    """The ID of the parent node."""
    child_id: UUID
    """The ID of the child node."""
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

    async def graph_dump_related(self) -> Sequence[Any]:
        return []

    @classmethod
    async def graph_load(cls, self_records: Sequence[OrmNodeLink], _: dict) -> Sequence[Self]:
        return [
            cls(
                link_id=r.link_id,
                child_id=r.child_id,
                parent_id=r.parent_id,
                label=r.label,
            )
            for r in self_records
        ]
