from __future__ import annotations

from dataclasses import field
from typing import (
    Any,
    ClassVar,
    Sequence,
    TypeVar,
)
from uuid import UUID, uuid1

from typing_extensions import Self

from artigraph.core.api.base import GraphObject
from artigraph.core.api.filter import Filter, LinkFilter, NodeFilter
from artigraph.core.orm.link import OrmLink
from artigraph.core.orm.node import OrmNode
from artigraph.core.utils.misc import FrozenDataclass

N = TypeVar("N", bound=OrmNode)


class Node(FrozenDataclass, GraphObject[N, OrmLink, NodeFilter[Any]]):
    """A wrapper around an ORM node record."""

    graph_orm_type: ClassVar[type[OrmNode]] = OrmNode
    """The ORM type for this node."""

    graph_id: UUID = field(default_factory=uuid1)
    """The unique ID of this node"""

    def graph_filter_self(self) -> NodeFilter[Any]:
        return NodeFilter(id=self.graph_id)

    async def graph_dump_self(self) -> OrmNode:
        return OrmNode(id=self.graph_id)

    async def graph_dump_related(self) -> Sequence[Any]:
        return []

    @classmethod
    def graph_filter_related(cls, where: NodeFilter[Any]) -> dict[type[OrmLink], Filter]:
        return {OrmLink: LinkFilter(parent=where) | LinkFilter(child=where)}

    @classmethod
    async def graph_load(
        cls,
        self_records: Sequence[N],
        related_records: dict[type[OrmLink], Sequence[OrmLink]],  # noqa: ARG003
    ) -> Sequence[Self]:
        return [
            cls(
                graph_id=r.id,
                **(await cls._graph_load_extra_kwargs(r)),
            )
            for r in self_records
        ]

    @classmethod
    async def _graph_load_extra_kwargs(
        cls,
        self_record: N,  # noqa: ARG003
    ) -> dict[str, Any]:
        return {}
