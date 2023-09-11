from __future__ import annotations

from collections import defaultdict
from dataclasses import field
from typing import (
    Any,
    ClassVar,
    Generic,
    Sequence,
    TypeVar,
)
from uuid import UUID, uuid1

from typing_extensions import Self

from artigraph.core.api.filter import NodeFilter, NodeLinkFilter
from artigraph.core.api.funcs import dump
from artigraph.core.api.link import NodeLink
from artigraph.core.orm.link import OrmNodeLink
from artigraph.core.orm.node import OrmNode
from artigraph.core.utils.misc import Dataclass

N = TypeVar("N", bound=OrmNode)


class Node(Dataclass, Generic[N]):
    """A wrapper around an ORM node record."""

    graph_orm_type: ClassVar[type[N]] = OrmNode
    """The ORM type for this node."""

    node_id: UUID = field(default_factory=uuid1)
    """The unique ID of this node"""

    parent_links: Sequence[NodeLink] = ()
    """The links to this node from its parents"""

    child_links: Sequence[NodeLink] = ()
    """The links from this node to its children"""

    def graph_filter_self(self) -> NodeFilter[Any]:
        return NodeFilter(node_id=self.node_id)

    @classmethod
    def graph_filter_related(
        cls, where: NodeFilter[Any]
    ) -> dict[type[OrmNodeLink], NodeLinkFilter]:
        return {OrmNodeLink: NodeLinkFilter(parent=where) | NodeLinkFilter(child=where)}

    async def graph_dump_self(self) -> OrmNode:
        return OrmNode(node_id=self.node_id)

    async def graph_dump_related(self) -> Sequence[OrmNode]:
        return await dump(self.child_links + self.parent_links)

    @classmethod
    async def graph_load(
        cls,
        self_records: Sequence[N],
        related_records: dict[type[OrmNodeLink], Sequence[OrmNodeLink]],
    ) -> Sequence[Self]:
        parent_links, child_links = await cls.graph_load_parent_and_child_links(related_records)
        return [
            cls(
                node_id=r.node_id,
                parent_links=parent_links.get(r.node_id, ()),
                child_links=child_links.get(r.node_id, ()),
            )
            for r in self_records
        ]

    @classmethod
    async def graph_load_parent_and_child_links(
        cls, related_records: dict[type[OrmNodeLink], Sequence[OrmNodeLink]]
    ) -> tuple[dict[UUID, NodeLink], dict[UUID, NodeLink]]:
        parent_links: defaultdict[str, list[OrmNodeLink]] = defaultdict(list)
        child_links: defaultdict[str, list[OrmNodeLink]] = defaultdict(list)
        for link in related_records[OrmNodeLink]:
            parent_links[link.child_id].append(link)
            child_links[link.parent_id].append(link)
        return (
            {k: await NodeLink.graph_load(v, {}) for k, v in parent_links.items()},
            {k: await NodeLink.graph_load(v, {}) for k, v in child_links.items()},
        )

    def __post_init__(self) -> None:
        for c in self.child_links:
            if c.parent_id != self.node_id:
                msg = f"Expected link to child {c!r} to have parent_id={self.node_id!r}"
                raise ValueError(msg)
        for p in self.parent_links:
            if p.child_id != self.node_id:
                msg = f"Expected link from parent {p!r} to have child_id={self.node_id!r}"
                raise ValueError(msg)
