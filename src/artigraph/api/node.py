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

from artigraph.api.filter import NodeFilter, NodeLinkFilter
from artigraph.api.link import NodeLink
from artigraph.orm.link import OrmNodeLink
from artigraph.orm.node import OrmNode
from artigraph.utils.misc import Dataclass

N = TypeVar("N", bound=OrmNode)


class Node(Dataclass, Generic[N]):
    """A wrapper around an ORM node record."""

    orm_type: ClassVar[type[N]] = OrmNode
    """The ORM type for this node."""

    id: UUID = field(default_factory=uuid1)  # noqa: A003
    """The unique ID of this node"""

    parent_links: Sequence[NodeLink] = ()
    """The links to this node from its parents"""

    child_links: Sequence[NodeLink] = ()
    """The links from this node to its children"""

    def orm_filter_self(self) -> NodeFilter[Any]:
        return NodeFilter(node_id=self.id)

    @classmethod
    def orm_filter_related(cls, where: NodeFilter[Any]) -> dict[type[OrmNodeLink], NodeLinkFilter]:
        return {OrmNodeLink: NodeLinkFilter(parent=where) | NodeLinkFilter(child=where)}

    async def orm_dump(self) -> Sequence[OrmNode]:
        return [
            OrmNode(node_id=self.id),
            *[o for link in self.parent_links for o in await link.orm_dump()],
            *[o for link in self.child_links for o in await link.orm_dump()],
        ]

    @classmethod
    async def orm_load(
        cls,
        records: Sequence[N],
        related_records: dict[type[OrmNodeLink], Sequence[OrmNodeLink]],
    ) -> Sequence[Self]:
        parent_links, child_links = await cls.orm_load_parent_and_child_links(related_records)
        return [
            cls(
                id=r.node_id,
                parent_links=parent_links.get(r.node_id, ()),
                child_links=child_links.get(r.node_id, ()),
            )
            for r in records
        ]

    @classmethod
    async def orm_load_parent_and_child_links(
        cls, related_records: dict[type[OrmNodeLink], Sequence[OrmNodeLink]]
    ) -> tuple[dict[UUID, NodeLink], dict[UUID, NodeLink]]:
        parent_links: defaultdict[str, list[OrmNodeLink]] = defaultdict(list)
        child_links: defaultdict[str, list[OrmNodeLink]] = defaultdict(list)
        for link in related_records[OrmNodeLink]:
            parent_links[link.parent_id].append(link)
            child_links[link.child_id].append(link)
        return (
            {k: await NodeLink.orm_load(v, {}) for k, v in parent_links.items()},
            {k: await NodeLink.orm_load(v, {}) for k, v in child_links.items()},
        )
