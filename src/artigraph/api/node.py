from __future__ import annotations

from dataclasses import field
from logging import Filter
from typing import (
    ClassVar,
    Sequence,
    TypeVar,
)

from artigraph.api.filter import NodeFilter, NodeLinkFilter
from artigraph.orm.base import OrmBase, make_uuid
from artigraph.orm.link import OrmNodeLink
from artigraph.orm.node import OrmNode

N = TypeVar("N", bound=OrmNode)


class Node:
    """A wrapper around an ORM node record."""

    orm_type: ClassVar[type[N]] = OrmNode

    node_id: str = field(default_factory=make_uuid)
    """The unique ID of this node"""

    def __post_init__(self, api_orm: N | None) -> None:
        if api_orm is not None:
            self.node_id = api_orm.node_id
        super().__post_init__(api_orm)

    def filters(self) -> dict[type[OrmBase], Filter]:
        return {
            # select this node
            OrmNode: NodeFilter(node_id=self.node_id),
            # select all links to/from this node
            OrmNodeLink: NodeLinkFilter(parent=self.node_id) | NodeLinkFilter(child=self.node_id),
        }

    async def to_orms(self) -> Sequence[OrmNode]:
        return [OrmNode(node_id=self.node_id)]

    @classmethod
    async def from_orm(cls, orm: N, /) -> Node:
        return cls(node_id=orm.node_id, orm=orm)
