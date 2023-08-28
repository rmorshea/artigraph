from __future__ import annotations

from dataclasses import dataclass, field
from typing import ClassVar, Sequence, TypeVar

from typing_extensions import Self

from artigraph.api.filter import NodeLinkFilter
from artigraph.api.func import Api
from artigraph.orm.base import make_uuid
from artigraph.orm.link import OrmNodeLink

L = TypeVar("L", bound=OrmNodeLink)


@dataclass
class NodeLink(Api[L]):
    """A wrapper around an ORM node link record."""

    orm_type: ClassVar[type[L]] = OrmNodeLink
    """The ORM type for this node."""

    child_id: str
    """The ID of the child node."""

    parent_id: str | None = None
    """The ID of the parent node."""

    label: str | None = None
    """A label for the link."""

    link_id: str = field(default_factory=make_uuid)
    """The unique ID of this link"""

    def filters(self) -> dict[type[OrmNodeLink], NodeLinkFilter]:
        return {OrmNodeLink: NodeLinkFilter(link_id=self.link_id)}

    async def to_orms(self) -> Sequence[OrmNodeLink]:
        return [
            OrmNodeLink(
                link_id=self.link_id,
                child_id=self.child_id,
                parent_id=self.parent_id,
                label=self.label,
            )
        ]

    @classmethod
    async def from_orm(cls, orm: L, /) -> Self:
        return cls(
            child_id=orm.child_id,
            parent_id=orm.parent_id,
            label=orm.label,
            api_orm=orm,
        )
