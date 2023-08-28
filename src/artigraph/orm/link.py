from __future__ import annotations

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.base import OrmBase
from artigraph.orm.node import OrmNode


class OrmNodeLink(OrmBase):
    """A link between two nodes."""

    __tablename__ = "artigraph_node_link"
    __table_args__ = (
        UniqueConstraint("parent_id", "child_id"),
        UniqueConstraint("parent_id", "label"),
    )

    link_id: Mapped[str] = mapped_column(primary_key=True)
    """The ID of the link."""

    child_id: Mapped[int] = mapped_column(ForeignKey(OrmNode.node_id), nullable=False)
    """The ID of the child node."""

    parent_id: Mapped[int | None] = mapped_column(ForeignKey(OrmNode.node_id), default=None)
    """The ID of the parent node."""

    label: Mapped[str | None] = mapped_column(nullable=True, default=None)
    """A label for the link."""
