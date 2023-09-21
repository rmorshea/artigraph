from __future__ import annotations

from uuid import UUID

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.core.orm.base import OrmBase
from artigraph.core.orm.node import OrmNode


class OrmLink(OrmBase):
    """A link between two nodes."""

    __tablename__ = "artigraph_link"
    __table_args__ = (
        UniqueConstraint("source_id", "target_id"),
        UniqueConstraint("source_id", "label"),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True)
    """The ID of the link."""
    target_id: Mapped[UUID] = mapped_column(ForeignKey(OrmNode.id), nullable=False, index=True)
    """The ID of the node to which this link points."""
    source_id: Mapped[UUID] = mapped_column(ForeignKey(OrmNode.id), nullable=False, index=True)
    """The ID of the node from which this link originates."""
    label: Mapped[str | None] = mapped_column(nullable=True, default=None, index=True)
    """A label for the link - labels must be unique for a given source node."""
