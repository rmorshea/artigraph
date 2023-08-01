from typing import Any, ClassVar, Optional

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.base import Base


class Node(Base):
    """A base class for describing a node in a graph."""

    __tablename__ = "artigraph_node"
    __mapper_args__: ClassVar[dict[str, Any]] = {
        "polymorphic_identity": "node",
        "polymorphic_on": "node_type",
    }

    node_parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("artigraph_node.node_id"))
    """The ID of the parent node."""

    node_id: Mapped[int] = mapped_column(primary_key=True, init=False)
    """The unique ID of this node"""

    node_type: Mapped[str] = mapped_column(nullable=False, init=False)
    """The type of the node link."""


class NodeMetadata(Base):
    """A tag for a node."""

    __tablename__ = "node_metadata"
    __table_args__ = (UniqueConstraint("node_id", "key"),)

    id: Mapped[int] = mapped_column(primary_key=True, init=False)  # noqa: A003
    """The unique ID of this node metadata."""

    node_id: Mapped[int] = mapped_column(ForeignKey("artigraph_node.node_id"))
    """The ID of the node that this metadata is associated with."""

    key: Mapped[str] = mapped_column(nullable=False)
    """The key of this metadata."""

    value: Mapped[str] = mapped_column(nullable=True)
    """The value of this metadata."""
