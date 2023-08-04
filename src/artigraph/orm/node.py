from datetime import datetime
from typing import Any, ClassVar, Optional

from sqlalchemy import ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.base import Base


class Node(Base):
    """A base class for describing a node in a graph."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        # Table args cannot be define on subclasses without a __tablename__ but this is
        # inconvenient and somewhat defeats the purpose of using a base class. Instead
        # transfer the table args to the first subclass that has a __tablename__ before
        # SQLAlchemy complains. This is safe since we're using single table inheritance
        # and the table args are the same for all subclasses.
        if "__table_args__" in cls.__dict__:
            table_args = cls.__table_args__
            for parent_cls in cls.mro():  # nocov (this)
                if hasattr(parent_cls, "__tablename__"):
                    cls.__table_args__ += table_args
                    break
            del cls.__table_args__

        if "polymorphic_identity" in cls.__dict__:
            cls.__mapper_args__ = {
                **cls.__mapper_args__,
                "polymorphic_identity": cls.polymorphic_identity,
            }

        if "__mapper_args__" in cls.__dict__:
            if "polymorphic_on" in cls.__mapper_args__:
                cls.polymorphic_identity = cls.__mapper_args__["polymorphic_on"]

        Node.polymorphic_identity_mapping[cls.polymorphic_identity] = cls

        super().__init_subclass__(**kwargs)

    polymorphic_identity: ClassVar[str] = "node"
    """The type of the node - should be overridden by subclasses and passed to mapper args."""

    polymorphic_identity_mapping: ClassVar[dict[str, type["Node"]]] = {}
    """A mapping of node types to their subclasses."""

    __tablename__ = "artigraph_node"
    __mapper_args__: ClassVar[dict[str, Any]] = {
        "polymorphic_identity": polymorphic_identity,
        "polymorphic_on": "node_type",
    }

    node_parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("artigraph_node.node_id"))
    """The ID of the parent node."""

    node_id: Mapped[int] = mapped_column(primary_key=True, init=False)
    """The unique ID of this node"""

    node_type: Mapped[str] = mapped_column(nullable=False, init=False)
    """The type of the node link."""

    node_created_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default_factory=func.now,
        init=False,
    )
    """The time that this node link was created."""

    node_updated_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default_factory=func.now,
        onupdate=func.now(),
        init=False,
    )
    """The time that this node link was last updated."""
