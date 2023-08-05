from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar, Optional

from sqlalchemy import ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.base import Base

NODE_TYPE_BY_POLYMORPHIC_IDENTITY: dict[str, type[Node]] = {}


class Node(Base, kw_only=True):
    """A base class for describing a node in a graph."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls._shuttle_table_args()
        cls._set_polymorphic_identity()
        NODE_TYPE_BY_POLYMORPHIC_IDENTITY[cls.polymorphic_identity] = cls
        super().__init_subclass__(**kwargs)

    polymorphic_identity: ClassVar[str] = "node"
    """The type of the node - should be overridden by subclasses and passed to mapper args."""

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

    @classmethod
    def _shuttle_table_args(cls: type[Node]) -> None:
        """Transfer table args from non-table subclasses to the base which has a table.

        This method exists because __table_args__ cannot be define on subclasses without
        a __tablename__. Since we're using single table inheritance this effectively means
        subclasses cannot specify __table_args__. To work around this, we transfer the
        any __table_args__ defined on a subclass to the first base that has a __tablename__
        (which is Node) before SQLAlchemy complains.
        """
        if "__table_args__" in cls.__dict__:
            table_args = cls.__table_args__
            for parent_cls in cls.mro():  # nocov (this)
                if hasattr(parent_cls, "__tablename__"):
                    cls.__table_args__ += table_args
                    break
            del cls.__table_args__

    @classmethod
    def _set_polymorphic_identity(cls: type[Node]) -> None:
        """Sets a polymorphic identity attribute on the class for easier use."""
        mapper_args = getattr(cls, "__mapper_args__", {})
        poly_id_from_attr: str | None = cls.__dict__.get("polymorphic_identity")
        poly_id_from_args: str | None = mapper_args.get("polymorphic_identity")
        if poly_id_from_attr and poly_id_from_args:
            if poly_id_from_attr != poly_id_from_args:
                msg = f"polymorphic_identity defined on class and in __mapper_args__ but with different values: {poly_id_from_attr} != {poly_id_from_args}"
                raise ValueError(msg)
        elif poly_id_from_attr:
            cls.__mapper_args__ = {
                **cls.__mapper_args__,
                "polymorphic_identity": cls.polymorphic_identity,
            }
        elif poly_id_from_args:
            cls.polymorphic_identity = poly_id_from_args
