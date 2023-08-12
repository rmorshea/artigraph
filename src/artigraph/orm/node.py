from __future__ import annotations

import sys
from datetime import datetime
from typing import Any, ClassVar, Optional, Sequence, TypeVar

from sqlalchemy import ForeignKey, func
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.base import Base
from artigraph.utils import get_subclasses

T = TypeVar("T")

NODE_TYPE_BY_POLYMORPHIC_IDENTITY: dict[str, type[Node]] = {}


_node_dataclass_kwargs = {} if sys.version_info < (3, 10) else {"kw_only": True}


def get_polymorphic_identities(
    node_types: Sequence[type[Node]],
    *,
    subclasses: bool = False,
) -> Sequence[str]:
    """Get the polymorphic identities of the given node types and optionall their subclasses."""
    node_types = [s for c in node_types for s in get_subclasses(c)] if subclasses else node_types
    return [nt.polymorphic_identity for nt in node_types if not nt.is_abstract()]


class Node(Base, **_node_dataclass_kwargs):
    """A base class for describing a node in a graph."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls._shuttle_table_args()
        cls._set_polymorphic_identity()
        if not cls.__mapper_args__.get("polymorphic_abstract"):
            NODE_TYPE_BY_POLYMORPHIC_IDENTITY[cls.polymorphic_identity] = cls
        super().__init_subclass__(**kwargs)

    @classmethod
    def is_abstract(cls) -> bool:
        """Returns True if the class is abstract. That is, it defines a polymorphic identity."""
        return "polymorphic_identity" not in cls.__dict__

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
        poly_id: str
        for c in cls.mro():
            mapper_args = getattr(c, "__mapper_args__", {})
            if "polymorphic_identity" in mapper_args:
                poly_id = mapper_args["polymorphic_identity"]
                break
        else:  # nocov
            msg = "No polymorphic_identity found in mro"
            raise TypeError(msg)
        if poly_id != cls.polymorphic_identity:
            msg = (
                f"polymorphic_identity class attribute {cls.polymorphic_identity!r} "
                f"does not match value from __mapper_args__ {poly_id!r}"
            )
            raise ValueError(msg)


# Have to manually add Node to NODE_TYPE_BY_POLYMORPHIC_IDENTITY
NODE_TYPE_BY_POLYMORPHIC_IDENTITY[Node.polymorphic_identity] = Node
