from __future__ import annotations

import sys
from typing import Any, ClassVar, Sequence, TypeVar
from uuid import UUID

from sqlalchemy.orm import Mapped, mapped_column

from artigraph.core.orm.base import OrmBase
from artigraph.core.utils.misc import get_subclasses

T = TypeVar("T")


_node_dataclass_kwargs = {} if sys.version_info < (3, 10) else {"kw_only": True}


def get_polymorphic_identities(
    node_types: Sequence[type[OrmNode]],
    *,
    subclasses: bool = False,
) -> Sequence[str]:
    """Get the polymorphic identities of the given node types and optionall their subclasses."""
    node_types = [s for c in node_types for s in get_subclasses(c)] if subclasses else node_types
    return [nt.polymorphic_identity for nt in node_types if not nt.is_abstract()]


class OrmNode(OrmBase, **_node_dataclass_kwargs):
    """A base class for describing a node in a graph."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        cls._shuttle_table_args()
        cls._set_polymorphic_identity()
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

    id: Mapped[UUID] = mapped_column(primary_key=True)
    """The unique ID of this node"""

    node_type: Mapped[str] = mapped_column(nullable=False, init=False, index=True)
    """The type of the node link."""

    @classmethod
    def _shuttle_table_args(cls: type[OrmNode]) -> None:
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
    def _set_polymorphic_identity(cls: type[OrmNode]) -> None:
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
