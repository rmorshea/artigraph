from __future__ import annotations

from typing import Any, ClassVar, Optional

from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class Group(Node):
    """A group of nodes"""

    polymorphic_identity = "group"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    group_label: Mapped[Optional[str]] = mapped_column(default=None)
    """The label of this group."""
