from datetime import datetime
from typing import Any, ClassVar, Optional

from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class Span(Node):
    """A base class for describing a span of a pipeline."""

    polymorphic_identity = "span"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    span_opened_at: Mapped[Optional[datetime]] = mapped_column(default=None)
    """The time that this span opened."""

    span_closed_at: Mapped[Optional[datetime]] = mapped_column(default=None)
    """The time that this span closed."""
