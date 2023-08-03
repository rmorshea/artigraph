from datetime import datetime
from typing import Any, ClassVar, Optional

from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class Run(Node):
    """A run of a pipeline."""

    polymorphic_identity = "run"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    run_finished_at: Mapped[Optional[datetime]] = mapped_column(default=None)
    """The time that this run finished."""
