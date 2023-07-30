from datetime import datetime
from typing import Any, ClassVar, Optional

from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class Run(Node):
    """A run of a pipeline."""

    __tablename__ = "run"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "run"}

    version: Mapped[str] = mapped_column(nullable=False)
    """The version of the pipeline that was run (e.g. a Git commit hash)."""

    description: Mapped[str] = mapped_column(nullable=False)
    """A description of this run, providing more context about its purpose."""

    finished_at: Mapped[Optional[datetime]] = mapped_column()
    """The time that this run finished."""
