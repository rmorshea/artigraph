from datetime import datetime
from typing import Any, ClassVar, Optional

from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class Run(Node):
    """A run of a pipeline."""

    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "run"}

    run_version: Mapped[str] = mapped_column(nullable=True)
    """The version of the pipeline that was run (e.g. a Git commit hash)."""

    run_description: Mapped[str] = mapped_column(nullable=True)
    """A description of this run, providing more context about its purpose."""

    run_finished_at: Mapped[Optional[datetime]] = mapped_column(default=None)
    """The time that this run finished."""
