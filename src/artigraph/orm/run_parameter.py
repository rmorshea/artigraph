from sqlalchemy import JSON, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.base import Base


class RunParameter(Base):
    """A parameter of a run."""

    __tablename__ = "run_parameter"

    id: Mapped[int] = mapped_column(primary_key=True, init=False)  # noqa: A003
    """The unique ID of this parameter."""

    run_id: Mapped[int] = mapped_column(ForeignKey("run.id"))
    """The ID of the run that produced this parameter."""

    key: Mapped[str] = mapped_column(nullable=False)
    """The name of the parameter."""

    value: Mapped[str] = mapped_column(JSON, nullable=False)
    """The value of the parameter."""
