from datetime import datetime

from sqlalchemy import func
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column


class Base(DeclarativeBase, MappedAsDataclass):
    """A base class for all database models."""

    created_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default=func.now,
        init=False,
    )
    """The time that this node link was created."""

    updated_at: Mapped[datetime] = mapped_column(
        nullable=False,
        default=func.now,
        onupdate=func.now,
        init=False,
    )
    """The time that this node link was last updated."""
