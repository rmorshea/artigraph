from typing import Any, ClassVar

from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class StorageArtifact(Node):
    """An artifact saved via a storage backend."""

    __tablename__ = "storage_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "storage_artifact"}

    location: Mapped[str] = mapped_column(nullable=False, init=False)
    """A string describing where the artifact is stored."""

    label: Mapped[str] = mapped_column(nullable=False)
    """A label for the artifact."""

    serializer: Mapped[str] = mapped_column(nullable=False)
    """The name of the serializer used to serialize the artifact."""

    storage: Mapped[str] = mapped_column(nullable=False)
    """The name of the storage method for the artifact."""


class DatabaseArtifact(Node):
    """An artifact saved directly in the database."""

    __tablename__ = "database_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": "database_artifact"}

    label: Mapped[str] = mapped_column(nullable=False)
    """A label for the artifact."""

    value: Mapped[Any] = mapped_column(JSON, nullable=False)
    """The data of the artifact."""
