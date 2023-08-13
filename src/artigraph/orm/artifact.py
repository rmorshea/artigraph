from __future__ import annotations

from typing import Any, ClassVar, Optional

from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.orm.node import Node


class BaseArtifact(Node):
    """A base class for artifacts."""

    __table_args__ = (UniqueConstraint("node_parent_id", "artifact_label"),)
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_abstract": True}

    artifact_serializer: Mapped[str] = mapped_column(nullable=True)
    """The name of the serializer used to serialize the artifact."""

    artifact_label: Mapped[str] = mapped_column(nullable=True)
    """A label for the node."""


class RemoteArtifact(BaseArtifact):
    """An artifact saved via a storage backend."""

    polymorphic_identity = "remote_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    remote_artifact_storage: Mapped[str] = mapped_column(nullable=True)
    """The name of the storage method for the artifact."""

    remote_artifact_location: Mapped[str] = mapped_column(init=False, nullable=True)
    """A string describing where the artifact is stored."""


class DatabaseArtifact(BaseArtifact):
    """An artifact saved directly in the database."""

    polymorphic_identity = "database_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    database_artifact_value: Mapped[Optional[bytes]]
    """The data of the artifact."""


class ModelArtifact(DatabaseArtifact):
    """An artifact that is a model."""

    polymorphic_identity = "model_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    model_artifact_type: Mapped[str] = mapped_column(nullable=True)
    """The type of the model."""

    model_artifact_version: Mapped[int] = mapped_column(nullable=True)
    """The version of the model."""
