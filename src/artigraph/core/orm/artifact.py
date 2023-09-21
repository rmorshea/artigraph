from __future__ import annotations

from typing import Any, ClassVar

from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from artigraph.core.orm.node import OrmNode


class OrmArtifact(OrmNode):
    """A base class for artifacts."""

    __table_args__ = (UniqueConstraint("node_source_id", "artifact_label"),)
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_abstract": True}

    artifact_serializer: Mapped[str | None] = mapped_column(index=True)
    """The name of the serializer used to serialize the artifact."""


class OrmRemoteArtifact(OrmArtifact):
    """An artifact saved via a storage backend."""

    polymorphic_identity = "remote_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    remote_artifact_storage: Mapped[str] = mapped_column(nullable=True, index=True)
    """The name of the storage method for the artifact."""

    remote_artifact_location: Mapped[str] = mapped_column(nullable=True, index=True)
    """A string describing where the artifact is stored."""


class OrmDatabaseArtifact(OrmArtifact):
    """An artifact saved directly in the database."""

    polymorphic_identity = "database_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    database_artifact_data: Mapped[bytes | None]
    """The data of the artifact."""


class OrmModelArtifact(OrmDatabaseArtifact):
    """An artifact that is a model."""

    polymorphic_identity = "model_artifact"
    __mapper_args__: ClassVar[dict[str, Any]] = {"polymorphic_identity": polymorphic_identity}

    model_artifact_type_name: Mapped[str] = mapped_column(nullable=True, index=True)
    """The type of the model."""

    model_artifact_version: Mapped[int] = mapped_column(nullable=True, index=True)
    """The version of the model."""
