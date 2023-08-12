from __future__ import annotations

from dataclasses import field
from typing import TYPE_CHECKING, Generic, Sequence, TypeVar

from artigraph.api.filter import (
    ArtifactFilter,
    Filter,
    NodeFilter,
    NodeTypeFilter,
    Query,
    ValueFilter,
)
from artigraph.orm.artifact import BaseArtifact, ModelArtifact

if TYPE_CHECKING:
    from artigraph.model.base import BaseModel


M = TypeVar("M", bound="BaseModel")


class ModelFilter(ArtifactFilter[ModelArtifact], Generic[M]):
    """A filter for models."""

    node_type: NodeTypeFilter[ModelArtifact] = field(
        # delay this in case tables are defined late
        default_factory=lambda: NodeTypeFilter(type_in=[ModelArtifact])
    )
    """Models must be one of these types."""

    is_root: bool = False
    """Model node must be the root node. That is, it's parent is not an artifact."""

    model_types: Sequence[ModelTypeFilter[M]] = ()
    """Models must be one of these types."""

    def apply(self, query: Query) -> Query:
        query = super().apply(query)

        for model_type in self.model_types:
            query = model_type.apply(query)

        if self.is_root:
            query = NodeFilter(
                node_type=NodeTypeFilter(type_not_in=BaseArtifact.__subclasses__())
            ).apply(query)

        return query


class ModelTypeFilter(Generic[M], Filter):
    """Filter models by their type and version"""

    model_type: type[M]
    """Models must be this type."""

    model_version: ValueFilter | None = None
    """Models must be this version."""

    def apply(self, query: Query) -> Query:
        query = query.where(ModelArtifact.model_artifact_type == self.model_type.model_name)

        if self.model_version:
            query = self.model_version.using(ModelArtifact.model_artifact_version).apply(query)

        return query
