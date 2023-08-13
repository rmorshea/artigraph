from __future__ import annotations

from dataclasses import field
from typing import TYPE_CHECKING, Generic, Sequence, TypeVar

from sqlalchemy import or_, select

from artigraph.api.filter import (
    ArtifactFilter,
    Filter,
    NodeTypeFilter,
    Query,
    ValueFilter,
    to_sequence_or_none,
    to_value_filter,
)
from artigraph.orm.artifact import ModelArtifact
from artigraph.utils import get_subclasses

if TYPE_CHECKING:
    from artigraph.model.base import BaseModel


M = TypeVar("M", bound="BaseModel")


class ModelFilter(ArtifactFilter[ModelArtifact], Generic[M]):
    """A filter for models."""

    node_type: NodeTypeFilter[ModelArtifact] = field(
        # delay this in case tables are defined late
        default_factory=lambda: NodeTypeFilter(type=[ModelArtifact])
    )
    """Models must be one of these types."""
    model_type: Sequence[ModelTypeFilter[M]] | ModelTypeFilter[M] | type[M] | None = None
    """Models must be one of these types."""

    def apply(self, query: Query) -> Query:
        query = super().apply(query)

        model_type = to_sequence_or_none(self.model_type)

        if model_type is not None:
            sub_queries = [
                ModelArtifact.node_id.in_(
                    _to_model_type_filter(mt).apply(select(ModelArtifact.node_id))
                )
                for mt in model_type
            ]
            query = query.where(or_(*sub_queries))

        return query


class ModelTypeFilter(Generic[M], Filter):
    """Filter models by their type and version"""

    type: type[M]  # noqa: A003
    """Models must be this type."""
    version: ValueFilter | int | None = None
    """Models must be this version."""
    subclasses: bool = True
    """If True, include subclasses of the given model type."""

    def apply(self, query: Query) -> Query:
        if self.subclasses:
            query = query.where(
                ModelArtifact.model_artifact_type.in_(
                    [m.model_name for m in get_subclasses(self.type)]
                )
            )
        else:
            query = query.where(ModelArtifact.model_artifact_type == self.type.model_name)

        if self.version is not None:
            query = (
                to_value_filter(self.version)
                .using(ModelArtifact.model_artifact_version)
                .apply(query)
            )

        return query


def _to_model_type_filter(model_type: type[BaseModel] | ModelTypeFilter) -> ModelTypeFilter:
    return (
        model_type
        if isinstance(model_type, ModelTypeFilter)
        else ModelTypeFilter(type=model_type, version=model_type.model_version)
    )
