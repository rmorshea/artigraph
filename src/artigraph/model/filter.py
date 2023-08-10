from __future__ import annotations

from typing import TYPE_CHECKING, Generic, Sequence, TypeVar

from sqlalchemy.sql.selectable import Select

from artigraph.api.filter import (
    ArtifactFilter,
    Filter,
    IntegerFilter,
    NodeFilter,
    NodeTypeFilter,
)
from artigraph.orm.artifact import BaseArtifact, ModelArtifact

if TYPE_CHECKING:
    from artigraph.model.base import BaseModel


M = TypeVar("M", bound="BaseModel")


class ModelFilter(ArtifactFilter, Generic[M]):
    """A filter for models."""

    is_root: bool = False
    """Model node must be the root node. That is, it's parent is not an artifact."""

    model_types: Sequence[ModelTypeFilter[M]] = ()
    """Models must be one of these types."""

    def apply(self, query: Select) -> Select:
        self.node_type = NodeTypeFilter(any_of=ModelArtifact.__subclasses__())

        for model_type in self.model_types:
            query = model_type.apply(query)

        if self.is_root:
            query = NodeFilter(
                node_type=NodeTypeFilter(none_of=BaseArtifact.__subclasses__())
            ).apply(query)

        return query


class ModelTypeFilter(Generic[M], Filter):
    """Filter models by their type and version"""

    model_type: type[M]
    """Models must be this type."""

    model_version: IntegerFilter | None = None
    """Models must be this version."""

    def apply(self, query: Select) -> Select:
        query = query.where(ModelArtifact.model_artifact_type == self.model_type.__name__)

        if self.model_version:
            query = self.model_version.using(ModelArtifact.model_artifact_version).apply(query)

        return query
