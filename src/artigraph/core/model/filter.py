from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Generic,
    Sequence,
    TypeVar,
)

from sqlalchemy import (
    ColumnElement,
    Delete,
    Exists,
    Select,
    Update,
)
from typing_extensions import ParamSpec

from artigraph.core.api.filter import (
    ArtifactFilter,
    Filter,
    MultiFilter,
    NodeTypeFilter,
    ValueFilter,
    to_sequence_or_none,
    to_value_filter,
)
from artigraph.core.orm.artifact import OrmArtifact, OrmModelArtifact
from artigraph.core.orm.node import OrmNode
from artigraph.core.utils.misc import get_subclasses

if TYPE_CHECKING:
    from artigraph.core.model.base import GraphModel

P = ParamSpec("P")
T = TypeVar("T")
N = TypeVar("N", bound=OrmNode)
A = TypeVar("A", bound=OrmArtifact)
Q = TypeVar("Q", bound="Select | Update | Delete | Exists")
M = TypeVar("M", bound="GraphModel")

Expression = ColumnElement[bool]
"""An alias for a sqlalchemy `OperatorExpression`"""


class ModelFilter(ArtifactFilter[OrmModelArtifact], Generic[M]):
    """A filter for models."""

    node_type: NodeTypeFilter[OrmModelArtifact] = NodeTypeFilter(type=[OrmModelArtifact])
    """Models must be one of these types."""
    model_type: Sequence[ModelTypeFilter[M]] | ModelTypeFilter[M] | type[M] | None = None
    """Models must be one of these types."""

    def compose(self, expr: Expression) -> Expression:
        expr = super().compose(expr)

        model_type = to_sequence_or_none(self.model_type)

        if model_type:
            expr &= MultiFilter(
                op="or",
                filters=[_to_model_type_filter(mt) for mt in model_type],
            ).create()

        return expr


class ModelTypeFilter(Generic[M], Filter):
    """Filter models by their type and version"""

    type: type[M]
    """Models must be this type."""
    version: ValueFilter | int | None = None
    """Models must be this version."""
    subclasses: bool = True
    """If True, include subclasses of the given model type."""

    def compose(self, expr: Expression) -> Expression:
        version = to_value_filter(self.version)

        if self.subclasses:
            expr &= OrmModelArtifact.model_artifact_type_name.in_(
                [m.graph_model_name for m in get_subclasses(self.type)]
            )
        else:
            expr &= OrmModelArtifact.model_artifact_type_name == self.type.graph_model_name

        if version:
            expr &= version.against(OrmModelArtifact.model_artifact_version).create()

        return expr


def _to_model_type_filter(model_type: type[GraphModel] | ModelTypeFilter) -> ModelTypeFilter:
    return (
        model_type
        if isinstance(model_type, ModelTypeFilter)
        else ModelTypeFilter(type=model_type, version=model_type.graph_model_version)
    )
