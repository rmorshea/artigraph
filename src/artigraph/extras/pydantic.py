from typing import Any
from uuid import UUID, uuid1

from pydantic import BaseModel as _BaseModel
from pydantic import Field
from typing_extensions import Self

from artigraph.core.model.base import GraphModel, ModelData, ModelInfo
from artigraph.core.model.dataclasses import get_annotated_model_data


class PydanticModel(GraphModel, _BaseModel, version=1):
    """A base for all artifacts modeled with Pydantic."""

    graph_id: UUID = Field(default_factory=uuid1, exclude=True)
    """The unique ID of this model."""

    def graph_model_data(self) -> ModelData:
        return get_annotated_model_data(
            self,
            [name for name, field in self.model_fields.items() if not field.exclude],
        )

    @classmethod
    def graph_model_init(cls, info: ModelInfo, kwargs: dict[str, Any]) -> Self:
        return cls(graph_id=info.graph_id, **kwargs)
