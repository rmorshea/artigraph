from __future__ import annotations

from dataclasses import field, fields
from functools import lru_cache
from typing import Annotated, Any, Sequence, TypedDict, get_args, get_origin, get_type_hints
from uuid import UUID, uuid1

from typing_extensions import Self

from artigraph.core.model.base import (
    FieldConfig,
    GraphModel,
    ModelData,
    ModelInfo,
)
from artigraph.core.serializer.base import Serializer
from artigraph.core.storage.base import Storage
from artigraph.core.utils.misc import Dataclass


class DataclassModelFieldMetadata(TypedDict, total=False):
    graph_is_not_model_field: bool


class DataclassModel(
    GraphModel,
    Dataclass,
    version=1,
    frozen=True,
):
    """Describes a structure of data that can be saved as artifacts."""

    graph_node_id: UUID = field(default_factory=uuid1)
    """The unique ID of this model."""

    @classmethod
    def graph_model_init(cls, info: ModelInfo, data: dict[str, Any]) -> Self:
        return cls(graph_node_id=info.node_id, **data)

    def graph_model_data(self) -> ModelData:
        return get_annotated_model_data(
            self,
            [
                f.name
                for f in fields(self)
                if f.init
                # exclude this since it's on the DB record anyway
                and f.name != "graph_node_id"
            ],
        )


def get_annotated_model_data(obj: Any, field_names: Sequence[str]) -> ModelData:
    """Get the model data for a dataclass-like instance."""
    model_field_configs = {}
    cls_hints = _get_type_hints(type(obj))
    for f_name in field_names:  # type: ignore
        f_config = FieldConfig()
        hint = cls_hints.get(f_name)
        if get_origin(hint) is Annotated:
            for f_type_arg in get_args(hint):
                if isinstance(f_type_arg, Serializer):
                    f_config["serializer"] = f_type_arg
                elif isinstance(f_type_arg, Storage):
                    f_config["storage"] = f_type_arg
        model_field_configs[f_name] = f_config
    return {name: (getattr(obj, name), config) for name, config in model_field_configs.items()}


@lru_cache(maxsize=None)
def _get_type_hints(cls: type[DataclassModel]) -> dict[str, Any]:
    # This can be pretty slow and there should be a finite number of classes so we cache it.
    return get_type_hints(cls, include_extras=True)
