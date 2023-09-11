from __future__ import annotations

from dataclasses import field, fields
from functools import lru_cache
from typing import Annotated, Any, TypedDict, get_args, get_origin, get_type_hints
from uuid import UUID, uuid1

from typing_extensions import Self

from artigraph.core.model.base import FieldConfig, GraphModel, LabeledArtifactsByParentId, ModelData
from artigraph.core.orm.artifact import OrmModelArtifact
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

    graph_node_id: UUID = field(
        default_factory=uuid1,
        metadata=DataclassModelFieldMetadata(graph_is_not_model_field=True),
    )
    """The unique ID of this model."""

    @classmethod
    def graph_model_init(cls, version: int, data: dict[str, Any]) -> Self:  # noqa: ARG003
        return cls(**data)

    def graph_model_data(self) -> ModelData:
        return {
            name: (getattr(self, name), config)
            for name, config in self.graph_model_field_configs().items()
        }

    @classmethod
    def graph_model_field_configs(cls) -> dict[str, FieldConfig]:
        model_field_configs = cls._model_field_configs = {}
        cls_hints = _get_type_hints(cls)
        for f in filter(lambda f: f.init, fields(cls)):  # type: ignore
            metadata: DataclassModelFieldMetadata = f.metadata
            if metadata.get("graph_is_not_model_field"):
                continue
            f_config = FieldConfig()
            hint = cls_hints.get(f.name)
            if get_origin(hint) is Annotated:
                for f_type_arg in get_args(hint):
                    if isinstance(f_type_arg, Serializer):
                        f_config["serializer"] = f_type_arg
                    elif isinstance(f_type_arg, Storage):
                        f_config["storage"] = f_type_arg
            model_field_configs[f.name] = f_config
        return model_field_configs

    @classmethod
    async def _graph_load_kwargs_from_labeled_artifacts_by_parent_id(
        cls,
        model_metadata_artifact: OrmModelArtifact,
        labeled_artifacts_by_parent_id: LabeledArtifactsByParentId,
    ) -> dict[str, Any]:
        return {
            "graph_node_id": model_metadata_artifact.node_id,
            **(
                await super()._graph_load_kwargs_from_labeled_artifacts_by_parent_id(
                    model_metadata_artifact,
                    labeled_artifacts_by_parent_id,
                )
            ),
        }


@lru_cache(maxsize=None)
def _get_type_hints(cls: type[DataclassModel]) -> dict[str, Any]:
    # This can be pretty slow and there should be a finite number of classes so we cache it.
    return get_type_hints(cls, include_extras=True)
