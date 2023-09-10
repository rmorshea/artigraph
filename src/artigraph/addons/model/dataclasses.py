from __future__ import annotations

from dataclasses import fields
from functools import lru_cache
from typing import Annotated, Any, get_args, get_origin, get_type_hints

from typing_extensions import Self

from artigraph.api.model import FieldConfig, GraphModel, ModelData
from artigraph.core.serializer.base import Serializer
from artigraph.core.storage.base import Storage
from artigraph.utils.misc import Dataclass


class DataModel(GraphModel, Dataclass, version=1, frozen=True):
    """Describes a structure of data that can be saved as artifacts."""

    @classmethod
    def model_field_configs(cls) -> dict[str, FieldConfig]:
        model_field_configs = cls._model_field_configs = {}
        cls_hints = _get_type_hints(cls)
        for f in filter(lambda f: f.init, fields(cls)):  # type: ignore
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
    def model_init(cls, version: int, data: dict[str, Any]) -> Self:  # noqa: ARG003
        return cls(**data)

    def model_data(self) -> ModelData:
        return {
            name: (getattr(self, name), config)
            for name, config in self.model_field_configs().items()
        }


@lru_cache(maxsize=None)
def _get_type_hints(cls: type[DataModel]) -> dict[str, Any]:
    # This can be pretty slow and there should be a finite number of classes so we cache it.
    return get_type_hints(cls, include_extras=True)
