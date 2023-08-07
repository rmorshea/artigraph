from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Annotated, Any, ClassVar, get_args, get_origin

from typing_extensions import Self, dataclass_transform

from artigraph.model.base import BaseModel, FieldConfig, ModelData
from artigraph.serializer.core import Serializer
from artigraph.storage.core import Storage


@dataclass_transform()
class _DataModelMeta(type):
    def __new__(
        cls,
        name: str,
        bases: tuple[type[Any, ...]],
        namespace: dict[str, Any],
        *,
        version: int,
        **kwargs: Any,
    ):
        namespace["model_version"] = version
        model_field_configs = namespace["_model_field_configs"] = {}

        self = super().__new__(cls, name, bases, namespace, **kwargs)
        self = dataclass(frozen=True, **kwargs)(self)

        for f in filter(lambda f: f.init, fields(self)):
            f_config = FieldConfig()
            if get_origin(f.type) is Annotated:
                for f_type_arg in get_args(f.type):
                    if isinstance(f_type_arg, Serializer):
                        f_config["serializer"] = f_type_arg
                    elif isinstance(f_type_arg, Storage):
                        f_config["storage"] = f_type_arg
            model_field_configs[f.name] = f_config

        return self


class DataModel(BaseModel, metaclass=_DataModelMeta, version=1):
    """Describes a structure of data that can be saved as artifacts."""

    _model_field_configs: ClassVar[dict[str, FieldConfig]] = {}

    @classmethod
    def model_init(cls, version: int, data: dict[str, Any]) -> Self:  # noqa: ARG003
        return cls(**data)

    def model_data(self) -> ModelData:
        return {
            name: (getattr(self, name), config)
            for name, config in self._model_field_configs.items()
        }
