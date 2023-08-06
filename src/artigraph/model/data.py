from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, get_args, get_origin

from typing_extensions import Self, dataclass_transform

from artigraph.model.base import BaseModel, FieldConfig
from artigraph.serializer.core import Serializer
from artigraph.serializer.json import json_serializer
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
        model_field_configs = namespace["model_field_configs"] = {}
        self = super().__new__(cls, name, bases, namespace, **kwargs)
        self = dataclass(frozen=True, **kwargs)(self)
        for f in filter(lambda f: f.init, fields(self)):
            f_config = FieldConfig(serializer=json_serializer)
            if get_origin(f.type) is Annotated:
                for f_type_arg in get_args(f.type):
                    if isinstance(f_type_arg, Serializer):
                        f_config["serializer"] = f_type_arg
                    elif isinstance(f_type_arg, Storage):
                        f_config["storage"] = f_type_arg
            model_field_configs[f.name] = f_config
        return self


# convince type checkers that this is a dataclass
_cast_dataclass = dataclass if TYPE_CHECKING else lambda c: c


@_cast_dataclass
class DataModel(BaseModel, metaclass=_DataModelMeta, version=1):
    """Describes a structure of data that can be saved as artifacts."""

    model_field_configs: ClassVar[dict[str, FieldConfig]] = {}
    """The configuration for the data model fields."""

    @classmethod
    def model_migrate(
        cls,
        version: int,  # noqa: ARG003
        kwargs: dict[str, Any],
    ) -> Self:
        """Migrate the data model to a new version."""
        return cls(**kwargs)

    def model_data(self) -> dict[str, tuple[Any, FieldConfig]]:
        """The data for the data model."""
        return {
            f.name: (getattr(self, f.name), self.model_field_configs[f.name])
            for f in fields(self)
            if f.init
        }
