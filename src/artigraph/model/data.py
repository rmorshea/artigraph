from __future__ import annotations

from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, get_args, get_origin

from typing_extensions import Self, dataclass_transform

from artigraph.model.base import BaseModel, FieldConfig
from artigraph.serializer.core import Serializer
from artigraph.storage.core import Storage


@dataclass_transform
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
        for f in fields(self):
            if get_origin(f.type) is Annotated:
                f_config = FieldConfig()
                for a in get_args(f.type):
                    if isinstance(a, Serializer):
                        f_config["serializer"] = a
                    elif isinstance(a, Storage):
                        f_config["storage"] = a
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
            f.name: (getattr(self, f.name), self.model_field_configs.get(f.name, FieldConfig()))
            for f in fields(self)
            if f.init
        }
