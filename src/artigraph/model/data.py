from __future__ import annotations

from dataclasses import Field, dataclass, field, fields
from typing import TYPE_CHECKING, Any, ClassVar

from typing_extensions import Self, dataclass_transform

from artigraph.model.base import BaseModel, FieldConfig, ModelConfig
from artigraph.serializer import Serializer
from artigraph.storage import Storage


def model_field(
    *,
    serializer: Serializer[Any] | None = None,
    storage: Storage | None = None,
    **kwargs: Any,
) -> Field:
    """A dataclass field with a serializer and storage."""
    metadata = kwargs.get("metadata", {})
    metadata = {
        "artifact_model_field_config": FieldConfig(
            serializer=serializer,
            storage=storage,
        ),
        **metadata,
    }
    return field(**kwargs)


@dataclass_transform(field_specifiers=(model_field,))
class _DataModelMeta(type):
    def __new__(
        cls,
        name: str,
        bases: tuple[type[Any, ...]],
        namespace: dict[str, Any],
        *,
        version: int = 1,
        config: ModelConfig | None = None,
        **kwargs: Any,
    ):
        namespace["model_version"] = version
        namespace["model_config"] = config or ModelConfig()
        self = super().__new__(cls, name, bases, namespace, **kwargs)
        self = dataclass(frozen=True, **kwargs)(self)
        return self


# convince type checkers that this is a dataclass
_cast_dataclass = dataclass if TYPE_CHECKING else lambda c: c


@_cast_dataclass
class DataModel(BaseModel, metaclass=_DataModelMeta):
    """A collection of artifacts that are saved together."""

    model_version: ClassVar[int] = 1
    """The version of the artifact model."""

    model_config: ClassVar[ModelConfig] = ModelConfig()
    """The configuration for the artifact model."""

    @classmethod
    def model_migrate(
        cls,
        version: int,  # noqa: ARG003
        kwargs: dict[str, Any],
    ) -> Self:
        """Migrate the artifact model to a new version."""
        return cls(**kwargs)

    def model_data(self) -> dict[str, tuple[Any, FieldConfig]]:
        """The data for the artifact model."""
        return {
            f.name: (
                getattr(self, f.name),
                f.metadata.get("artifact_model_field_config", FieldConfig()),
            )
            for f in fields(self)
            if f.init
        }
