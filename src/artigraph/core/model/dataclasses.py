from __future__ import annotations

from dataclasses import dataclass as _dataclass
from dataclasses import field, fields
from typing import (
    Any,
    Callable,
    Sequence,
    TypeVar,
    cast,
    dataclass_transform,
)
from uuid import UUID, uuid1

from typing_extensions import Self

from artigraph.core.model.base import (
    FieldConfig,
    GraphModel,
    ModelData,
    ModelInfo,
    allow_model_type_overwrites,
)
from artigraph.core.utils.type_hints import get_annotation_info

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T", bound=type[GraphModel])


def _copy_signature(_: F) -> Callable[[Any], F]:
    def wrapper(copy_to: Any) -> F:
        return cast(F, copy_to)

    return wrapper


@dataclass_transform(field_specifiers=(field,))
def dataclass(cls: type[T] | None = None, **kwargs: Any) -> type[T] | Callable[[type[T]], type[T]]:
    """A decorator that makes a class into a dataclass GraphModel.

    See: [dataclass](https://docs.python.org/3/library/dataclasses.html#dataclasses.dataclass)
    """

    def decorator(cls: type[T]) -> type[T]:
        if not issubclass(cls, GraphModel):
            msg = f"{cls} does not inherit from GraphModel"
            raise TypeError(msg)

        cls = _dataclass(cls, **kwargs)

        with allow_model_type_overwrites():

            @_dataclass(**kwargs)
            class _DataclassModel(cls, version=cls.graph_model_version):
                graph_node_id: UUID = field(default_factory=uuid1, init=False, compare=False)
                graph_model_name = getattr(cls, "graph_model_name", cls.__name__)

                @classmethod
                def graph_model_init(cls, info: ModelInfo, data: dict[str, Any]) -> Self:
                    self = cls(**data)
                    object.__setattr__(self, "graph_node_id", info.node_id)
                    return self

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

        _DataclassModel.__name__ = cls.__name__
        _DataclassModel.__qualname__ = cls.__qualname__

        return cast(type[T], _DataclassModel)

    return decorator if cls is None else decorator(cls)


def get_annotated_model_data(obj: Any, field_names: Sequence[str]) -> ModelData:
    """Get the model data for a dataclass-like instance."""
    model_field_configs = {}

    cls_hints = get_annotation_info(type(obj), use_cache=True)
    for f_name in field_names:  # type: ignore
        f_config = FieldConfig()
        if f_name in cls_hints:
            f_config["serializers"] = cls_hints[f_name].serializers
            if cls_hints[f_name].storage is not None:
                f_config["storage"] = cls_hints[f_name].storage
        model_field_configs[f_name] = f_config
    return {name: (getattr(obj, name), config) for name, config in model_field_configs.items()}
