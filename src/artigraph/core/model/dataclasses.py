from __future__ import annotations

from dataclasses import dataclass as _dataclass
from dataclasses import field, fields
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Sequence,
    TypeVar,
    cast,
    get_args,
    get_origin,
    get_type_hints,
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
from artigraph.core.serializer.base import Serializer
from artigraph.core.storage.base import Storage

T = TypeVar("T", bound=type[GraphModel])


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
                graph_node_id: UUID = field(default_factory=uuid1, kw_only=True)
                graph_model_name = getattr(cls, "graph_model_name", cls.__name__)

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

        _DataclassModel.__name__ = cls.__name__
        _DataclassModel.__qualname__ = cls.__qualname__

        return cast(type[T], _DataclassModel)

    return decorator if cls is None else decorator(cls)


if TYPE_CHECKING:
    # we should have the exact same interface
    dataclass = _dataclass  # type: ignore  # noqa: F811


def get_annotated_model_data(obj: Any, field_names: Sequence[str]) -> ModelData:
    """Get the model data for a dataclass-like instance."""
    model_field_configs = {}
    cls_hints = _get_type_hints(type(obj))
    for f_name in field_names:  # type: ignore
        f_config = FieldConfig()
        hint = cls_hints.get(f_name)
        serializers: list[Serializer] = []
        for annotated_hint in _find_all_annotated_metadata(hint):
            for f_type_arg in get_args(annotated_hint):
                if isinstance(f_type_arg, Serializer):
                    serializers.append(f_type_arg)
                elif isinstance(f_type_arg, Storage):
                    if "storage" in f_config:
                        msg = (
                            f"Multiple storage types specified for {f_name!r} "
                            f"- {f_type_arg} and {f_config['storage']}"
                        )
                        raise ValueError(msg)
                    f_config["storage"] = f_type_arg
            if serializers:
                f_config["serializers"] = serializers
        model_field_configs[f_name] = f_config
    return {name: (getattr(obj, name), config) for name, config in model_field_configs.items()}


def _find_all_annotated_metadata(hint: Any) -> Sequence[Annotated]:
    """Find all Annotated metadata in a type hint."""
    if get_origin(hint) is Annotated:
        return [hint, *[a for h in get_args(hint) for a in _find_all_annotated_metadata(h)]]
    return [a for h in get_args(hint) for a in _find_all_annotated_metadata(h)]


@lru_cache(maxsize=None)
def _get_type_hints(cls: type[Any]) -> dict[str, Any]:
    # This can be pretty slow and there should be a finite number of classes so we cache it.
    return get_type_hints(cls, include_extras=True)
