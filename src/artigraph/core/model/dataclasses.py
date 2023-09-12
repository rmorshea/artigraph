from __future__ import annotations

from dataclasses import dataclass as _dataclass
from dataclasses import field, fields
from functools import lru_cache
from typing import (
    Annotated,
    Any,
    Callable,
    Literal,
    Sequence,
    TypeVar,
    dataclass_transform,
    get_args,
    get_origin,
    get_type_hints,
    overload,
)
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

T = TypeVar("T", bound=type[GraphModel])


@overload
@dataclass_transform(field_specifiers=(field,))
def dataclass(
    *,
    init: Literal[False] = False,
    repr: bool = True,  # noqa: A002
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    kw_only: bool = False,
    slots: bool = False,
) -> Callable[[T], T]:
    ...


@overload
@dataclass_transform(field_specifiers=(field,))
def dataclass(
    cls: T,
    /,
    *,
    init: Literal[False] = False,
    repr: bool = True,  # noqa: A002
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    kw_only: bool = False,
    slots: bool = False,
) -> type[T]:
    ...


@dataclass_transform(field_specifiers=(field,))
def dataclass(
    cls: type[Any] | None = None,
    /,
    *,
    init: Literal[False] = False,
    repr: bool = True,  # noqa: A002
    eq: bool = True,
    order: bool = False,
    unsafe_hash: bool = False,
    frozen: bool = False,
    kw_only: bool = False,
    slots: bool = False,
) -> type[GraphModel]:
    """A decorator that makes a class into a dataclass GraphModel.

    See: [dataclass](https://docs.python.org/3/library/dataclasses.html#dataclasses.dataclass)
    """

    def decorator(cls: type[T]) -> type[GraphModel]:
        if not issubclass(cls, GraphModel):
            msg = f"{cls} does not inherit from GraphModel"
            raise TypeError(msg)

        cls = _dataclass(
            cls,
            init=init,
            repr=repr,
            eq=eq,
            order=order,
            unsafe_hash=unsafe_hash,
            frozen=frozen,
            kw_only=kw_only,
            slots=slots,
        )

        @_dataclass(frozen=frozen)
        class _DataclassModel(cls, version=cls.graph_model_version):
            graph_node_id: UUID = field(default_factory=uuid1, kw_only=True)

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

        return _DataclassModel

    return decorator if cls is None else decorator(cls)


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
def _get_type_hints(cls: type[Any]) -> dict[str, Any]:
    # This can be pretty slow and there should be a finite number of classes so we cache it.
    return get_type_hints(cls, include_extras=True)
