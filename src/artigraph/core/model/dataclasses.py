from __future__ import annotations

from dataclasses import dataclass as _dataclass
from dataclasses import field, fields
from typing import (
    Any,
    Callable,
    Sequence,
    TypeVar,
    cast,
)
from uuid import UUID, uuid1

from typing_extensions import Self, dataclass_transform

from artigraph.core.model.base import (
    GraphModel,
    ModelData,
    ModelInfo,
    allow_model_type_overwrites,
)
from artigraph.core.utils.type_hints import get_save_specs_from_type_hints

F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T", bound=type[GraphModel])


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
                graph_id: UUID = field(default_factory=uuid1, init=False, compare=False)
                graph_model_name = getattr(cls, "graph_model_name", cls.__name__)

                @classmethod
                def graph_model_init(cls, info: ModelInfo, data: dict[str, Any]) -> Self:
                    self = cls(**data)
                    object.__setattr__(self, "graph_id", info.graph_id)
                    return self

                def graph_model_data(self) -> ModelData:
                    return get_annotated_model_data(
                        self,
                        [
                            f.name
                            for f in fields(self)
                            if f.init
                            # exclude this since it's on the DB record anyway
                            and f.name != "graph_id"
                        ],
                    )

        _DataclassModel.__name__ = cls.__name__
        _DataclassModel.__qualname__ = cls.__qualname__

        return cast(type[T], _DataclassModel)

    return decorator if cls is None else decorator(cls)


def get_annotated_model_data(obj: Any, field_names: Sequence[str]) -> ModelData:
    """Get the model data for a dataclass-like instance."""
    save_specs = get_save_specs_from_type_hints(type(obj), use_cache=True)
    return {name: (getattr(obj, name), save_specs[name]) for name in field_names}
