from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar
from uuid import UUID, uuid1

from typing_extensions import Self

from artigraph.core.api.artifact import SaveSpec
from artigraph.core.model.base import MODELED_TYPES, GraphModel, ModelData, ModelInfo

T = TypeVar("T")


class DictModel(GraphModel, dict[str, T], version=1):
    """A dictionary of artifacts"""

    if TYPE_CHECKING:

        def __new__(cls, *_a: Any, __graph_id: UUID | None = None, **_kw: Any) -> Self:
            ...

    def __init__(self, *args: Any, __graph_id: UUID | None = None, **kwargs: Any) -> None:
        self.graph_id = __graph_id or uuid1()
        super().__init__(*args, **kwargs)

    @classmethod
    def graph_model_init(cls, info: ModelInfo, data: dict[str, Any]) -> Self:
        return cls(data, _DictModel__graph_id=info.graph_id)

    def graph_model_data(self) -> ModelData:
        return {k: (v, SaveSpec()) for k, v in self.items()}


class FrozenSetModel(GraphModel, frozenset[T], version=1):
    """A dictionary of artifacts"""

    if TYPE_CHECKING:

        def __new__(cls, *_a: Any, __graph_id: UUID | None = None, **_kw: Any) -> Self:
            ...

    def __init__(self, *_args: Any, __graph_id: UUID | None = None, **_kwargs: Any) -> None:
        self.graph_id = __graph_id or uuid1()

    @classmethod
    def graph_model_init(cls, info: ModelInfo, data: dict[str, Any]) -> Self:
        return cls(data.values(), _FrozenSetModel__graph_id=info.graph_id)

    def graph_model_data(self) -> ModelData:
        return {str(i): (v, SaveSpec()) for i, v in enumerate(self)}


class ListModel(list[T], GraphModel, version=1):
    """A list of artifacts"""

    if TYPE_CHECKING:

        def __new__(cls, *_a: Any, __graph_id: UUID | None = None, **_kw: Any) -> Self:
            ...

    def __init__(self, *args: Any, __graph_id: UUID | None = None, **kwargs: Any) -> None:
        self.graph_id = __graph_id or uuid1()
        super().__init__(*args, **kwargs)

    @classmethod
    def graph_model_init(cls, info: ModelInfo, data: dict[str, Any]) -> Self:
        list_from_data = [None] * len(data)
        for k, v in data.items():
            list_from_data[int(k)] = v
        return cls(list_from_data, _ListModel__graph_id=info.graph_id)

    def graph_model_data(self) -> ModelData:
        return {str(i): (v, SaveSpec()) for i, v in enumerate(self)}


class SetModel(GraphModel, set[T], version=1):
    """A dictionary of artifacts"""

    if TYPE_CHECKING:

        def __new__(cls, *_a: Any, __graph_id: UUID | None = None, **_kw: Any) -> Self:
            ...

    def __init__(self, *args: Any, __graph_id: UUID | None = None, **kwargs: Any) -> None:
        self.graph_id = __graph_id or uuid1()
        super().__init__(*args, **kwargs)

    @classmethod
    def graph_model_init(cls, info: ModelInfo, data: dict[str, Any]) -> Self:
        return cls(data.values(), _SetModel__graph_id=info.graph_id)

    def graph_model_data(self) -> ModelData:
        return {str(i): (v, SaveSpec()) for i, v in enumerate(self)}


class TupleModel(GraphModel, tuple[T], version=1):
    """A tuple of artifacts"""

    if TYPE_CHECKING:

        def __new__(cls, *_a: Any, __graph_id: UUID | None = None, **_kw: Any) -> Self:
            ...

    def __init__(self, *_args: Any, __graph_id: UUID | None = None, **_kwargs: Any) -> None:
        self.graph_id = __graph_id or uuid1()

    @classmethod
    def graph_model_init(cls, info: ModelInfo, data: dict[str, Any]) -> Self:
        data_from_kwargs = [None] * len(data)
        for k, v in data.items():
            data_from_kwargs[int(k)] = v
        return cls(data_from_kwargs, _TupleModel__graph_id=info.graph_id)

    def graph_model_data(self) -> ModelData:
        return {str(i): (v, SaveSpec()) for i, v in enumerate(self)}


MODELED_TYPES[list] = ListModel
MODELED_TYPES[tuple] = TupleModel
MODELED_TYPES[dict] = DictModel
MODELED_TYPES[set] = SetModel
MODELED_TYPES[frozenset] = FrozenSetModel
