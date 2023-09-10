from __future__ import annotations

from typing import Any, TypeVar

from typing_extensions import Self

from artigraph.core.model.base import MODELED_TYPES, FieldConfig, GraphModel, ModelData

T = TypeVar("T")


class DictModel(GraphModel, dict[str, T], version=1):
    """A dictionary of artifacts"""

    @classmethod
    def graph_model_init(cls, _: int, data: dict[str, Any]) -> Self:
        return cls(data)

    def graph_model_data(self) -> ModelData:
        return {k: (v, FieldConfig()) for k, v in self.items()}


class FrozenSetModel(GraphModel, frozenset[T], version=1):
    """A dictionary of artifacts"""

    @classmethod
    def graph_model_init(cls, _: int, data: dict[str, Any]) -> Self:
        return cls(data.values())

    def graph_model_data(self) -> ModelData:
        return {str(i): (v, FieldConfig()) for i, v in enumerate(self)}


class ListModel(list[T], GraphModel, version=1):
    """A list of artifacts"""

    @classmethod
    def graph_model_init(cls, _: int, data: dict[str, Any]) -> Self:
        list_from_data = [None] * len(data)
        for k, v in data.items():
            list_from_data[int(k)] = v
        return cls(list_from_data)

    def graph_model_data(self) -> ModelData:
        return {str(i): (v, FieldConfig()) for i, v in enumerate(self)}


class SetModel(GraphModel, set[T], version=1):
    """A dictionary of artifacts"""

    @classmethod
    def graph_model_init(cls, _: int, data: dict[str, Any]) -> Self:
        return cls(data.values())

    def graph_model_data(self) -> ModelData:
        return {str(i): (v, FieldConfig()) for i, v in enumerate(self)}


class TupleModel(GraphModel, tuple[T], version=1):
    """A tuple of artifacts"""

    @classmethod
    def graph_model_init(cls, _: int, data: dict[str, Any]) -> Self:
        data_from_kwargs = [None] * len(data)
        for k, v in data.items():
            data_from_kwargs[int(k)] = v
        return cls(data_from_kwargs)

    def graph_model_data(self) -> ModelData:
        return {str(i): (v, FieldConfig()) for i, v in enumerate(self)}


MODELED_TYPES[list] = ListModel
MODELED_TYPES[tuple] = TupleModel
MODELED_TYPES[dict] = DictModel
MODELED_TYPES[set] = SetModel
MODELED_TYPES[frozenset] = FrozenSetModel
