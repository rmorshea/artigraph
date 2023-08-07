from __future__ import annotations

from typing import Any, Collection, Sequence, TypeVar

from typing_extensions import Self

from artigraph.model.base import MODELED_TYPES, BaseModel, FieldConfig, ModelData

T = TypeVar("T")


class DictModel(BaseModel, dict[str, T]):
    """A dictionary of artifacts"""

    @classmethod
    def model_migrate(cls, _: int, kwargs: dict[str, Any]) -> Self:
        """Migrate the artifact model to a new version."""
        return cls(kwargs)

    def model_data(self) -> ModelData:
        """The data for the artifact model."""
        return {k: (v, FieldConfig()) for k, v in self.items()}


class FrozenSetModel(BaseModel, frozenset[T]):
    """A dictionary of artifacts"""

    @classmethod
    def model_migrate(cls, _: int, kwargs: dict[str, Any]) -> Self:
        """Migrate the artifact model to a new version."""
        return cls(kwargs)

    def model_data(self) -> ModelData:
        """The data for the artifact model."""
        return {hash(v): (v, FieldConfig()) for v in self}

    def __init__(self, *args: Collection[T], **kwargs: T) -> None:
        args += (kwargs.values(),)
        super().__init__(*args)


class ListModel(BaseModel, list[T]):
    """A list of artifacts"""

    @classmethod
    def model_migrate(cls, _: int, kwargs: dict[str, Any]) -> Self:
        """Migrate the artifact model to a new version."""
        return cls(kwargs)

    def model_data(self) -> ModelData:
        """The data for the artifact model."""
        return {str(i): (v, FieldConfig()) for i, v in enumerate(self._data)}

    def __init__(self, *args: Sequence[T], **kwargs: T) -> None:
        list_from_kwargs = [None] * len(kwargs)
        for k, v in kwargs.items():
            list_from_kwargs[int(k)] = v
        args += (list_from_kwargs,)
        super().__init__(*args)


class SetModel(BaseModel, set[T]):
    """A dictionary of artifacts"""

    @classmethod
    def model_migrate(cls, _: int, kwargs: dict[str, Any]) -> Self:
        """Migrate the artifact model to a new version."""
        return cls(kwargs)

    def model_data(self) -> ModelData:
        """The data for the artifact model."""
        return {str(i): (v, FieldConfig()) for i, v in enumerate(self)}

    def __init__(self, *args: Collection[T], **kwargs: T) -> None:
        args += (kwargs.values(),)
        super().__init__(*args)


class TupleModel(BaseModel, tuple[T]):
    """A tuple of artifacts"""

    @classmethod
    def model_migrate(cls, _: int, kwargs: dict[str, Any]) -> Self:
        """Migrate the artifact model to a new version."""
        return cls(kwargs)

    def model_data(self) -> ModelData:
        """The data for the artifact model."""
        return {str(i): (v, FieldConfig()) for i, v in enumerate(self._data)}

    def __init__(self, *args: Sequence[T], **kwargs: T) -> None:
        list_from_kwargs = [None] * len(kwargs)
        for k, v in kwargs.items():
            list_from_kwargs[int(k)] = v
        args += (list_from_kwargs,)
        super().__init__(*args)


MODELED_TYPES[list] = ListModel
MODELED_TYPES[tuple] = TupleModel
MODELED_TYPES[dict] = DictModel
MODELED_TYPES[set] = SetModel
MODELED_TYPES[frozenset] = FrozenSetModel
