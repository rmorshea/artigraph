from typing import Any, Mapping, MutableMapping, TypeVar

from typing_extensions import Self

from artigraph.model.base import BaseModel, FieldConfig

T = TypeVar("T")


class DictModel(BaseModel, MutableMapping[str, T]):
    """A dictionary of artifacts"""

    @classmethod
    def model_migrate(cls, _: int, kwargs: dict[str, Any]) -> Self:
        """Migrate the artifact model to a new version."""
        return cls(kwargs)

    def model_data(self) -> dict[str, tuple[T, FieldConfig]]:
        """The data for the artifact model."""
        return {k: (v, FieldConfig()) for k, v in self._data.items()}

    def __init__(self, *args: Mapping[str, T], **kwargs: T) -> None:
        self._data: dict[str, T] = dict(*args, **kwargs)

    def __getitem__(self, key: str) -> T:
        return self._data[key]

    def __setitem__(self, key: str, value: T) -> None:
        self._data[key] = value

    def __iter__(self) -> Any:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return repr(self._data)
