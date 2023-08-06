from typing import Any, MutableSequence, Sequence, TypeVar

from typing_extensions import Self

from artigraph.model.base import BaseModel, FieldConfig

T = TypeVar("T")


class ListModel(BaseModel, MutableSequence[str, T]):
    """A list of artifacts"""

    @classmethod
    def model_migrate(cls, _: int, kwargs: dict[str, Any]) -> Self:
        """Migrate the artifact model to a new version."""
        return cls(kwargs)

    def model_data(self) -> dict[str, tuple[T, FieldConfig]]:
        """The data for the artifact model."""
        return {str(i): (v, FieldConfig()) for i, v in enumerate(self._data)}

    def __init__(self, value: Sequence[T] | None = None, /, **kwargs: T) -> None:
        if value is not None and kwargs:
            msg = "Cannot specify both args and kwargs"
            raise TypeError(msg)
        elif value:
            self._data = dict(enumerate(value))
        else:
            self._data = {int(k): v for k, v in kwargs.items()}

    def __getitem__(self, index: int) -> T:
        return self._data[index]

    def __setitem__(self, index: int, value: T) -> None:
        self._data[index] = value

    def __delitem__(self, index: int) -> None:
        del self._data[index]

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        sorted_items = sorted(self._data.items())
        return repr([v for _, v in sorted_items])
