from __future__ import annotations

import asyncio
import re
import sys
from abc import ABCMeta
from dataclasses import KW_ONLY, dataclass
from functools import partial
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Coroutine,
    Generic,
    Sequence,
    TypeVar,
    cast,
    dataclass_transform,
)

from anyio import to_thread
from typing_extensions import ParamSpec, Self

F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
R = TypeVar("R")

SLUG_REPLACE_PATTERN = re.compile(r"[^a-z0-9]+")
"""A pattern for replacing non-alphanumeric characters in slugs"""


def create_sentinel(name: str) -> Any:
    """Create a sentinel object."""
    return cast(Any, type(name, (), {"__repr__": lambda _: name}))()


UNDEFINED = create_sentinel("UNDEFINED")
"""A sentinel for undefined values"""


def slugify(string: str) -> str:
    """Convert a string to a slug."""
    return SLUG_REPLACE_PATTERN.sub("-", string.lower()).strip("-")


async def run_in_thread(func: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """Span a sync function in a thread."""
    return await to_thread.run_sync(partial(func, *args, **kwargs))  # type: ignore


class TaskBatch(Generic[R]):
    """A batch of coroutines that are each executed with their own session"""

    def __init__(self) -> None:
        self._funcs: list[Callable[[], Coroutine[None, None, R]]] = []

    def add(
        self,
        func: Callable[P, Coroutine[None, None, R]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> Self:
        """Add a new task to the batch"""
        self._funcs.append(lambda: func(*args, **kwargs))
        return self

    def map(  # noqa: A003
        self,
        func: Callable[..., Coroutine[None, None, R]],
        *mapped_args: Sequence[Any],
    ) -> Self:
        """Map the given function to each set of arguments"""
        for args in zip(*mapped_args):
            self.add(func, *args)
        return self

    async def gather(self) -> Sequence[R]:
        """Execute all tasks in the batch and return the results"""
        return await asyncio.gather(*[t() for t in self._funcs])

    async def as_completed(self) -> AsyncIterator[R]:
        """Execute all tasks in the batch and return the results as they complete"""
        for t in asyncio.as_completed([t() for t in self._funcs]):
            yield await t


def get_subclasses(cls: type[R]) -> list[type[R]]:
    return [cls, *(s for c in cls.__subclasses__() for s in get_subclasses(c))]


@dataclass_transform()
class _DataclassMeta(ABCMeta):
    def __new__(
        cls,
        name: str,
        bases: tuple[type[Any, ...]],
        namespace: dict[str, Any],
        **kwargs: Any,
    ):
        self = super().__new__(cls, name, bases, namespace, **kwargs)
        kwargs = kwargs if sys.version_info < (3, 10) else {"kw_only": True, **kwargs}
        self = dataclass(**kwargs)(self)
        return self


class Dataclass(metaclass=_DataclassMeta):
    """All subclasses are treated as keyword only dataclasses"""

    _: KW_ONLY
