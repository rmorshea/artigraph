from __future__ import annotations

import asyncio
import re
from functools import partial
from typing import Any, Callable, Coroutine, Generic, Sequence, TypeVar, cast

from anyio import to_thread
from typing_extensions import ParamSpec, Self

from artigraph.db import session_context

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


class SessionBatch(Generic[R]):
    """A batch of coroutines that are each executed with their own session"""

    def __init__(self, **session_kwargs: Any) -> None:
        self._tasks: list[Callable[[], Coroutine[None, None, R]]] = []
        self._session_kwargs = session_kwargs

    def add(self, func: Callable[P, R], *args: P.args, **kwargs: P.kwargs) -> Self:
        """Add a new task to the batch"""

        async def wrapper():
            async with session_context(**self._session_kwargs):
                return await func(*args, **kwargs)

        self._tasks.append(wrapper)
        return self

    def map(self, func: Callable[..., R], *arg_sequences: Sequence[Any]) -> Self:  # noqa: A003
        """Map the given function to each set of arguments"""
        for args in zip(*arg_sequences):
            self.add(func, *args)
        return self

    async def gather(self) -> Sequence[R]:
        return await asyncio.gather(*[t() for t in self._tasks])
