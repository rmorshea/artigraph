from functools import partial, wraps
from typing import Any, Callable, Coroutine, Protocol, TypeVar, cast

from anyio import from_thread
from typing_extensions import ParamSpec

F = TypeVar("F", bound=Callable)
P = ParamSpec("P")
R = TypeVar("R")


UNDEFINED = cast(Any, type("UNDEFINED", (), {"__repr__": lambda: "UNDEFINED"}))
"""A sentinel for undefined values"""


def syncable(async_function: F) -> "Syncable[F]":
    async_function.sync = _syncify(async_function)
    return async_function  # type: ignore  (we know it's Syncable)


class Syncable(Protocol[F]):
    """A callable that can be called synchronously or asynchronously."""

    __call__: F
    sync: Callable[..., Any]


def _syncify(async_function: Callable[P, Coroutine[None, None, R]]) -> Callable[P, R]:
    @wraps(async_function)
    def sync_function(*args: P.args, **kwargs: P.kwargs) -> R:
        return from_thread.run(partial(async_function, *args, **kwargs))

    return sync_function
