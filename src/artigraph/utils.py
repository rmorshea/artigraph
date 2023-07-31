from functools import partial, wraps
from typing import Any, Callable, Coroutine, Protocol, TypeVar, cast

from anyio import from_thread
from typing_extensions import ParamSpec

F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
R = TypeVar("R")


UNDEFINED = cast(Any, type("UNDEFINED", (), {"__repr__": lambda: "UNDEFINED"}))
"""A sentinel for undefined values"""


def syncable(async_function: F) -> "Syncable[F]":
    """Make an async function callable synchronously via an added 'sync' attribute."""
    async_function.sync = _syncify(async_function)  # type: ignore[attr-defined]
    return async_function  # type: ignore[return-value]


async def run_in_thread(func: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """Run a sync function in a thread."""
    return await from_thread.run(partial(func, *args, **kwargs))  # type: ignore


class Syncable(Protocol[F]):
    """A callable that can be called synchronously or asynchronously."""

    __call__: F
    sync: Callable[..., Any]


def _syncify(async_function: Callable[P, Coroutine[None, None, R]]) -> Callable[P, R]:
    @wraps(async_function)
    def sync_function(*args: P.args, **kwargs: P.kwargs) -> R:
        return from_thread.run(partial(async_function, *args, **kwargs))

    return sync_function
