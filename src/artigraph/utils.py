import re
from functools import partial, wraps
from typing import Any, Callable, Coroutine, Protocol, TypeVar, cast

from anyio import from_thread, to_thread
from typing_extensions import ParamSpec

F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
R = TypeVar("R")

SLUG_REPLACE_PATTERN = re.compile(r"[^a-z0-9]+")
"""A pattern for replacing non-alphanumeric characters in slugs"""

UNDEFINED = cast(Any, type("UNDEFINED", (), {"__repr__": lambda _: "UNDEFINED"}))()
"""A sentinel for undefined values"""


def syncable(async_function: F) -> "Syncable[F]":
    """Make an async function callable synchronously via an added 'sync' attribute."""
    async_function.sync = _syncify(async_function)  # type: ignore[attr-defined]
    return async_function  # type: ignore[return-value]


def slugify(string: str) -> str:
    """Convert a string to a slug."""
    return SLUG_REPLACE_PATTERN.sub("-", string.lower()).strip("-")


async def run_in_thread(func: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """Run a sync function in a thread."""
    return await to_thread.run_sync(partial(func, *args, **kwargs))  # type: ignore


class Syncable(Protocol[F]):
    """A callable that can be called synchronously or asynchronously."""

    __call__: F
    sync: Callable[..., Any]


def _syncify(async_function: Callable[P, Coroutine[None, None, R]]) -> Callable[P, R]:
    @wraps(async_function)
    def sync_function(*args: P.args, **kwargs: P.kwargs) -> R:
        with from_thread.start_blocking_portal() as portal:
            return portal.call(partial(async_function, *args, **kwargs))

    return sync_function
