import re
from functools import partial
from typing import Any, Callable, TypeVar, cast

from anyio import to_thread
from typing_extensions import ParamSpec

F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
R = TypeVar("R")

SLUG_REPLACE_PATTERN = re.compile(r"[^a-z0-9]+")
"""A pattern for replacing non-alphanumeric characters in slugs"""

UNDEFINED = cast(Any, type("UNDEFINED", (), {"__repr__": lambda _: "UNDEFINED"}))()
"""A sentinel for undefined values"""


def slugify(string: str) -> str:
    """Convert a string to a slug."""
    return SLUG_REPLACE_PATTERN.sub("-", string.lower()).strip("-")


async def run_in_thread(func: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """Run a sync function in a thread."""
    return await to_thread.run_sync(partial(func, *args, **kwargs))  # type: ignore
