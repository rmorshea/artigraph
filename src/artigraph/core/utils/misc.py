from __future__ import annotations

import asyncio
import inspect
import re
import sys
from abc import ABCMeta
from dataclasses import KW_ONLY, dataclass, field
from functools import partial
from inspect import signature
from traceback import format_exception
from typing import (
    Any,
    Callable,
    Coroutine,
    Generic,
    Sequence,
    TypeVar,
    cast,
)

from anyio import to_thread
from typing_extensions import ParamSpec, Self, dataclass_transform

F = TypeVar("F", bound=Callable[..., Any])
P = ParamSpec("P")
R = TypeVar("R")

SLUG_REPLACE_PATTERN = re.compile(r"[^a-z0-9]+")
"""A pattern for replacing non-alphanumeric characters in slugs"""

if sys.version_info < (3, 11):  # nocov

    class ExceptionGroup(Exception):  # noqa: N818, A001
        """An exception that contains multiple exceptions

        A best effort attempt to replicate the `ExceptionGroup` from Python 3.11
        """

        def __init__(self, message: str, exceptions: Sequence[Exception], /) -> None:
            super().__init__(message)
            self.exceptions = exceptions

        def __str__(self) -> str:
            nl = "\n"  # can't include backslash in f-string
            tracebacks = nl.join(
                f"{index + 1} - {nl.join(format_exception(exc))}"
                for index, exc in enumerate(self.exceptions)
            )
            return f"{super().__str__()}\n\n{tracebacks}"

else:
    ExceptionGroup = ExceptionGroup  # noqa: A001,PLW0127


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

    def map(
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
        if not self._funcs:
            return []

        tasks = [asyncio.create_task(t()) for t in self._funcs]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        errors = list(filter(None, [d.exception() for d in done]))
        if errors:
            if pending:
                for t in pending:
                    t.cancel()
                await asyncio.wait(pending)
            errors = _raise_if_non_exception(errors)
            msg = "One or more tasks failed"
            raise ExceptionGroup(msg, errors) if len(errors) > 1 else errors[0]

        return [t.result() for t in tasks]


def get_subclasses(cls: type[R]) -> list[type[R]]:
    return [cls, *(s for c in cls.__subclasses__() for s in get_subclasses(c))]


_DATACLASS_KWONLY_PARAMS = {
    p.name
    for p in signature(dataclass).parameters.values()
    if p.kind is inspect.Parameter.KEYWORD_ONLY
}


@dataclass_transform(field_specifiers=(field,), kw_only_default=True)
class _FrozenDataclassMeta(ABCMeta):
    """A metaclass that makes all subclasses dataclasses"""

    def __new__(
        cls,
        name: str,
        bases: tuple[type[Any, ...]],
        namespace: dict[str, Any],
        *,
        kw_only: bool = True,
        frozen=True,
        **kwargs: Any,
    ):
        dataclass_kwargs = {k: kwargs.pop(k) for k in _DATACLASS_KWONLY_PARAMS if k in kwargs}
        self = super().__new__(cls, name, bases, namespace, **kwargs)
        self = dataclass(frozen=frozen, kw_only=kw_only, **dataclass_kwargs)(self)
        return self


class FrozenDataclass(metaclass=_FrozenDataclassMeta):
    """All subclasses are treated as frozen keyword-only dataclasses"""

    _: KW_ONLY


def _raise_if_non_exception(errors: Sequence[BaseException]) -> Sequence[Exception]:
    for e in errors:
        if not isinstance(e, Exception):  # nocov
            raise e
    return cast(Sequence[Exception], errors)
