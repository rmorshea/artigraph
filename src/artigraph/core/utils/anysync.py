from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from functools import wraps
from types import TracebackType
from typing import (
    Any,
    Callable,
    Concatenate,
    Coroutine,
    Generic,
    ParamSpec,
    Protocol,
    TypeVar,
    cast,
)

P = ParamSpec("P")
R = TypeVar("R")


def anysync(func: Callable[P, Coroutine[None, None, R]]) -> AnySyncFunc[P, R]:
    """Create a function that can be called synchronously or asynchronously.

    This is achieved by checking if the current thread is running an event loop.
    If it is, the function is called asynchronously. Otherwise, it is called
    synchronously.

    To force a function to be called synchronously, use the `s` attribute.
    To force a function to be called asynchronously, use the `a` attribute.
    """
    anysnc_f = _create_anysync_function(func)
    anysnc_f.a = func
    anysnc_f.s = _create_sync_function(func)
    return cast(AnySyncFunc[P, R], anysnc_f)


def anysyncmethod(
    method: Callable[Concatenate[Any, P], Coroutine[None, None, R]]
) -> AnySyncMethod[P, R]:
    """Create a method that can be called synchronously or asynchronously.

    See [anysync][artigraph.utils.anysync.anysync] for more information.
    """
    return AnySyncMethod(method)


class AnySyncFunc(Protocol[P, R]):
    """A function that can be called synchronously or asynchronously."""

    s: Callable[P, R]
    a: Callable[P, Coroutine[None, None, R]]
    __call__: Callable[P, Coroutine[None, None, R] | R]
    __get__: Callable[..., Any]


class AnySyncMethod(Generic[P, R]):
    """A method that can be called synchronously or asynchronously."""

    def __init__(self, func: Callable[Concatenate[Any, P], Coroutine[None, None, R]]):
        self._func = func

    def __get__(self, obj: Any, objtype: Any = None) -> AnySyncFunc[P, R]:
        bound_func = self._func.__get__(obj, objtype)
        return anysync(bound_func)


class AnySyncContextManager(Generic[R]):
    """A context manager that can be used synchronously or asynchronously."""

    def _enter(self) -> None:  # nocov
        # these methods exist primarilly to control contextvars is a reliable manner
        pass

    def _exit(self) -> None:  # nocov
        # these methods exist primarilly to control contextvars is a reliable manner
        pass

    async def _aenter(self) -> R:  # nocov
        raise NotImplementedError()

    async def _aexit(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None | bool:  # nocov
        pass

    def __enter__(self) -> R:
        self._enter()
        return self._anyenter.s()

    def __exit__(self, *args: Any) -> bool | None:
        try:
            self._anyexit.s(*args)
        finally:
            self._exit()

    async def __aenter__(self) -> R:
        self._enter()
        return await self._anyenter.a()

    async def __aexit__(self, *args: Any) -> bool | None:
        try:
            return await self._anyexit.a(*args)
        finally:
            self._exit()

    @anysyncmethod
    async def _anyenter(self) -> R:
        return await self._aenter()

    @anysyncmethod
    async def _anyexit(self, *args: Any) -> bool | None:
        return await self._aexit(*args)


def _create_anysync_function(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(func(*args, **kwargs))
        else:
            return func(*args, **kwargs)

    return wrapper


def _create_sync_function(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # We pass the current context to the task, but make no attempt to get the
        # changes that were made there. That doesn't seem to be reliable (not sure why).
        context = copy_context()

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            return _THREAD_POOL.submit(
                lambda: context.run(asyncio.run, func(*args, **kwargs))
            ).result()

        return context.run(asyncio.run, func(*args, **kwargs))

    return wrapper


_THREAD_POOL = ThreadPoolExecutor(max_workers=1)
