from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import wraps
from types import TracebackType
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
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
Y = TypeVar("Y")
S = TypeVar("S")
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


def anysynccontextmanager(
    func: Callable[P, AsyncIterator[R]]
) -> Callable[P, _AnySyncGeneratorContextManager[R]]:
    make_ctx = asynccontextmanager(func)

    @wraps(make_ctx)
    def wrapper(*args: Any, **kwargs: Any) -> _AnySyncGeneratorContextManager[R]:
        return _AnySyncGeneratorContextManager(make_ctx(*args, **kwargs))

    return wrapper


class AnySyncFunc(Protocol[P, R]):
    """A function that can be called synchronously or asynchronously."""

    s: Callable[P, R]
    a: Callable[P, Coroutine[None, None, R]]
    __call__: Callable[P, Coroutine[None, None, R] | R]
    __get__: Callable[..., Any]


class AnySyncMethod(Generic[P, R]):
    """A method that can be called synchronously or asynchronously."""

    def __init__(self, func: Callable[P, Coroutine[None, None, R]]):
        self._func = func

    def __get__(self, obj: Any, objtype: Any = None) -> AnySyncFunc[P, R]:
        bound_func = self._func.__get__(obj, objtype)
        return anysync(bound_func)


class AnySynContextManager(Generic[R]):
    async def _enter(self) -> R:
        raise NotImplementedError()

    async def _exit(
        self,
        exc_type: type[BaseException],
        exc_value: BaseException,
        exc_tb: TracebackType,
    ) -> None | bool:
        raise NotImplementedError()

    def __enter__(self) -> R:
        return self._anyenter.s()

    def __exit__(self, *args: Any) -> None:
        self._anyexit.s(*args)

    async def __aenter__(self) -> R:
        return await self._anyenter.a()

    async def __aexit__(self, *args: Any) -> None:
        return await self._anyexit.a(*args)

    @anysyncmethod
    async def _anyenter(self) -> R:
        return await self._enter()

    @anysyncmethod
    async def _anyexit(self, *args: Any) -> bool | None:
        return await self._exit(*args)


class _AnySyncGeneratorContextManager(AnySynContextManager[R]):
    def __init__(self, ctx: AsyncContextManager) -> None:
        self._ctx = ctx

    async def _enter(self) -> R:
        return await self._ctx.__aenter__()

    async def _exit(self, *args) -> None:
        return await self._ctx.__aexit__(*args)


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
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(func(*args, **kwargs))
        else:
            return _THREAD_POOL.submit(lambda: asyncio.run(func(*args, **kwargs))).result()

    return wrapper


_THREAD_POOL = ThreadPoolExecutor(max_workers=1)
