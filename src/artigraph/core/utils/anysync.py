from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import (
    Any,
    Callable,
    Concatenate,
    Coroutine,
    Generic,
    ParamSpec,
    Protocol,
    TypeVar,
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
    return anysnc_f


def anysyncmethod(
    method: Callable[Concatenate[Any, P], Coroutine[None, None, R]]
) -> AnySyncMethod[P, R]:
    """Create a method that can be called synchronously or asynchronously.

    See [anysync][artigraph.utils.anysync.anysync] for more information.
    """
    return AnySyncMethod(method)


class AnySyncFunc(Protocol[P, R]):
    __call__: Callable[P, Coroutine[None, None, R] | R]
    s: Callable[P, R]
    a: Callable[P, Coroutine[None, None, R]]
    __get__: Callable[..., Any]


class AnySyncMethod(Generic[P, R]):
    def __init__(self, func: Callable[P, Coroutine[None, None, R]]):
        self._func = func

    def __get__(self, obj: Any, objtype: Any = None) -> AnySyncFunc[P, R]:
        bound_func = self._func.__get__(obj, objtype)
        return anysync(bound_func)


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
