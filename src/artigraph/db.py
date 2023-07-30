from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import (
    AsyncContextManager,
    AsyncIterator,
    Callable,
    ContextManager,
    Iterator,
    Literal,
    overload,
)

from sqlalchemy import Engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
)
from sqlalchemy.orm import Session

_CURRENT_SYNC_ENGINE: ContextVar[Engine] = ContextVar("CURRENT_SYNC_ENGINE")
_CURRENT_ASYNC_ENGINE: ContextVar[AsyncEngine] = ContextVar("CURRENT_ASYNC_ENGINE")
_CURRENT_SYNC_SESSION: ContextVar[Session] = ContextVar("CURRENT_SYNC_SESSION")
_CURRENT_ASYNC_SESSION: ContextVar[AsyncSession] = ContextVar("CURRENT_ASYNC_SESSION")


@contextmanager
def engine_context(engine: Engine | AsyncEngine) -> Iterator[None]:
    """Define which engine to use in the context."""
    reset = set_engine(engine)
    try:
        yield
    finally:
        reset()


def set_engine(engine: Engine | AsyncEngine) -> Callable[[], None]:
    """Set the current engine."""
    if isinstance(engine, Engine):
        var = _CURRENT_SYNC_ENGINE
    elif isinstance(engine, AsyncEngine):
        var = _CURRENT_ASYNC_ENGINE
    else:
        msg = f"Unsupported engine type: {type(engine)}"
        raise TypeError(msg)
    token = var.set(engine)
    return lambda: var.reset(token)


@overload
def enter_session(*, sync: Literal[True]) -> ContextManager[Session]:
    ...


@overload
def enter_session(*, sync: Literal[False] = ...) -> AsyncContextManager[AsyncSession]:
    ...


def enter_session(
    *, sync: bool = False
) -> ContextManager[Session] | AsyncContextManager[AsyncSession]:
    """A context manager for a database session."""
    if sync:
        return _sync_session_context()
    else:
        return _async_session_context()


@contextmanager
def _sync_session_context() -> Iterator[Session]:
    """A context manager for a synchronous database session."""
    try:
        session = _CURRENT_SYNC_SESSION.get()
    except LookupError:
        pass
    else:
        yield session
        return

    try:
        engine = _CURRENT_SYNC_ENGINE.get()
    except LookupError:
        msg = "No current synchronous engine"
        raise LookupError(msg) from None

    with Session(engine) as session:
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


@asynccontextmanager
async def _async_session_context() -> AsyncIterator[AsyncSession]:
    """A context manager for an asynchronous database session."""
    try:
        session = _CURRENT_ASYNC_SESSION.get()
    except LookupError:
        pass
    else:
        yield session
        return

    try:
        engine = _CURRENT_ASYNC_ENGINE.get()
    except LookupError:
        msg = "No current asynchronous engine"
        raise LookupError(msg) from None

    async with AsyncSession(engine) as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
