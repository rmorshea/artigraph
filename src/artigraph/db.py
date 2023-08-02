from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Callable, Iterator, TypeVar

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

E = TypeVar("E", bound=AsyncEngine)

_CURRENT_ENGINE: ContextVar[AsyncEngine] = ContextVar("CURRENT_ASYNC_ENGINE")
_CURRENT_SESSION: ContextVar[AsyncSession] = ContextVar("CURRENT_ASYNC_SESSION")


@contextmanager
def engine_context(engine: E) -> Iterator[E]:
    """Define which engine to use in the context."""
    reset = set_engine(engine)
    try:
        yield engine
    finally:
        reset()


@asynccontextmanager
async def session_context(**kwargs: Any) -> AsyncIterator[AsyncSession]:
    """Define which session to use in the context."""
    async with async_sessionmaker(get_engine(), **kwargs)() as session:
        reset = set_session(session)
        try:
            yield session
        finally:
            reset()


@asynccontextmanager
async def current_session() -> AsyncIterator[AsyncSession]:
    """A context manager for an asynchronous database session."""
    try:
        session = get_session()
    except LookupError:
        pass
    else:
        yield session
        return

    async with AsyncSession(get_engine()) as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


def set_engine(engine: AsyncEngine) -> Callable[[], None]:
    """Set the current engine."""
    var = _CURRENT_ENGINE
    token = var.set(engine)
    return lambda: var.reset(token)


def get_engine() -> AsyncEngine:
    """Get the current engine."""
    try:
        return _CURRENT_ENGINE.get()
    except LookupError:  # nocov
        msg = "No current asynchronous engine"
        raise LookupError(msg) from None


def set_session(session: AsyncSession) -> Callable[[], None]:
    """Set the current session."""
    var = _CURRENT_SESSION
    token = var.set(session)
    return lambda: var.reset(token)


def get_session() -> AsyncSession:
    """Get the current session."""
    try:
        return _CURRENT_SESSION.get()
    except LookupError:  # nocov
        msg = "No current asynchronous session"
        raise LookupError(msg) from None
