from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    ContextManager,
    Iterator,
    Literal,
    TypeVar,
    overload,
)

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

E = TypeVar("E", bound=AsyncEngine)

_CURRENT_ASYNC_ENGINE: ContextVar[AsyncEngine] = ContextVar("CURRENT_ASYNC_ENGINE")
_CURRENT_ASYNC_SESSION: ContextVar[AsyncSession] = ContextVar("CURRENT_ASYNC_SESSION")


@contextmanager
def engine_context(engine: E) -> Iterator[E]:
    """Define which engine to use in the context."""
    reset = set_engine(engine)
    try:
        yield engine
    finally:
        reset()


def set_engine(engine: AsyncEngine) -> Callable[[], None]:
    """Set the current engine."""
    var = _CURRENT_ASYNC_ENGINE
    token = var.set(engine)
    return lambda: var.reset(token)


@asynccontextmanager
async def current_session(**kwargs: Any) -> AsyncIterator[AsyncSession]:
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

    async with AsyncSession(engine, **kwargs) as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
