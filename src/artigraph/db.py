from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Callable, Iterator, TypeVar

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from typing_extensions import ParamSpec

from artigraph.orm.base import Base

E = TypeVar("E", bound=AsyncEngine)
P = ParamSpec("P")
R = TypeVar("R")

_CREATE_TABLES: ContextVar[bool] = ContextVar("CREATE_TABLES", default=False)
_CURRENT_ENGINE: ContextVar[AsyncEngine] = ContextVar("CURRENT_ASYNC_ENGINE")
_CURRENT_SESSION: ContextVar[AsyncSession] = ContextVar("CURRENT_ASYNC_SESSION")


@contextmanager
def engine_context(
    engine: AsyncEngine | str,
    *,
    create_tables: bool = False,
) -> Iterator[AsyncEngine]:
    """Define which engine to use in the context."""
    if isinstance(engine, str):
        engine = create_async_engine(engine)
    reset = set_engine(engine, create_tables=create_tables)
    try:
        yield engine
    finally:
        reset()


@asynccontextmanager
async def session_context(**kwargs: Any) -> AsyncIterator[AsyncSession]:
    """Define which session to use in the context."""
    async with async_sessionmaker(await get_engine(), **kwargs)() as session:
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

    async with AsyncSession(await get_engine()) as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise


def set_engine(engine: AsyncEngine | str, *, create_tables: bool = False) -> Callable[[], None]:
    """Set the current engine and whether to try creating tables if they don't exist."""
    if isinstance(engine, str):
        engine = create_async_engine(engine)
    current_engine_token = _CURRENT_ENGINE.set(engine)
    create_tables_token = _CREATE_TABLES.set(create_tables)

    def reset() -> None:
        _CURRENT_ENGINE.reset(current_engine_token)
        _CREATE_TABLES.reset(create_tables_token)

    return reset


async def get_engine() -> AsyncEngine:
    """Get the current engine."""
    try:
        engine = _CURRENT_ENGINE.get()
    except LookupError:  # nocov
        msg = "No current asynchronous engine"
        raise LookupError(msg) from None
    if _CREATE_TABLES.get():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    return engine


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
