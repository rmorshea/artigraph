from __future__ import annotations

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

from artigraph.core.orm.base import OrmBase

E = TypeVar("E", bound=AsyncEngine)
P = ParamSpec("P")
R = TypeVar("R")

_CREATE_TABLES: ContextVar[bool] = ContextVar("CREATE_TABLES", default=False)
_CURRENT_ENGINE: ContextVar[AsyncEngine] = ContextVar("CURRENT_ENGINE")
_CURRENT_SESSION: ContextVar[AsyncSession | None] = ContextVar("CURRENT_SESSION", default=None)


@contextmanager
def engine_context(
    engine: AsyncEngine | str,
    *,
    create_tables: bool = False,
) -> Iterator[AsyncEngine]:
    """Define which engine to use in the context."""
    engine = create_async_engine(engine) if isinstance(engine, str) else engine
    reset = set_engine(engine, create_tables=create_tables)
    try:
        yield engine
    finally:
        reset()


@asynccontextmanager
async def new_session(**kwargs: Any) -> AsyncIterator[AsyncSession]:
    """Define which session to use in the context."""
    kwargs.setdefault("expire_on_commit", False)
    async with async_sessionmaker(await get_engine(), **kwargs)() as session:
        reset = set_session(session)
        try:
            yield session
        finally:
            reset()


@asynccontextmanager
async def current_session() -> AsyncIterator[AsyncSession]:
    """A context manager for an asynchronous database session."""
    session = get_session()

    if session is not None:
        yield session
        return

    async with AsyncSession(await get_engine(), expire_on_commit=False) as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        else:
            await session.commit()


def set_engine(engine: AsyncEngine | str, *, create_tables: bool = False) -> Callable[[], None]:
    """Set the current engine and whether to try creating tables if they don't exist.

    Tables are only created when the engine is retrieved for the first time.
    """
    engine = create_async_engine(engine) if isinstance(engine, str) else engine
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
            await conn.run_sync(OrmBase.metadata.create_all)
        _CREATE_TABLES.set(False)  # no need to create next time
    return engine


def set_session(session: AsyncSession | None) -> Callable[[], None]:
    """Set the current session."""
    var = _CURRENT_SESSION
    token = var.set(session)
    return lambda: var.reset(token)


def get_session() -> AsyncSession | None:
    """Get the current session."""
    return _CURRENT_SESSION.get()
