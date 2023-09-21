from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from types import TracebackType
from typing import AsyncContextManager, Callable, Iterator, TypeVar

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from typing_extensions import ParamSpec

from artigraph.core.orm.base import OrmBase
from artigraph.core.utils.anysync import AnySyncContextManager

E = TypeVar("E", bound=AsyncEngine)
P = ParamSpec("P")
R = TypeVar("R")

_CREATE_TABLES: ContextVar[bool] = ContextVar("CREATE_TABLES", default=False)
_CURRENT_ENGINE: ContextVar[AsyncEngine] = ContextVar("CURRENT_ENGINE")
_CURRENT_SESSION: ContextVar[AsyncSession | None] = ContextVar("CURRENT_SESSION", default=None)


@contextmanager
def current_engine(
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


def current_session(
    session_maker: async_sessionmaker[AsyncSession] | None = None,
) -> AsyncContextManager[AsyncSession]:
    """A context manager for an asynchronous database session."""
    return _CurrentSession(session_maker)


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


def get_engine() -> AsyncEngine:
    """Get the current engine."""
    try:
        engine = _CURRENT_ENGINE.get()
    except LookupError:  # nocov
        msg = "No current asynchronous engine"
        raise LookupError(msg) from None
    return engine


def set_session(session: AsyncSession | None) -> Callable[[], None]:
    """Set the current session."""
    var = _CURRENT_SESSION
    token = var.set(session)
    return lambda: var.reset(token)


def get_session() -> AsyncSession | None:
    """Get the current session."""
    return _CURRENT_SESSION.get()


class _CurrentSession(AnySyncContextManager[AsyncSession]):
    def __init__(self, session_maker: async_sessionmaker[AsyncSession] | None) -> None:
        self._session_maker = session_maker

    async def _aenter(self) -> AsyncSession:
        if self._prior_session:
            return self._prior_session

        if _CREATE_TABLES.get():
            async with get_engine().begin() as conn:
                await conn.run_sync(OrmBase.metadata.create_all)
            _CREATE_TABLES.set(False)  # no need to create next time

        return await self._own_session.__aenter__()

    def _enter(self) -> None:
        self._prior_session = get_session()
        if not self._prior_session:
            make = self._session_maker or async_sessionmaker(get_engine(), expire_on_commit=False)
            self._own_session = make()
            self._reset_session = set_session(self._own_session)
        else:
            self._reset_session = lambda: None

    def _exit(self) -> None:
        self._reset_session()

    async def _aexit(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        if self._prior_session is None:
            if exc_type is not None:
                await self._own_session.rollback()
            else:
                await self._own_session.commit()
            await self._own_session.__aexit__(exc_type, exc_value, exc_tb)
        else:
            return None
