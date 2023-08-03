from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import AsyncIterator, Callable, Iterator

from artigraph.db import current_session
from artigraph.orm.run import Run

_CURRENT_RUN_ID: ContextVar[int] = ContextVar("CURRENT_RUN_ID")


@asynccontextmanager
async def new_run(run: Run) -> AsyncIterator[None]:
    """Enter a run context."""
    async with current_session() as session:
        session.add(run)
        await session.commit()
        await session.refresh(run)
        await session.commit()

    with run_context(run):
        yield

    async with current_session() as session:
        run.run_finished_at = datetime.now(timezone.utc)
        session.add(run)
        await session.commit()


@contextmanager
def run_context(run: Run | int) -> Iterator[None]:
    """Enter a run context."""
    reset = set_run(run)
    try:
        yield
    finally:
        reset()


def set_run(run: Run | int) -> Callable[[], None]:
    """Set the current run."""
    run_id = run.node_id if isinstance(run, Run) else run
    token = _CURRENT_RUN_ID.set(run_id)
    return lambda: _CURRENT_RUN_ID.reset(token)
