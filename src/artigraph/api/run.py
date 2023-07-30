from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Callable, Iterator, TypeVar

from typing_extensions import ParamSpec

from artigraph.api.artifact_group import ArtifactGroup
from artigraph.db import enter_session
from artigraph.orm.run import Run
from artigraph.orm.run_parameter import RunParameter

P = ParamSpec("P")
A = TypeVar("A", bound=ArtifactGroup)
_CURRENT_RUN_ID: ContextVar[int] = ContextVar("CURRENT_RUN_ID")


@contextmanager
async def enter_run(run: Run, parameters: dict[str, Any]) -> Iterator[None]:
    """Enter a run context."""
    async with enter_session() as session:
        session.add(run)
        await session.commit()
        await session.refresh(run)
        for k, v in parameters.items():
            session.add(RunParameter(run_id=run.id, key=k, value=v))
        await session.commit()

    with run_context(run):
        yield

    async with enter_session() as session:
        run.finished_at = datetime.now(timezone.utc)
        session.add(run)
        session.commit()


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
    run_id = run.id if isinstance(run, Run) else run
    token = _CURRENT_RUN_ID.set(run_id)
    return lambda: _CURRENT_RUN_ID.reset(token)
