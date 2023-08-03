import asyncio
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from typing import Any, Generic, Literal, TypeVar, overload

from sqlalchemy import select
from typing_extensions import Self

from artigraph.api.artifact_model import ArtifactModel, get_artifact_model_type_by_name
from artigraph.db import current_session, session_context
from artigraph.orm.artifact import BaseArtifact
from artigraph.orm.run import Run
from artigraph.utils import run_in_thread, syncable

R = TypeVar("R", bound=Run)
_CURRENT_RUN_CONTEXT: ContextVar["RunContext"] = ContextVar("CURRENT_RUN_CONTEXT")


@overload
def current_run_context(*, allow_none: Literal[True]) -> "RunContext | None":
    ...


@overload
def current_run_context(*, allow_none: Literal[False] = ...) -> "RunContext":
    ...


def current_run_context(*, allow_none: bool = False) -> "RunContext | None":
    """Get the current run context."""
    try:
        return _CURRENT_RUN_CONTEXT.get()
    except LookupError:
        if allow_none:
            return None
        msg = "No current run context."
        raise LookupError(msg) from None


class RunContext(Generic[R]):
    """A run context."""

    def __init__(self, run: R):
        """Initialize the run context."""
        self.run = run
        self.run_id: int
        self._run_context_reset_token: Token[RunContext]

    @syncable
    async def save_artifact(self, label: str, artifact: ArtifactModel) -> int:
        """Add an artifact to the run and return its ID"""
        return await save_artifact(self.run.node_id, label, artifact)

    @syncable
    async def save_artifacts(self, artifacts: dict[str, ArtifactModel]) -> dict[str, int]:
        """Add artifacts to the run and return their IDs."""
        return await save_artifacts(self.run.node_id, artifacts)

    @syncable
    async def load_artifact(self, label: str) -> ArtifactModel:
        """Load an artifact for this run."""
        return await load_artifact(self.run.node_id, label)

    @syncable
    async def load_artifacts(self) -> dict[str, ArtifactModel]:
        """Load all artifacts for this run."""
        return await load_artifacts(self.run.node_id)

    def __enter__(self) -> Self:
        return run_in_thread(self.__aenter__)

    def __exit__(self, *exc: Any) -> None:
        return run_in_thread(self.__exit__, *exc)

    async def __aenter__(self) -> Self:
        """Enter the run context."""
        if self.run.node_id is None:
            async with session_context(expire_on_commit=False) as session:
                session.add(self.run)
                await session.commit()
                await session.refresh(self.run)
                self.run_id = self.run.node_id
        self._run_context_reset_token = _CURRENT_RUN_CONTEXT.set(self)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        """Exit the run context."""
        _CURRENT_RUN_CONTEXT.reset(self._run_context_reset_token)
        async with session_context(expire_on_commit=False) as session:
            self.run.run_finished_at = datetime.now(timezone.utc)
            session.add(self.run)
            await session.commit()


@syncable
async def save_artifact(run_id: int, label: str, artifact: ArtifactModel) -> int:
    """Add an artifact to the run and return its ID"""
    return await artifact.save(label, run_id)


@syncable
async def save_artifacts(run_id: int, artifacts: dict[str, ArtifactModel]) -> dict[str, int]:
    """Add artifacts to the run and return their IDs."""
    artifacts = await asyncio.gather(*[save_artifact(run_id, k, a) for k, a in artifacts.items()])
    return dict(zip(artifacts.keys(), artifacts))


@syncable
async def load_artifact(run_id: int, label: str) -> ArtifactModel:
    """Load an artifact for this run."""
    async with current_session() as session:
        stmt = (
            select(BaseArtifact.node_id)
            .where(BaseArtifact.artifact_label == label)
            .where(BaseArtifact.node_parent_id == run_id)
        )
        result = await session.execute(stmt)
        node_id = result.scalar_one()
        return await ArtifactModel.load(node_id)


@syncable
async def load_artifacts(run_id: int) -> dict[str, ArtifactModel]:
    """Load all artifacts for this run."""
    artifact_models: dict[str, ArtifactModel] = {}
    async with current_session() as session:
        stmt = (
            select(BaseArtifact.node_id, BaseArtifact.artifact_label)
            .where(BaseArtifact.artifact_label.is_not(None))
            .where(BaseArtifact.node_parent_id == run_id)
        )
        result = await session.execute(stmt)
        node_ids, artifact_labels = zip(*result.scalars().all())
        for label, model in zip(
            artifact_labels, asyncio.gather(*[ArtifactModel.load(n) for n in node_ids])
        ):
            artifact_models[label] = model
    return artifact_models
