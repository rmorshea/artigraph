import asyncio
from contextvars import ContextVar, Token
from datetime import datetime, timezone
from typing import Any, Generic, Literal, Sequence, TypeVar, overload

from sqlalchemy import select
from typing_extensions import Self

from artigraph.api.artifact_model import ArtifactModel
from artigraph.api.node import read_descendants
from artigraph.db import current_session, session_context
from artigraph.orm.artifact import BaseArtifact
from artigraph.orm.run import Run

R = TypeVar("R", bound=Run)
_CURRENT_RUN_MANAGER: ContextVar["RunManager"] = ContextVar("CURRENT_RUN_MANAGER")


@overload
def run_manager(*, allow_none: Literal[True]) -> "RunManager | None":
    ...


@overload
def run_manager(*, allow_none: Literal[False] = ...) -> "RunManager":
    ...


def run_manager(*, allow_none: bool = False) -> "RunManager | None":
    """Get the current run context."""
    try:
        return _CURRENT_RUN_MANAGER.get()
    except LookupError:
        if allow_none:
            return None
        msg = "No current run context."
        raise LookupError(msg) from None


class RunManager(Generic[R]):
    """A run context."""

    def __init__(self, run: R):
        """Initialize the run context."""
        self.run = run
        self._run_context_reset_token: Token[RunManager]

    async def load_descendant_runs(self) -> Sequence[Run]:
        """Get the descendants of this run."""
        return await read_descendants(self.run.node_id)

    async def save_artifact(self, label: str, artifact: ArtifactModel) -> int:
        """Add an artifact to the run and return its ID"""
        return await save_artifact(self.run.node_id, label, artifact)

    async def save_artifacts(self, artifacts: dict[str, ArtifactModel]) -> dict[str, int]:
        """Add artifacts to the run and return their IDs."""
        return await save_artifacts(self.run.node_id, artifacts)

    async def load_artifact(self, label: str) -> ArtifactModel:
        """Load an artifact for this run."""
        return await load_artifact(self.run.node_id, label)

    async def load_artifacts(self) -> dict[str, ArtifactModel]:
        """Load all artifacts for this run."""
        return await load_artifacts(self.run.node_id)

    async def load_descendant_artifacts(self) -> dict[int, dict[str, ArtifactModel]]:
        """Load all artifacts for this run."""
        run_ids = [r.node_id for r in await self.load_descendant_runs()]
        artifacts = await asyncio.gather(*[load_artifacts(i) for i in run_ids])
        return dict(zip(run_ids, artifacts))

    async def __aenter__(self) -> Self:
        """Enter the run context."""
        parent_run_context = run_manager(allow_none=True)
        parent_run_id = parent_run_context.run.node_id if parent_run_context else None
        if self.run.node_id is None:
            async with session_context(expire_on_commit=False) as session:
                self.run.node_parent_id = parent_run_id
                session.add(self.run)
                await session.commit()
                await session.refresh(self.run)
                self.run_id = self.run.node_id
        self._run_context_reset_token = _CURRENT_RUN_MANAGER.set(self)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        """Exit the run context."""
        _CURRENT_RUN_MANAGER.reset(self._run_context_reset_token)
        async with session_context(expire_on_commit=False) as session:
            self.run.run_finished_at = datetime.now(timezone.utc)
            session.add(self.run)
            await session.commit()


async def run_descendants(run_id: int) -> Sequence[Run]:
    """Get all descendants of a run."""
    return await read_descendants(run_id, Run)


async def save_artifact(run_id: int, label: str, artifact: ArtifactModel) -> int:
    """Add an artifact to the run and return its ID"""
    return await artifact.save(label, run_id)


async def save_artifacts(run_id: int, artifacts: dict[str, ArtifactModel]) -> dict[str, int]:
    """Add artifacts to the run and return their IDs."""
    ids = await asyncio.gather(*[save_artifact(run_id, k, a) for k, a in artifacts.items()])
    return dict(zip(artifacts.keys(), ids))


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
        node_ids, artifact_labels = zip(*list(result.all()))
        for label, model in zip(
            artifact_labels, await asyncio.gather(*[ArtifactModel.load(n) for n in node_ids])
        ):
            artifact_models[label] = model
    return artifact_models
