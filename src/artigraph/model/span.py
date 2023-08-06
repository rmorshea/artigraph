from sqlalchemy import select

from artigraph.api.span import with_group_id
from artigraph.db import current_session
from artigraph.model.base import BaseModel, create_model, read_model
from artigraph.orm.artifact import BaseArtifact
from artigraph.utils import SessionBatch


@with_group_id
async def add_span_model(span_id: int, *, label: str, artifact: BaseModel) -> int:
    """Add an artifact to the span and return its ID"""
    return await create_model(label, artifact, parent_id=span_id)


@with_group_id
async def add_span_models(span_id: int, artifacts: dict[str, BaseModel]) -> dict[str, int]:
    """Add artifacts to the span and return their IDs."""
    return {
        # FIXME: Not really sure why we can't do this concurrently.
        # If we do, we sometimes don't write all records.
        k: await add_span_model(span_id, label=k, artifact=a)
        for k, a in artifacts.items()
    }


@with_group_id
async def get_span_model(span_id: int, *, label: str) -> BaseModel:
    """Load an artifact for this span."""
    async with current_session() as session:
        cmd = (
            select(BaseArtifact.node_id)
            .where(BaseArtifact.artifact_label == label)
            .where(BaseArtifact.node_parent_id == span_id)
        )
        result = await session.execute(cmd)
        node_id = result.scalar_one()
        return await read_model(node_id)


@with_group_id
async def read_span_models(span_id: int) -> dict[str, BaseModel]:
    """Load all artifacts for this span."""
    artifact_models: dict[str, BaseModel] = {}
    async with current_session() as session:
        cmd = (
            select(BaseArtifact.node_id, BaseArtifact.artifact_label)
            .where(BaseArtifact.artifact_label.is_not(None))
            .where(BaseArtifact.node_parent_id == span_id)
        )
        result = await session.execute(cmd)
        node_ids_and_labels = list(result.all())

    if not node_ids_and_labels:
        return artifact_models

    node_ids, artifact_labels = zip(*node_ids_and_labels)
    for label, model in zip(
        artifact_labels,
        await SessionBatch().map(read_model, node_ids).gather(),
    ):
        artifact_models[label] = model

    return artifact_models
