from typing import Sequence, TypeVar

from sqlalchemy import alias, select
from sqlalchemy.orm import aliased

from artigraph.db import current_session
from artigraph.orm.node import Node, NodeMetadata
from artigraph.utils import syncable

T = TypeVar("T")
N = TypeVar("N", bound=Node)


def group_nodes_by_parent_id(nodes: Sequence[N]) -> dict[int | None, list[N]]:
    """Group nodes by their parent ID."""
    grouped_nodes: dict[int | None, list[N]] = {}
    for node in nodes:
        grouped_nodes.setdefault(node.parent_id, []).append(node)
    return grouped_nodes


@syncable
async def create_metadata(node: Node, metadata: dict[str, str]) -> None:
    """Create metadata for a node."""
    metadata = [
        NodeMetadata(
            node_id=node.id,
            key=key,
            value=value,
        )
        for key, value in metadata.items()
    ]
    async with current_session() as session:
        session.add_all(metadata)
        await session.commit()


@syncable
async def read_metadata(node: Node) -> dict[str, str]:
    """Read the metadata for a node."""
    stmt = select(NodeMetadata).where(NodeMetadata.node_id == node.id)
    async with current_session() as session:
        result = await session.execute(stmt)
        metadata_records = result.scalars().all()
    return {record.key: record.value for record in metadata_records}


@syncable
async def read_direct_children(
    root_node: Node,
    node_types: Sequence[type[N]] = (),
) -> Sequence[N]:
    """Read the direct children of a node."""
    stmt = select(Node).where(Node.parent_id == root_node.id)
    for nt in node_types:
        node_type_name = nt.__mapper_args__["polymorphic_identity"]
        stmt = stmt.where(Node.type == node_type_name)
    async with current_session() as session:
        result = await session.execute(stmt)
        children = result.scalars().all()

    # we know we've filtered appropriately, so we can ignore the type check
    return children  # type: ignore


@syncable
async def read_recursive_children(
    root_node: Node,
    node_types: Sequence[type[N]] = (),
) -> Sequence[N]:
    """Use a recursive query to read the children of a node."""

    # Create an alias for the node table to use in the recursive query.
    node = alias(Node.__table__)
    node_alias = aliased(Node, node)

    # Create a recursive query to select the children of the root node.
    stmt = (
        select(node)
        .where(node.c.parent_id == root_node.id)
        .where(
            node.c.node_type.in_([nt.__mapper_args__["polymorphic_identity"] for nt in node_types])
        )
        .cte(recursive=True)
        .union_all(
            select(node)
            .join(node_alias, node_alias.c.parent_id == node.c.id)  # type: ignore
            .where(
                node.c.node_type.in_(
                    [nt.__mapper_args__["polymorphic_identity"] for nt in node_types]
                )
            )
        )
    )

    # Execute the query.
    async with current_session() as session:
        result = await session.execute(stmt)  # type: ignore
        children = result.scalars().all()

    return children
