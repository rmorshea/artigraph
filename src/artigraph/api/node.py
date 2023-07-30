from typing import Sequence, TypeVar

from sqlalchemy import alias, join, select
from sqlalchemy.orm import aliased

from artigraph.db import enter_session
from artigraph.orm.node import Node, NodeMetadata
from artigraph.utils import syncable

T = TypeVar("T")
N = TypeVar("N", bound=Node)


def group_nodes_by_parent_id(nodes: Sequence[N]) -> dict[int, Sequence[N]]:
    """Group nodes by their parent ID."""
    grouped_nodes: dict[int, list[N]] = {}
    for node in nodes:
        grouped_nodes.setdefault(node.parent_id, []).append(node)
    return grouped_nodes


@syncable
async def read_metadata(node: Node) -> dict[str, str]:
    """Read the metadata for a node."""
    stmt = select(NodeMetadata).where(NodeMetadata.node_id == node.id)
    async with enter_session() as session:
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
        stmt = stmt.where(Node.node_type == node_type_name)
    async with enter_session() as session:
        result = await session.execute(stmt)
        children = result.scalars().all()
    return children


@syncable
async def read_recursive_children(
    root_node: Node,
    node_types: Sequence[type[N]] = (),
) -> Sequence[N]:
    """Recursively read the children of a node - returns a mapping from node ID to children."""

    # Create a recursive CTE to find all of the children of the root node.
    node = aliased(Node)
    parent = aliased(Node)
    cte = select(node).where(node.parent_id == parent.id).cte(recursive=True, name="children")
    cte = cte.union_all(select(node).where(node.parent_id == parent.id).select_from(cte))
    cte = cte.where(parent.id == root_node.id)

    # Join the CTE to the node metadata table to get the metadata for each node.
    metadata = alias(NodeMetadata)
    stmt = (
        select(node, metadata)
        .select_from(join(cte, metadata, node.id == metadata.node_id))
        .order_by(node.id)
    )

    for nt in node_types:
        node_type_name = nt.__mapper_args__["polymorphic_identity"]
        stmt = stmt.where(Node.node_type == node_type_name)

    async with enter_session() as session:
        result = await session.execute(stmt)
        return result.scalars().all()
