from typing import Sequence, TypeVar

from sqlalchemy import delete, join, select
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
        grouped_nodes.setdefault(node.node_parent_id, []).append(node)
    return grouped_nodes


async def read_node_by_id(node_id: int, node_type: type[N] = Node) -> N:
    """Read a node by its ID."""
    stmt = select(node_type).where(node_type.node_id == node_id)
    async with current_session() as session:
        result = await session.execute(stmt)
        return result.scalar_one()


@syncable
async def node_exists(node_id: int) -> bool:
    """Check if a node exists."""
    stmt = select(Node.node_id).where(Node.node_id == node_id)
    async with current_session() as session:
        result = await session.execute(stmt)
        return bool(result.scalar_one_or_none())


@syncable
async def delete_nodes(node_ids: Sequence[int]) -> None:
    """Delete nodes."""
    async with current_session() as session:
        stmt = delete(Node).where(Node.node_id.in_(node_ids))
        await session.execute(stmt)
        await session.commit()


@syncable
async def create_metadata(node: Node, metadata: dict[str, str]) -> None:
    """Create metadata for a node."""
    metadata_nodes = [
        NodeMetadata(
            node_id=node.node_id,
            key=key,
            value=value,
        )
        for key, value in metadata.items()
    ]
    async with current_session() as session:
        session.add_all(metadata_nodes)
        await session.commit()


@syncable
async def create_parent_child_relationships(
    parent_child_pairs: Sequence[tuple[Node, Node]]
) -> None:
    """Create parent-to-child links between nodes."""
    async with current_session() as session:
        for parent, child in parent_child_pairs:
            child.node_parent_id = parent.node_id
            session.add(child)
        await session.commit()


@syncable
async def read_metadata(node: Node) -> dict[str, str]:
    """Read the metadata for a node."""
    stmt = select(NodeMetadata).where(NodeMetadata.node_id == node.node_id)
    async with current_session() as session:
        result = await session.execute(stmt)
        metadata_records = result.scalars().all()
    return {record.key: record.value for record in metadata_records}


@syncable
async def read_children(
    root_node: Node,
    node_type: type[N] = Node,
) -> Sequence[N]:
    """Read the direct children of a node."""
    stmt = select(node_type).where(node_type.node_parent_id == root_node.node_id)
    async with current_session() as session:
        result = await session.execute(stmt)
        children = result.scalars().all()
    # we know we've filtered appropriately, so we can ignore the type check
    return children  # type: ignore


@syncable
async def read_descendants(root_node: Node, node_type: type[N] = Node) -> Sequence[N]:
    """Read all descendants of this node."""
    node_id = root_node.node_id

    # Create a CTE to get the descendants recursively
    node_cte = (
        select(node_type.node_id.label("descendant_id"), node_type.node_parent_id)
        .where(node_type.node_id == node_id)
        .cte(name="descendants", recursive=True)
    )

    # Recursive case: select the children of the current nodes
    parent_node = aliased(node_type)
    node_cte = node_cte.union_all(
        select(parent_node.node_id, parent_node.node_parent_id).where(
            parent_node.node_parent_id == node_cte.c.descendant_id
        )
    )

    # Join the CTE with the actual Node table to get the descendants
    descendants_query = (
        select(node_type)
        .select_from(join(node_type, node_cte, node_type.node_id == node_cte.c.descendant_id))
        .where(node_cte.c.descendant_id != node_id)  # Exclude the root node itself
        .order_by(Node.node_id)
    )

    async with current_session() as session:
        result = await session.execute(descendants_query)
        descendants = result.scalars().all()

    return descendants
