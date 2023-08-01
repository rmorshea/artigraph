from typing import Optional, Sequence, TypeVar

from sqlalchemy import join, select
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
        node = result.scalar()
    return node


@syncable
async def create_metadata(node: Node, metadata: dict[str, str]) -> None:
    """Create metadata for a node."""
    metadata = [
        NodeMetadata(
            node_id=node.node_id,
            key=key,
            value=value,
        )
        for key, value in metadata.items()
    ]
    async with current_session() as session:
        session.add_all(metadata)
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
    node_types: Sequence[type[N] | str] = (),
) -> Sequence[N]:
    """Read the direct children of a node."""
    stmt = select(Node).where(Node.node_parent_id == root_node.node_id)
    node_type_names = [
        nt if isinstance(nt, str) else nt.__mapper_args__["polymorphic_identity"]
        for nt in node_types
    ]
    if node_type_names:
        stmt = stmt.where(Node.node_type.in_(_get_node_type_names(node_types)))
    async with current_session() as session:
        result = await session.execute(stmt)
        children = result.scalars().all()

    # we know we've filtered appropriately, so we can ignore the type check
    return children  # type: ignore


@syncable
async def read_descendants(
    root_node: Node,
    node_types: Sequence[type[Node] | str] = (),
    limit: Optional[int] = None,
) -> Sequence[Node]:
    """Read all descendants of this node."""
    node_id = root_node.node_id

    # Create a CTE to get the descendants recursively
    node_cte = (
        select(Node.node_id.label("descendant_id"), Node.node_parent_id)
        .where(Node.node_id == node_id)
        .cte(name="descendants", recursive=True)
    )

    # Recursive case: select the children of the current nodes
    parent_node = aliased(Node)
    node_cte = node_cte.union_all(
        select(parent_node.node_id, parent_node.node_parent_id).where(
            parent_node.node_parent_id == node_cte.c.descendant_id
        )
    )

    # Join the CTE with the actual Node table to get the descendants
    descendants_query = (
        select(Node)
        .select_from(join(Node, node_cte, Node.node_id == node_cte.c.descendant_id))
        .where(node_cte.c.descendant_id != node_id)  # Exclude the root node itself
        .order_by(Node.node_id)
    )

    if node_types:
        descendants_query = descendants_query.where(
            Node.node_type.in_(_get_node_type_names(node_types))
        )

    if limit is not None:
        descendants_query = descendants_query.limit(limit)

    async with current_session() as session:
        result = await session.execute(descendants_query)
        descendants = result.scalars().all()

    return descendants


def _get_node_type_names(node_types: Sequence[type[Node] | str]) -> list[str]:
    """Get the node types as strings."""
    return [
        nt if isinstance(nt, str) else nt.__mapper_args__["polymorphic_identity"]
        for nt in node_types
    ]
