from typing import Sequence, TypeVar

from sqlalchemy import and_, literal, or_, select, union_all
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
async def create_parent_child_relationships(
    parent_child_pairs: Sequence[tuple[Node, Node]]
) -> None:
    """Create parent-to-child links between nodes."""
    async with current_session() as session:
        for parent, child in parent_child_pairs:
            child.parent_id = parent.id
            session.add(child)
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
    node_types: Sequence[type[N] | str] = (),
) -> Sequence[N]:
    """Read the direct children of a node."""
    stmt = select(Node).where(Node.parent_id == root_node.id)
    node_type_names = [
        nt if isinstance(nt, str) else nt.__mapper_args__["polymorphic_identity"]
        for nt in node_types
    ]
    if node_type_names:
        stmt = stmt.where(Node.type.in_(node_type_names))
    async with current_session() as session:
        result = await session.execute(stmt)
        children = result.scalars().all()

    # we know we've filtered appropriately, so we can ignore the type check
    return children  # type: ignore


@syncable
async def read_recursive_children(
    root_node: Node,
    node_types: Sequence[type[Node]] = (),
) -> Sequence[Node]:
    """Use a recursive query to read the children of a node."""
    # Convert node_types to their respective polymorphic_identity strings
    node_type_names = [
        nt if isinstance(nt, str) else nt.__mapper_args__["polymorphic_identity"]
        for nt in node_types
    ]

    # Define the CTE for recursive traversal
    node_cte = aliased(Node)
    recursive_cte = select(Node).where(Node.parent_id == node_cte.id).cte(name="recursive_cte")

    # Union the root node and its recursive children
    recursive_query = union_all(
        select(Node).where(Node.id == root_node.id),
        select(Node)
        .select_from(Node, recursive_cte)
        .where(
            and_(
                Node.parent_id == recursive_cte.c.id,
                or_(Node.type.in_(node_type_names), literal(False)),
            )
        ),
    )

    async with current_session() as session:
        # Execute the recursive query
        result = await session.execute(recursive_query)

        # Return the list of children nodes
        return result.scalars().all()
