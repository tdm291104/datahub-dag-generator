"""
Build and topologically sort the lineage graph (Kahn's algorithm).
Returns nodes in execution order: upstream tables first, downstream last.
"""
from __future__ import annotations

from agent.datahub_client import DatasetNode


def topological_sort(nodes: dict[str, DatasetNode]) -> list[DatasetNode]:
    """
    Kahn's algorithm on the upstream lineage graph.
    Returns nodes ordered for pipeline execution (sources first, sinks last).
    Raises ValueError if a cycle is detected (shouldn't happen in real lineage).
    """
    # in_degree[urn] = number of upstream predecessors in this subgraph
    in_degree: dict[str, int] = {}
    for urn, node in nodes.items():
        known_upstream_count = sum(1 for u in node.upstream_urns if u in nodes)
        in_degree[urn] = known_upstream_count

    # Start with source tables (no known upstreams)
    queue = sorted(
        [urn for urn, deg in in_degree.items() if deg == 0],
        key=lambda u: nodes[u].simple_name,
    )

    sorted_nodes: list[DatasetNode] = []
    while queue:
        urn = queue.pop(0)
        sorted_nodes.append(nodes[urn])

        # Decrement in_degree for nodes that depend on this one
        for candidate_urn, candidate_node in nodes.items():
            if urn in candidate_node.upstream_urns:
                in_degree[candidate_urn] -= 1
                if in_degree[candidate_urn] == 0:
                    queue.append(candidate_urn)
                    queue.sort(key=lambda u: nodes[u].simple_name)

    if len(sorted_nodes) != len(nodes):
        raise ValueError(
            "Cycle detected in lineage graph — cannot produce a valid execution order."
        )

    return sorted_nodes
