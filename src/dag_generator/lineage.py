"""
Topologically sort the lineage graph (Kahn's algorithm).
Returns nodes in execution order: upstream tables first, downstream last.
"""
from __future__ import annotations

from collections import defaultdict
from heapq import heapify, heappop, heappush

from dag_generator.models import DatasetNode


def topological_sort(nodes: dict[str, DatasetNode]) -> list[DatasetNode]:
    """
    Kahn's algorithm on the upstream lineage graph.
    Returns nodes ordered for pipeline execution (sources first, sinks last).
    Raises ValueError if a cycle is detected (shouldn't happen in real lineage).
    """
    # in_degree[urn] = number of upstream predecessors in this subgraph
    in_degree: dict[str, int] = {}
    downstream: dict[str, list[str]] = defaultdict(list)
    for urn, node in nodes.items():
        known_upstreams = [upstream for upstream in node.upstream_urns if upstream in nodes]
        in_degree[urn] = len(known_upstreams)
        for upstream in known_upstreams:
            downstream[upstream].append(urn)

    # Start with source tables (no known upstreams)
    queue = [
        (nodes[urn].simple_name, urn)
        for urn, degree in in_degree.items()
        if degree == 0
    ]
    heapify(queue)

    sorted_nodes: list[DatasetNode] = []
    while queue:
        _, urn = heappop(queue)
        sorted_nodes.append(nodes[urn])

        for downstream_urn in downstream[urn]:
            in_degree[downstream_urn] -= 1
            if in_degree[downstream_urn] == 0:
                heappush(queue, (nodes[downstream_urn].simple_name, downstream_urn))

    if len(sorted_nodes) != len(nodes):
        raise ValueError(
            "Cycle detected in lineage graph — cannot produce a valid execution order."
        )

    return sorted_nodes
