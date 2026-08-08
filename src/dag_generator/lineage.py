"""
lineage.py — order and simplify the upstream lineage graph before rendering.

exports: topological_sort(nodes) -> list[DatasetNode]
         transitive_reduction(nodes) -> None
used_by: cli.py → _run_script | agent_loop.py → _parse_plan
rules:   transitive_reduction MUTATES node.upstream_urns in place — call it
         before topological_sort, never after the renderer has read the edges.
         Upstream URNs outside `nodes` are left untouched by both functions;
         they carry no path information about this subgraph.
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


def transitive_reduction(nodes: dict[str, DatasetNode]) -> None:
    """
    Drop upstream edges already implied by a longer path through the graph.

    A → B → C plus a declared A → C means the A → C edge adds nothing: C
    already waits for A via B. Keeping it clutters the DAG and misrepresents
    the pipeline. This removes those edges in place.

    Rules:   MUTATES node.upstream_urns. Does not raise on cycles — a cycle
             yields incomplete ancestor sets here and is caught immediately
             after by topological_sort.
    """
    ancestors: dict[str, set[str]] = {}

    def collect(urn: str) -> set[str]:
        cached = ancestors.get(urn)
        if cached is not None:
            return cached
        ancestors[urn] = set()  # cycle guard: an in-progress node claims no ancestors
        result: set[str] = set()
        for parent in nodes[urn].upstream_urns:
            if parent in nodes:
                result.add(parent)
                result |= collect(parent)
        ancestors[urn] = result
        return result

    for urn in nodes:
        collect(urn)

    for node in nodes.values():
        known = [urn for urn in node.upstream_urns if urn in nodes]
        redundant = {
            urn
            for urn in known
            if any(other != urn and urn in ancestors[other] for other in known)
        }
        if redundant:
            node.upstream_urns = [
                urn for urn in node.upstream_urns if urn not in redundant
            ]
