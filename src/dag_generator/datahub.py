"""
DataHub adapter: query lineage and metadata via the acryl-datahub SDK.
Single responsibility — knows how to talk to DataHub, nothing else.
"""
from __future__ import annotations

from collections import deque
from typing import Any

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    EditableDatasetPropertiesClass,
    GlobalTagsClass,
    GlossaryTermsClass,
    TagAssociationClass,
    UpstreamLineageClass,
)

from dag_generator.models import DatasetNode


class DataHubClient:
    def __init__(self, server: str, token: str | None = None):
        self.server = server
        self.token = token
        self.graph = DataHubGraph(DatahubClientConfig(server=server, token=token))
        self.emitter = DatahubRestEmitter(server, token=token)

    # ── lineage traversal ───────────────────────────────────────────────────

    def get_lineage_graph(self, root_urn: str) -> dict[str, DatasetNode]:
        """
        BFS upstream traversal from root_urn.
        Returns {urn: DatasetNode} for root and all ancestors.
        """
        visited: dict[str, DatasetNode] = {}
        queue = deque([root_urn])

        while queue:
            urn = queue.popleft()
            if urn in visited:
                continue
            node = self._fetch_node(urn)
            visited[urn] = node
            for upstream_urn in node.upstream_urns:
                if upstream_urn not in visited:
                    queue.append(upstream_urn)

        return visited

    def _fetch_node(self, urn: str) -> DatasetNode:
        simple_name = _urn_to_table_name(urn)

        upstream_urns = []
        lineage = self._get_aspect(urn, UpstreamLineageClass)
        if lineage:
            upstream_urns = [u.dataset for u in lineage.upstreams]

        tags = []
        tags_aspect = self._get_aspect(urn, GlobalTagsClass)
        if tags_aspect:
            for assoc in tags_aspect.tags:
                # "urn:li:tag:daily_refresh" → "daily_refresh"
                tags.append(assoc.tag.split(":")[-1])

        glossary_terms = []
        terms_aspect = self._get_aspect(urn, GlossaryTermsClass)
        if terms_aspect:
            for assoc in terms_aspect.terms:
                # "urn:li:glossaryTerm:FreshnessSLA" → "FreshnessSLA"
                glossary_terms.append(assoc.urn.split(":")[-1])

        description = ""
        props = self._get_aspect(urn, DatasetPropertiesClass)
        if props and props.description:
            description = props.description

        return DatasetNode(
            urn=urn,
            simple_name=simple_name,
            upstream_urns=upstream_urns,
            tags=tags,
            glossary_terms=glossary_terms,
            description=description,
        )

    # ── discovery ───────────────────────────────────────────────────────────

    def find_urn(self, table_name: str, platform_instance: str) -> str | None:
        """
        Search DataHub for a dataset URN by table name + platform instance.
        Returns the first exact match.
        """
        query = """
        query FindDataset($query: String!) {
            search(input: {
                type: DATASET,
                query: $query,
                start: 0,
                count: 50
            }) {
                searchResults {
                    entity {
                        urn
                        ... on Dataset {
                            name
                            platform { name }
                        }
                    }
                }
            }
        }
        """

        result = self.graph.execute_graphql(query, variables={"query": table_name})
        for item in result.get("search", {}).get("searchResults", []):
            urn = item.get("entity", {}).get("urn", "")
            # Match exact instance AND exact table name (avoid prefix collisions)
            if f",{platform_instance}." in urn and f".{table_name}," in urn:
                return urn
        return None

    # ── write-back ──────────────────────────────────────────────────────────

    def write_dag_provenance(self, urn: str, dag_id: str) -> None:
        """
        Write back to DataHub: add 'dag_managed' tag + append DAG info to description.
        Uses EditableDatasetProperties so it doesn't overwrite ingestion-set properties.
        """
        dag_tag_urn = "urn:li:tag:dag_managed"

        # Merge new tag with existing tags to avoid overwriting
        existing_tags_aspect = self._get_aspect(urn, GlobalTagsClass)
        existing_tag_list = list(existing_tags_aspect.tags) if existing_tags_aspect else []
        if not any(t.tag == dag_tag_urn for t in existing_tag_list):
            existing_tag_list.append(TagAssociationClass(tag=dag_tag_urn))
        self.emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=GlobalTagsClass(tags=existing_tag_list),
        ))

        # Update editable description (separate from ingestion-set description)
        existing_editable = self._get_aspect(urn, EditableDatasetPropertiesClass)
        existing_desc = (existing_editable.description or "") if existing_editable else ""
        dag_note = f"[DAG: {dag_id}] Managed by DataHub DAG Generator Agent."
        if dag_note not in existing_desc:
            new_desc = f"{existing_desc}\n{dag_note}".strip() if existing_desc else dag_note
            self.emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=EditableDatasetPropertiesClass(description=new_desc),
            ))

    # ── helpers ─────────────────────────────────────────────────────────────

    def _get_aspect(self, urn: str, aspect_type: type) -> Any:
        return self.graph.get_aspect(urn, aspect_type)


def _urn_to_table_name(urn: str) -> str:
    """
    urn:li:dataset:(urn:li:dataPlatform:sqlite,nyc_taxi.raw_trips,PROD)
    → "raw_trips"
    """
    try:
        inner = urn.split("(", 1)[1].rstrip(")")
        full_name = inner.split(",")[1].strip()
        return full_name.split(".")[-1]
    except (IndexError, AttributeError):
        return urn
