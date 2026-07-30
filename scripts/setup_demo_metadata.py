#!/usr/bin/env python3
"""
Attach demo tags and glossary terms to nyc-taxi datasets.

The static-assets add_metadata.py emits one tag at a time, which causes each
emission to overwrite the previous (DataHub UPSERT replaces the whole aspect).
This script collects ALL tags/terms for each table first, then emits once per table.

Run this after: datahub ingest + add_lineage.py + add_metadata.py
"""
from __future__ import annotations

import argparse
import time

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.ingestion.graph.client import DataHubGraph, DatahubClientConfig
from datahub.metadata.schema_classes import (
    AuditStampClass,
    GlobalTagsClass,
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
    TagAssociationClass,
)

from dag_generator.config import load_settings

PLATFORM = "sqlite"

# All tags each table should have (collected per-table so we emit all at once)
TABLE_TAGS: dict[str, list[str]] = {
    "raw_trips":           ["daily_refresh", "time_series", "pii", "pipeline_stage"],
    "staging_trips":       ["daily_refresh", "time_series", "pii", "pipeline_stage"],
    "mart_daily_summary":  ["daily_refresh", "pipeline_stage"],
}

TABLE_GLOSSARY: dict[str, list[str]] = {
    "raw_trips":           ["freshness_sla", "pipeline_stage"],
    "staging_trips":       ["freshness_sla", "pipeline_stage"],
    "mart_daily_summary":  ["freshness_sla", "empty_load", "pipeline_stage"],
}


def discover_urns(graph: DataHubGraph, platform_instance: str) -> dict[str, str]:
    query = """
    query FindDemoDatasets($query: String!) {
        search(input: {type: DATASET, query: $query, start: 0, count: 100}) {
            searchResults {
                entity {
                    urn
                    ... on Dataset { name platform { name } }
                }
            }
        }
    }
    """
    result = graph.execute_graphql(query, variables={"query": platform_instance})
    urn_map = {}
    for item in result.get("search", {}).get("searchResults", []):
        entity = item.get("entity", {})
        if entity.get("platform", {}).get("name", "") != PLATFORM:
            continue
        if f",{platform_instance}." not in entity.get("urn", ""):
            continue
        name = entity.get("name", "")
        simple = name.split(".")[-1] if "." in name else name
        urn_map[simple] = entity["urn"]
    return urn_map


def attach_all_metadata(emitter: DatahubRestEmitter, urn_map: dict[str, str]) -> None:
    now_ms = int(time.time() * 1000)

    for table, tags in TABLE_TAGS.items():
        if table not in urn_map:
            print(f"  skip (not found): {table}")
            continue
        urn = urn_map[table]

        # Emit ALL tags in one call (avoids overwrite issue)
        emitter.emit(MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=GlobalTagsClass(
                tags=[TagAssociationClass(tag=f"urn:li:tag:{t}") for t in tags]
            ),
        ))
        print(f"  ✓ tags {tags} → {table}")

        # Emit ALL glossary terms in one call
        terms = TABLE_GLOSSARY.get(table, [])
        if terms:
            emitter.emit(MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=GlossaryTermsClass(
                    terms=[
                        GlossaryTermAssociationClass(urn=f"urn:li:glossaryTerm:{t}")
                        for t in terms
                    ],
                    auditStamp=AuditStampClass(
                        time=now_ms, actor="urn:li:corpuser:datahub"
                    ),
                ),
            ))
            print(f"  ✓ glossary {terms} → {table}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Attach metadata to nyc-taxi demo data")
    parser.add_argument(
        "--instance",
        action="append",
        help="Instance to update; repeat for multiple instances",
    )
    args = parser.parse_args()
    instances = args.instance or ["nyc_taxi", "nyc_taxi_pipeline"]
    settings = load_settings()

    print(f"Connecting to {settings.datahub_server}...")
    graph = DataHubGraph(
        DatahubClientConfig(
            server=settings.datahub_server,
            token=settings.datahub_token,
        )
    )
    emitter = DatahubRestEmitter(
        settings.datahub_server,
        token=settings.datahub_token,
    )

    for instance in instances:
        print(f"\nInstance: {instance}")
        urn_map = discover_urns(graph, instance)
        if not urn_map:
            print("  No datasets found — run datahub ingest first")
            continue
        attach_all_metadata(emitter, urn_map)

    print("\nDone. Tags and glossary terms are now correctly set.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
