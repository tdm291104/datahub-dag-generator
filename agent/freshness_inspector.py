"""
Detect freshness monitoring requirements from DataHub metadata.

A dataset requires a freshness gate if DataHub says it's time-sensitive:
  - Tag:          daily_refresh / hourly_refresh / weekly_refresh
  - Glossary:     FreshnessSLA  (stored as "urn:li:glossaryTerm:FreshnessSLA")

When detected, the DAG renderer inserts a freshness_check_<table> task
after the main task and before the next downstream stage can start.
"""
from __future__ import annotations

from agent.datahub_client import DatasetNode

_FRESHNESS_TAGS = {"daily_refresh", "hourly_refresh", "weekly_refresh"}

# DataHub stores glossary term URNs; we match on the final component
_FRESHNESS_TERM_SUFFIXES = {"FreshnessSLA", "Freshness_SLA", "freshness_sla"}


def requires_freshness_check(node: DatasetNode) -> bool:
    has_tag = bool(set(node.tags) & _FRESHNESS_TAGS)
    has_term = any(
        t in _FRESHNESS_TERM_SUFFIXES or "freshness" in t.lower()
        for t in node.glossary_terms
    )
    return has_tag or has_term


def freshness_reason(node: DatasetNode) -> str:
    """Human-readable explanation of why a freshness check was added."""
    reasons = []
    for tag in node.tags:
        if tag in _FRESHNESS_TAGS:
            reasons.append(f"tag: {tag}")
    for term in node.glossary_terms:
        if term in _FRESHNESS_TERM_SUFFIXES or "freshness" in term.lower():
            reasons.append(f"glossary: {term}")
    return ", ".join(reasons)
