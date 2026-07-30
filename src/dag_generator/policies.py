"""
Metadata-driven data-quality policies.

A dataset requires a freshness gate if DataHub says it's time-sensitive:
  - Tag:          daily_refresh / hourly_refresh / weekly_refresh
  - Glossary:     FreshnessSLA  (stored as "urn:li:glossaryTerm:FreshnessSLA")

When detected, the DAG renderer inserts a freshness_check_<table> task
after the main task and before the next downstream stage can start.
"""
from __future__ import annotations

from dag_generator.models import DatasetNode, QualityCheck

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


def recommended_quality_checks(nodes: list[DatasetNode]) -> list[QualityCheck]:
    """Return deterministic checks for metadata signals used by script mode."""
    checks: list[QualityCheck] = []
    for node in nodes:
        normalized_terms = {
            term.lower().replace(" ", "").replace("_", "")
            for term in node.glossary_terms
        }
        if "emptyload" in normalized_terms:
            checks.append(QualityCheck(kind="row_count", after_urn=node.urn))
        if any(tag.lower() == "pii" for tag in node.tags):
            checks.append(QualityCheck(kind="pii_audit", after_urn=node.urn))
    return checks
