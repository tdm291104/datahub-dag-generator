"""Provider-independent data used by lineage, policies, and renderers."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class DatasetNode:
    urn: str
    simple_name: str
    upstream_urns: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    glossary_terms: list[str] = field(default_factory=list)
    description: str = ""
    owner: str = ""


@dataclass(frozen=True)
class QualityCheck:
    kind: str
    after_urn: str
