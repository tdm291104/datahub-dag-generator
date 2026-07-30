"""DataHub MCP Server client utilities."""
from __future__ import annotations

import json
from mcp import ClientSession
from mcp.types import Tool as MCPTool

# Shorter descriptions for the LLM — preserves essential syntax, strips verbose examples
_SHORT_DESCRIPTIONS: dict[str, str] = {
    "search": (
        "Search DataHub entities by keyword using full-text search.\n"
        "Always prefix with /q for structured search.\n"
        "Syntax examples:\n"
        "  /q raw_trips              → find dataset named raw_trips\n"
        "  /q daily_refresh+trips    → AND: both terms required\n"
        "  /q tag:PII                → search by tag name\n"
        "Filter (SQL-like WHERE clause):\n"
        "  entity_type = dataset\n"
        "  platform = sqlite\n"
        "  tag = urn:li:tag:daily_refresh\n"
        "Params: query (str), filter (str, optional), num_results (int, max 50), offset (int)"
    ),
    "get_lineage": (
        "Get upstream or downstream lineage for a dataset URN.\n"
        "upstream=true → upstream ancestors; upstream=false → downstream consumers.\n"
        "max_hops=3 is equivalent to unlimited depth.\n"
        "Returns entity URNs, names, tags, and glossary terms for each node in the lineage.\n"
        "Params: urn (str), upstream (bool), max_hops (int, default 3), filter (str, optional)"
    ),
    "get_entities": (
        "Get full metadata for one or more entities by URN: tags, glossary terms, schema, description, owner.\n"
        "Pass an ARRAY of URNs to batch multiple lookups in a single call (much more efficient).\n"
        "Params: urns (array of str)"
    ),
    "get_dataset_queries": (
        "Get SQL queries run against a dataset to understand usage patterns.\n"
        "Params: urn (str), source ('MANUAL'|'SYSTEM'|null), column (str, optional), count (int)"
    ),
    "list_schema_fields": (
        "List schema fields for a dataset with optional keyword filtering.\n"
        "Params: urn (str), keywords (list of str, optional), limit (int), offset (int)"
    ),
    "get_lineage_paths_between": (
        "Get detailed lineage path(s) between two specific entities or columns.\n"
        "Params: source_urn (str), target_urn (str), source_column (str, optional), "
        "target_column (str, optional), direction ('upstream'|'downstream', optional)"
    ),
    "search_documents": (
        "Search runbooks and knowledge documents stored in DataHub (not DataHub's own docs).\n"
        "Params: query (str), semantic_query (str, optional), filter (str, optional), "
        "num_results (int), offset (int)"
    ),
    "grep_documents": (
        "Search within document content using regex patterns.\n"
        "Params: urns (list of str), pattern (str, RE2 regex), context_chars (int), "
        "max_matches_per_doc (int), start_offset (int)"
    ),
}

def build_llm_tools(mcp_tools: list[MCPTool]) -> list[dict]:
    """Convert MCP tools into the provider-neutral tool definition used by the agent."""
    result = []
    for tool in mcp_tools:
        description = _SHORT_DESCRIPTIONS.get(tool.name) or tool.description or tool.name
        result.append({
            "name": tool.name,
            "description": description,
            # MCP server returns empty schemas; the LLM infers params from description
            "input_schema": {"type": "object"},
        })
    return result


async def call_tool_async(session: ClientSession, name: str, arguments: dict) -> str:
    """Forward a tool call to the MCP server and return the result as a string."""
    result = await session.call_tool(name, arguments)
    parts: list[str] = []
    for block in result.content:
        if hasattr(block, "text"):
            parts.append(block.text)
    text = "\n".join(parts) if parts else "{}"
    # Return as-is (already JSON from mcp-server-datahub)
    return text
