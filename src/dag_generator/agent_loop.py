"""Bounded LLM agent loop for DataHub discovery and DAG planning."""
from __future__ import annotations

import asyncio
import json
from typing import Any

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from dag_generator.airflow import render_dag
from dag_generator.config import Settings, load_settings
from dag_generator.datahub import DataHubClient
from dag_generator.lineage import topological_sort
from dag_generator.llm import LLMProvider, create_llm_provider
from dag_generator.mcp import build_llm_tools, call_tool_async
from dag_generator.models import DatasetNode, GenerationResult, QualityCheck

CUSTOM_TOOLS: list[dict[str, Any]] = [
    {
        "name": "render_airflow_dag",
        "description": (
            "Render the pipeline after discovering every node. The CLI controls DAG ID, "
            "schedule, platform instance, and executable command templates."
        ),
        "input_schema": {
            "type": "object",
            "required": ["nodes", "quality_checks"],
            "properties": {
                "nodes": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": [
                            "urn",
                            "simple_name",
                            "upstream_urns",
                            "tags",
                            "glossary_terms",
                        ],
                        "properties": {
                            "urn": {"type": "string"},
                            "simple_name": {"type": "string"},
                            "upstream_urns": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "tags": {"type": "array", "items": {"type": "string"}},
                            "glossary_terms": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                        },
                    },
                },
                "quality_checks": {
                    "type": "array",
                    "description": (
                        "Only predefined checks are accepted; never provide shell commands."
                    ),
                    "items": {
                        "type": "object",
                        "required": ["kind", "after_urn"],
                        "properties": {
                            "kind": {
                                "type": "string",
                                "enum": ["row_count", "pii_audit"],
                            },
                            "after_urn": {"type": "string"},
                        },
                    },
                },
            },
        },
    },
    {
        "name": "datahub_write_back",
        "description": (
            "Write DAG provenance to the exact datasets in the rendered plan. "
            "This tool is disabled unless the CLI was started with --writeback."
        ),
        "input_schema": {
            "type": "object",
            "required": ["urns"],
            "properties": {
                "urns": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
]

SYSTEM_PROMPT = """\
You are the DataHub DAG Generator. Discover lineage and metadata through the
DataHub MCP tools, then create a structured DAG plan.

1. Search for the target dataset in the requested platform instance.
2. Traverse direct upstream lineage until every ancestor is discovered.
3. Fetch metadata for all discovered URNs in one get_entities call.
4. Call render_airflow_dag with every node and only these predefined checks:
   - row_count when a glossary term indicates Empty Load.
   - pii_audit when a dataset has a PII tag.
   Freshness checks are added by the renderer from freshness metadata.
5. Call datahub_write_back only when the user message explicitly enables it.

Never produce or request arbitrary shell commands. Use direct upstream URNs,
not all ancestors. The renderer validates and topologically sorts the plan.
"""


def _string_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{field} must be a list of strings")
    return value


def _metadata_name(value: str) -> str:
    return value.rsplit(":", 1)[-1] if value.startswith("urn:li:") else value


def _parse_plan(
    tool_input: dict[str, Any],
    max_nodes: int,
) -> tuple[list[DatasetNode], list[QualityCheck]]:
    raw_nodes = tool_input.get("nodes")
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise ValueError("nodes must be a non-empty list")
    if len(raw_nodes) > max_nodes:
        raise ValueError(f"Plan has {len(raw_nodes)} nodes; limit is {max_nodes}")

    nodes: dict[str, DatasetNode] = {}
    for raw_node in raw_nodes:
        if not isinstance(raw_node, dict):
            raise ValueError("Each node must be an object")
        urn = raw_node.get("urn")
        simple_name = raw_node.get("simple_name")
        if not isinstance(urn, str) or not urn:
            raise ValueError("Each node needs a non-empty urn")
        if not isinstance(simple_name, str) or not simple_name:
            raise ValueError(f"Node {urn} needs a non-empty simple_name")
        if urn in nodes:
            raise ValueError(f"Duplicate node URN: {urn}")
        nodes[urn] = DatasetNode(
            urn=urn,
            simple_name=simple_name,
            upstream_urns=_string_list(raw_node.get("upstream_urns"), "upstream_urns"),
            tags=[
                _metadata_name(tag)
                for tag in _string_list(raw_node.get("tags"), "tags")
            ],
            glossary_terms=[
                _metadata_name(term)
                for term in _string_list(
                    raw_node.get("glossary_terms"), "glossary_terms"
                )
            ],
        )

    known_urns = set(nodes)
    for node in nodes.values():
        unknown = set(node.upstream_urns) - known_urns
        if unknown:
            raise ValueError(
                f"Node {node.urn} references undiscovered upstream URNs: {sorted(unknown)}"
            )

    raw_checks = tool_input.get("quality_checks")
    if not isinstance(raw_checks, list):
        raise ValueError("quality_checks must be a list")
    checks: list[QualityCheck] = []
    for raw_check in raw_checks:
        if not isinstance(raw_check, dict):
            raise ValueError("Each quality check must be an object")
        kind = raw_check.get("kind")
        after_urn = raw_check.get("after_urn")
        if not isinstance(kind, str) or not isinstance(after_urn, str):
            raise ValueError("Each quality check needs string kind and after_urn")
        checks.append(QualityCheck(kind=kind, after_urn=after_urn))

    return topological_sort(nodes), checks


def _execute_custom_tool(
    tool_name: str,
    tool_input: dict[str, Any],
    client: DataHubClient,
    settings: Settings,
    dag_id: str,
    schedule: str,
    platform_instance: str,
    target_table: str,
    writeback: bool,
    dry_run: bool,
    rendered_urns: set[str] | None,
) -> tuple[str, str | None, set[str] | None, list[DatasetNode] | None, list[QualityCheck] | None]:
    if tool_name == "render_airflow_dag":
        sorted_nodes, checks = _parse_plan(tool_input, settings.max_lineage_nodes)
        dag_source = render_dag(
            sorted_nodes=sorted_nodes,
            dag_id=dag_id,
            schedule=schedule,
            target_table=target_table,
            platform_instance=platform_instance,
            quality_checks=checks,
            database_path=settings.dag_database_path_template.replace(
                "{instance}", platform_instance
            ),
        )
        plan_urns = {node.urn for node in sorted_nodes}
        result = {
            "status": "rendered",
            "nodes": [node.simple_name for node in sorted_nodes],
            "quality_checks_added": len(checks),
        }
        return json.dumps(result), dag_source, plan_urns, sorted_nodes, checks

    if tool_name == "datahub_write_back":
        if dry_run or not writeback:
            return (
                json.dumps({"status": "skipped", "reason": "write-back is disabled"}),
                None,
                rendered_urns,
                None,
                None,
            )
        if rendered_urns is None:
            raise ValueError("Render the DAG before requesting write-back")
        requested_urns = set(_string_list(tool_input.get("urns"), "urns"))
        if requested_urns != rendered_urns:
            raise ValueError("Write-back URNs must exactly match the rendered plan")
        for urn in sorted(requested_urns):
            client.write_dag_provenance(urn, dag_id)
        return (
            json.dumps({"status": "ok", "tagged_count": len(requested_urns)}),
            None,
            rendered_urns,
            None,
            None,
        )

    raise ValueError(f"Unknown custom tool: {tool_name}")


def _bounded_result(result: str, limit: int) -> str:
    if len(result) <= limit:
        return result
    return result[:limit] + f"\n[truncated: tool result exceeded {limit} characters]"


async def _agent_loop_async(
    target_table: str,
    platform_instance: str,
    dag_id: str,
    schedule: str,
    dry_run: bool,
    writeback: bool,
    verbose: bool,
    llm: LLMProvider,
    settings: Settings,
) -> GenerationResult:
    client = DataHubClient(settings.datahub_server, settings.datahub_token)
    mcp_environment = {"DATAHUB_GMS_URL": settings.datahub_server}
    if settings.datahub_token:
        mcp_environment["DATAHUB_GMS_TOKEN"] = settings.datahub_token
    mcp_params = StdioServerParameters(
        command="uvx",
        args=[settings.mcp_server_package],
        env=mcp_environment,
    )

    async with stdio_client(mcp_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            response = await session.list_tools()
            mcp_tools = build_llm_tools(response.tools)
            discovered_names = {tool.name for tool in response.tools}
            tools = mcp_tools + CUSTOM_TOOLS

            if verbose:
                print(f"[mcp]   Connected with {settings.mcp_server_package}")
                print(f"[mcp]   Tools: {', '.join(sorted(discovered_names))}\n")

            writeback_instruction = (
                "Write-back is enabled; call datahub_write_back after rendering."
                if writeback and not dry_run
                else "Write-back is disabled; do not call datahub_write_back."
            )
            messages: list[dict[str, Any]] = [
                {
                    "role": "user",
                    "content": (
                        f"Generate the pipeline ending at {target_table!r} in platform "
                        f"instance {platform_instance!r}. DAG ID: {dag_id!r}; "
                        f"schedule: {schedule!r}. {writeback_instruction}"
                    ),
                }
            ]
            rendered_dag: str | None = None
            rendered_urns: set[str] | None = None
            rendered_nodes: list[DatasetNode] | None = None
            rendered_checks: list[QualityCheck] | None = None

            for _turn in range(settings.max_agent_turns):
                llm_response = llm.complete(SYSTEM_PROMPT, tools, messages)
                if verbose and llm_response.text:
                    print(f"[llm]    {llm_response.text}")
                if not llm_response.tool_calls:
                    break

                messages.append(
                    {
                        "role": "assistant",
                        "content": llm_response.text,
                        "tool_calls": [
                            {"id": call.id, "name": call.name, "input": call.input}
                            for call in llm_response.tool_calls
                        ],
                    }
                )

                tool_results = []
                for call in llm_response.tool_calls:
                    if verbose:
                        print(f"[tool]   {call.name}")
                    try:
                        if call.name in discovered_names:
                            result = await call_tool_async(session, call.name, call.input)
                            result = _bounded_result(
                                result, settings.max_tool_result_chars
                            )
                        else:
                            result, maybe_dag, maybe_urns, maybe_nodes, maybe_checks = _execute_custom_tool(
                                tool_name=call.name,
                                tool_input=call.input,
                                client=client,
                                settings=settings,
                                dag_id=dag_id,
                                schedule=schedule,
                                platform_instance=platform_instance,
                                target_table=target_table,
                                writeback=writeback,
                                dry_run=dry_run,
                                rendered_urns=rendered_urns,
                            )
                            if maybe_dag is not None:
                                rendered_dag = maybe_dag
                            rendered_urns = maybe_urns
                            if maybe_nodes is not None:
                                rendered_nodes = maybe_nodes
                            if maybe_checks is not None:
                                rendered_checks = maybe_checks
                    except (KeyError, TypeError, ValueError) as exc:
                        result = json.dumps({"error": str(exc)})
                    tool_results.append(
                        {"tool_call_id": call.id, "content": result}
                    )
                messages.append({"role": "tool", "tool_results": tool_results})
            else:
                raise RuntimeError(
                    f"Agent exceeded MAX_AGENT_TURNS={settings.max_agent_turns}"
                )

    if rendered_dag is None or rendered_nodes is None:
        raise RuntimeError("Agent finished without rendering a valid DAG")
    return GenerationResult(
        dag_source=rendered_dag,
        sorted_nodes=rendered_nodes,
        quality_checks=rendered_checks or [],
        target_table=target_table,
    )


def run_dag_agent(
    target_table: str,
    platform_instance: str,
    dag_id: str = "",
    schedule: str = "@daily",
    dry_run: bool = False,
    writeback: bool = False,
    verbose: bool = True,
    provider: str = "openrouter",
    model: str | None = None,
    settings: Settings | None = None,
) -> GenerationResult:
    """Run the bounded DataHub DAG agent and return a GenerationResult."""
    settings = settings or load_settings()
    dag_id = dag_id or f"{platform_instance}_pipeline"

    if verbose:
        print("[agent]  DataHub DAG Generator")
        print(f"[agent]  Target : {target_table} @ {platform_instance}")
        print(f"[agent]  DAG ID : {dag_id}  |  Schedule: {schedule}")
        print(f"[agent]  Server : {settings.datahub_server}")
        print(f"[agent]  LLM    : {provider} / {model or 'configured default'}\n")

    llm = create_llm_provider(provider, settings, model)
    return asyncio.run(
        _agent_loop_async(
            target_table=target_table,
            platform_instance=platform_instance,
            dag_id=dag_id,
            schedule=schedule,
            dry_run=dry_run,
            writeback=writeback,
            verbose=verbose,
            llm=llm,
            settings=settings,
        )
    )
