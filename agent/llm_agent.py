"""
Claude-powered DAG Generator Agent using DataHub MCP Server.

Architecture:
  1. Starts mcp-server-datahub as a stdio subprocess (via uvx)
  2. Claude discovers lineage + metadata through native DataHub MCP tools
  3. Claude reasons about metadata signals to decide what tasks to add
  4. Claude calls render_airflow_dag with the full pipeline specification
  5. Claude calls datahub_write_back to tag datasets with provenance

MCP tools used (read-only, served by mcp-server-datahub):
  - search          → find dataset URN by table name
  - get_lineage     → traverse upstream pipeline
  - get_entities    → read tags, glossary terms, description

Custom tools (not in MCP — mutations disabled on OSS DataHub):
  - render_airflow_dag   → generate valid Airflow DAG Python file
  - datahub_write_back   → tag datasets with dag_managed via Python SDK
"""
from __future__ import annotations

import asyncio
import json
import os

import anthropic
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from agent.datahub_client import DataHubClient, DatasetNode
from agent.datahub_mcp import MCP_TOOL_NAMES, build_claude_tools, call_tool_async
from agent.dag_renderer import render_dag, _stage_verb
from agent.lineage_graph import topological_sort


# ── Custom tool definitions ─────────────────────────────────────────────────

CUSTOM_TOOLS: list[dict] = [
    {
        "name": "render_airflow_dag",
        "description": (
            "Generate a valid Airflow DAG Python file from the pipeline specification.\n"
            "Call this after you have discovered all nodes and decided on extra tasks.\n"
            "Pass nodes in execution order (upstream first, downstream last).\n"
            "Include extra_tasks for any metadata-driven decisions (e.g. validate_row_count "
            "when 'empty_load' glossary is detected, data_audit when 'pii' tag is present)."
        ),
        "input_schema": {
            "type": "object",
            "required": ["dag_id", "platform_instance", "nodes"],
            "properties": {
                "dag_id": {"type": "string"},
                "schedule": {"type": "string", "default": "@daily"},
                "platform_instance": {"type": "string"},
                "nodes": {
                    "type": "array",
                    "description": "Pipeline nodes in execution order (upstream first).",
                    "items": {
                        "type": "object",
                        "required": ["urn", "simple_name", "upstream_urns", "tags", "glossary_terms"],
                        "properties": {
                            "urn": {"type": "string"},
                            "simple_name": {"type": "string"},
                            "upstream_urns": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "URNs of upstream tables this node depends on.",
                            },
                            "tags": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Tag names only, e.g. ['daily_refresh', 'pii'].",
                            },
                            "glossary_terms": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Glossary term names, e.g. ['FreshnessSLA', 'empty_load'].",
                            },
                        },
                    },
                },
                "extra_tasks": {
                    "type": "array",
                    "description": "Additional tasks based on your metadata reasoning.",
                    "items": {
                        "type": "object",
                        "required": ["task_id", "after_urn", "bash_command"],
                        "properties": {
                            "task_id": {"type": "string"},
                            "after_urn": {
                                "type": "string",
                                "description": "URN of the node after which this task runs.",
                            },
                            "bash_command": {"type": "string"},
                            "doc": {"type": "string"},
                        },
                    },
                },
            },
        },
    },
    {
        "name": "datahub_write_back",
        "description": (
            "Write provenance back to DataHub after the DAG is generated.\n"
            "Adds a 'dag_managed' tag and updates the editable description with the DAG ID "
            "on every dataset in the pipeline."
        ),
        "input_schema": {
            "type": "object",
            "required": ["urns", "dag_id"],
            "properties": {
                "urns": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "URNs of all datasets in the pipeline.",
                },
                "dag_id": {
                    "type": "string",
                    "description": "The generated DAG ID to record.",
                },
            },
        },
    },
]

SYSTEM_PROMPT = """\
You are the DataHub DAG Generator Agent. Use the DataHub MCP tools to discover
lineage and metadata, then generate a production-ready Airflow DAG.

## Step-by-Step Workflow

### Step 1 — Find the target URN
Call `search` with:
  query="/q <table_name>", filter="entity_type = dataset", num_results=10
Extract the URN that matches the platform instance (e.g. contains "nyc_taxi").

### Step 2 — Traverse the pipeline graph (BFS, max_hops=1 per node)
Use `get_lineage` with upstream=true, max_hops=1 for each node to find its DIRECT upstreams.
Start from the target node, then repeat for each newly discovered upstream until no more.

For each `get_lineage` response, extract direct upstream URNs from:
  response["upstreams"]["searchResults"][i]["entity"]["urn"]

Collect ALL node URNs discovered (target + all ancestors).

### Step 3 — Read full metadata for all nodes in one call
Call `get_entities` with the full array of URNs collected in Step 2.

For each entity in the response, extract:
  - Tags:    entity["tags"]["tags"][i]["tag"]["properties"]["name"]
  - Glossary: entity["glossaryTerms"]["terms"][i]["term"]["properties"]["name"]

### Step 4 — Reason about data quality tasks
Based on the metadata, decide which EXTRA tasks to add beyond the standard pipeline tasks:

**Freshness gate** (freshness_check_<table>):
  → Triggered by: tag "daily_refresh" / "hourly_refresh" / "weekly_refresh"
    OR glossary term containing "Freshness" or "freshness"
  → Handled automatically by render_airflow_dag. Do NOT add to extra_tasks.

**Row count validation** (validate_row_count_<table>):
  → Triggered by: glossary term "Empty Load" or "empty_load"
  → ADD to extra_tasks — this is a silent data quality failure:
    the pipeline succeeds but downstream tables get zero rows.
  → bash_command: python -c "import sqlite3,sys; conn=sqlite3.connect('/path/to/db');
    n=conn.execute('SELECT COUNT(*) FROM <table>').fetchone()[0]; conn.close();
    print(f'Row count: {n}'); sys.exit(0 if n > 0 else 1)"

**PII audit** (data_audit_<table>):
  → Triggered by: tag "pii"
  → ADD to extra_tasks if the table has PII data
  → bash_command: echo "Audit log: PII data access for <table> at $(date)"

### Step 5 — Render the DAG
Call `render_airflow_dag` with:
  - nodes: ALL pipeline nodes in execution order (upstream first, target last)
    Each node must include:
      urn, simple_name, upstream_urns (direct upstreams only), tags, glossary_terms
  - extra_tasks: only the tasks from Step 4 marked "ADD to extra_tasks"
  - dag_id, schedule, platform_instance

### Step 6 — Write provenance back to DataHub
Call `datahub_write_back` with:
  - urns: ALL pipeline node URNs
  - dag_id: the DAG ID used

## Response Format Rules
- Always explain your reasoning for each extra task before calling render_airflow_dag.
- Use upstream_urns correctly: each node's direct parents (from max_hops=1 lineage), not all ancestors.
- Extract the simple table name from the URN: the part after the last "." before the comma.
  Example: "urn:li:dataset:(urn:li:dataPlatform:sqlite,nyc_taxi.main.raw_trips,PROD)" → simple_name = "raw_trips"
"""


# ── Custom tool execution ───────────────────────────────────────────────────

def _execute_custom_tool(
    tool_name: str,
    tool_input: dict,
    datahub_client: DataHubClient,
    dag_id: str,
    schedule: str,
    platform_instance: str,
    dry_run: bool,
    no_writeback: bool,
) -> tuple[str, str | None]:
    """Execute a custom (non-MCP) tool. Returns (result_json, rendered_dag_or_None)."""
    rendered_dag: str | None = None

    if tool_name == "render_airflow_dag":
        nodes_data: list[dict] = tool_input["nodes"]
        render_dag_id: str = tool_input.get("dag_id", dag_id)
        render_schedule: str = tool_input.get("schedule", schedule)
        render_platform: str = tool_input.get("platform_instance", platform_instance)
        extra_tasks: list[dict] = tool_input.get("extra_tasks", [])

        # Convert plain dicts → DatasetNode objects
        nodes_dict: dict[str, DatasetNode] = {}
        for nd in nodes_data:
            node = DatasetNode(
                urn=nd["urn"],
                simple_name=nd["simple_name"],
                upstream_urns=nd.get("upstream_urns", []),
                tags=nd.get("tags", []),
                glossary_terms=nd.get("glossary_terms", []),
            )
            nodes_dict[node.urn] = node

        # Topological sort ensures correct execution order
        sorted_nodes = topological_sort(nodes_dict)

        dag_content = render_dag(
            sorted_nodes=sorted_nodes,
            dag_id=render_dag_id,
            schedule=render_schedule,
            target_table=sorted_nodes[-1].simple_name if sorted_nodes else "",
            platform_instance=render_platform,
        )

        # Append Claude-reasoned extra tasks
        if extra_tasks:
            urn_to_index = {n.urn: i for i, n in enumerate(sorted_nodes)}
            urn_to_name = {n.urn: n.simple_name for n in sorted_nodes}

            extra_lines = [
                "",
                "    # ── Claude-reasoned extra tasks ─────────────────────────────────",
                "    # Added based on DataHub metadata analysis",
            ]
            for task in extra_tasks:
                tid = task["task_id"]
                cmd = task["bash_command"].replace('"', '\\"')
                doc = task.get("doc", "Added by Claude based on DataHub metadata.")

                extra_lines += [
                    f"    {tid} = BashOperator(",
                    f'        task_id="{tid}",',
                    f'        bash_command="{cmd}",',
                    f'        doc_md="{doc}",',
                    "    )",
                ]

                after_urn = task.get("after_urn", "")
                if after_urn and after_urn in urn_to_index:
                    idx = urn_to_index[after_urn]
                    verb = _stage_verb(idx, len(sorted_nodes))
                    after_name = urn_to_name[after_urn]
                    extra_lines.append(f"    {verb}_{after_name} >> {tid}")

            dag_content = dag_content.rstrip("\n") + "\n" + "\n".join(extra_lines) + "\n"

        rendered_dag = dag_content
        result = {
            "status": "rendered",
            "dag_id": render_dag_id,
            "line_count": dag_content.count("\n"),
            "nodes": [n.simple_name for n in sorted_nodes],
            "extra_tasks_added": len(extra_tasks),
        }

    elif tool_name == "datahub_write_back":
        if dry_run or no_writeback:
            result = {"status": "skipped", "reason": "dry_run or no_writeback flag is set"}
        else:
            for urn in tool_input["urns"]:
                datahub_client.write_dag_provenance(urn, tool_input["dag_id"])
            result = {"status": "ok", "tagged_count": len(tool_input["urns"])}

    else:
        result = {"error": f"Unknown custom tool: {tool_name}"}

    return json.dumps(result), rendered_dag


# ── Agent loop (async — keeps MCP session open across all tool calls) ────────

async def _agent_loop_async(
    target_table: str,
    platform_instance: str,
    server: str,
    dag_id: str,
    schedule: str,
    dry_run: bool,
    no_writeback: bool,
    verbose: bool,
) -> str:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable is not set.")

    claude = anthropic.Anthropic(api_key=api_key)
    datahub_client = DataHubClient(server=server)

    mcp_params = StdioServerParameters(
        command="uvx",
        args=["mcp-server-datahub@latest"],
        env={
            "DATAHUB_GMS_URL": server,
            "DATAHUB_GMS_TOKEN": "",
        },
    )

    async with stdio_client(mcp_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Discover MCP tools and merge with custom tools
            mcp_tools_response = await session.list_tools()
            mcp_tool_defs = build_claude_tools(mcp_tools_response.tools)
            all_tools = mcp_tool_defs + CUSTOM_TOOLS
            discovered_mcp_names = {t.name for t in mcp_tools_response.tools}

            if verbose:
                print(f"[mcp]   Connected to mcp-server-datahub")
                print(f"[mcp]   MCP tools: {', '.join(sorted(discovered_mcp_names))}")
                print(f"[mcp]   Custom tools: render_airflow_dag, datahub_write_back")
                print()

            messages: list[dict] = [
                {
                    "role": "user",
                    "content": (
                        f"Generate an Airflow DAG for the data pipeline ending at table "
                        f"'{target_table}' in DataHub platform instance '{platform_instance}'. "
                        f"Use DAG ID '{dag_id}' and schedule '{schedule}'. "
                        + (
                            "Skip the write-back step (do NOT call datahub_write_back)."
                            if dry_run or no_writeback
                            else "After rendering the DAG, write provenance back to DataHub."
                        )
                    ),
                }
            ]

            rendered_dag: str | None = None

            # Manual agentic loop
            while True:
                response = claude.messages.create(
                    model="claude-opus-4-8",
                    max_tokens=8192,
                    thinking={"type": "adaptive"},
                    system=SYSTEM_PROMPT,
                    tools=all_tools,
                    messages=messages,
                )

                tool_uses = [b for b in response.content if b.type == "tool_use"]
                text_blocks = [b for b in response.content if b.type == "text"]

                if verbose and text_blocks:
                    for tb in text_blocks:
                        print(f"[claude] {tb.text}")

                if response.stop_reason == "end_turn" or not tool_uses:
                    break

                messages.append({"role": "assistant", "content": response.content})

                tool_results = []
                for tool_use in tool_uses:
                    if verbose:
                        args_preview = json.dumps(tool_use.input)
                        if len(args_preview) > 150:
                            args_preview = args_preview[:147] + "..."
                        print(f"[tool]   {tool_use.name}({args_preview})")

                    if tool_use.name in discovered_mcp_names:
                        # Forward to DataHub MCP server
                        result_text = await call_tool_async(session, tool_use.name, tool_use.input)
                        maybe_dag = None
                    else:
                        # Custom tool
                        result_text, maybe_dag = _execute_custom_tool(
                            tool_name=tool_use.name,
                            tool_input=tool_use.input,
                            datahub_client=datahub_client,
                            dag_id=dag_id,
                            schedule=schedule,
                            platform_instance=platform_instance,
                            dry_run=dry_run,
                            no_writeback=no_writeback,
                        )

                    if maybe_dag is not None:
                        rendered_dag = maybe_dag

                    if verbose:
                        preview = result_text[:200] + ("..." if len(result_text) > 200 else "")
                        print(f"[result] {preview}")
                        print()

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_use.id,
                        "content": result_text,
                    })

                messages.append({"role": "user", "content": tool_results})

    if rendered_dag is None:
        raise RuntimeError(
            "Agent finished without rendering a DAG. "
            "Check the output above for errors."
        )

    return rendered_dag


# ── Public entry point ───────────────────────────────────────────────────────

def run_dag_agent(
    target_table: str,
    platform_instance: str,
    server: str = "http://localhost:8080",
    dag_id: str = "",
    schedule: str = "@daily",
    dry_run: bool = False,
    no_writeback: bool = False,
    verbose: bool = True,
) -> str:
    """
    Run the Claude DAG Generator Agent. Returns the rendered DAG Python source.

    Requires:
      - ANTHROPIC_API_KEY env var
      - DataHub running at `server`
      - uvx available on PATH (ships with uv)
    """
    if not dag_id:
        dag_id = f"{platform_instance}_pipeline"

    if verbose:
        print(f"[agent]  DataHub DAG Generator Agent")
        print(f"[agent]  Target : {target_table} @ {platform_instance}")
        print(f"[agent]  DAG ID : {dag_id}  |  Schedule: {schedule}")
        print(f"[agent]  Server : {server}")
        print()

    return asyncio.run(
        _agent_loop_async(
            target_table=target_table,
            platform_instance=platform_instance,
            server=server,
            dag_id=dag_id,
            schedule=schedule,
            dry_run=dry_run,
            no_writeback=no_writeback,
            verbose=verbose,
        )
    )
