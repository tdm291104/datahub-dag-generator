# DataHub DAG Generator

Generates a production-ready Airflow DAG from lineage and metadata stored in
DataHub. The agent reads the real lineage graph, tags, and glossary terms via
the **DataHub MCP Server**, then renders a structured DAG that your data team
can review and merge — no hand-editing required.

A sample generated DAG is committed at [`dags/nyc_taxi_pipeline.py`](dags/nyc_taxi_pipeline.py).

## How it works

```text
datahub-dag --target mart_daily_summary --instance nyc_taxi --pr
         │
         ▼
   DataHub MCP Server  (mcp-server-datahub via uvx)
   ├─ search           → find dataset URN
   ├─ get_lineage      → BFS upstream traversal
   └─ get_entities     → tags + glossary terms for all nodes
         │
         ▼
   LLM agent  (OpenRouter or Anthropic)
   ├─ maps tags/glossary → freshness gates + quality checks
   └─ calls render_airflow_dag with the full validated plan
         │
         ▼
   Deterministic renderer  (no LLM in the loop)
   ├─ topological sort from lineage
   ├─ ingest / transform / aggregate task verbs
   ├─ freshness_check_<table>  from daily_refresh / FreshnessSLA
   ├─ data_audit_<table>       from pii tag
   ├─ validate_row_count_<table> from EmptyLoad glossary term
   └─ dependency wiring from DataHub lineage edges
         │
         ├─► dags/<dag_id>.py          (saved locally)
         ├─► DataHub write-back        (--writeback, optional)
         └─► GitHub PR                 (--pr, optional)
```

The LLM cannot inject arbitrary shell commands. DAG ID, schedule, output
location, write-back, and all executable templates are under application
control.

## Project structure

```text
dags/
  nyc_taxi_pipeline.py   sample generated DAG (committed for reference)
src/dag_generator/
  cli.py          command-line entry point
  config.py       .env loading and runtime limits
  models.py       provider-independent data contracts
  datahub.py      DataHub SDK queries and write-back
  mcp.py          DataHub MCP bridge (mcp-server-datahub)
  llm.py          OpenRouter and Anthropic adapters
  agent_loop.py   bounded agent orchestration
  lineage.py      topological sorting (Kahn's algorithm)
  policies.py     metadata-driven quality rules
  airflow.py      deterministic Airflow 3 renderer
scripts/
  setup_demo_metadata.py
tests/
generate_dag.py   backward-compatible wrapper
docker-compose.airflow.yml
```

## Setup

```bash
make install
cp .env.example .env
# Add OPENROUTER_API_KEY to .env

# Docker must already be running
make start
```

See [SETUP.md](./SETUP.md) for the full DataHub quickstart and nyc-taxi demo
dataset. After ingesting the demo data:

```bash
make setup-demo-metadata
```

## Usage

```bash
# Generate DAG and open a GitHub PR — the full end-to-end workflow
datahub-dag --target mart_daily_summary --instance nyc_taxi --pr

# Preview the DAG without writing anything
datahub-dag --target mart_daily_summary --instance nyc_taxi --dry-run

# Save to dags/ locally (no PR)
datahub-dag --target mart_daily_summary --instance nyc_taxi

# Different base branch for the PR
datahub-dag --target mart_daily_summary --instance nyc_taxi --pr --pr-base develop

# Deterministic mode — no LLM, uses DataHub Python SDK directly
datahub-dag --mode script --target mart_daily_summary --instance nyc_taxi

# Override the model for one run (any OpenRouter model ID)
datahub-dag --model deepseek/deepseek-v4-flash \
  --target mart_daily_summary --instance nyc_taxi

# Use Anthropic directly instead of OpenRouter
datahub-dag --provider anthropic \
  --target mart_daily_summary --instance nyc_taxi

# Write DAG provenance back to DataHub after rendering
datahub-dag --writeback --target mart_daily_summary --instance nyc_taxi
```

`--dry-run` never writes a file or changes DataHub metadata.
`--writeback` is disabled by default; it adds a `dag_managed` tag and a note
to the editable description of each dataset in the rendered plan.

### PR workflow

`--pr` requires `git` and the [GitHub CLI (`gh`)](https://cli.github.com/)
on PATH and authenticated. It:

1. Creates branch `datahub-dag/<dag_id>`
2. Writes `dags/<dag_id>.py`
3. Commits and pushes the branch
4. Opens a PR against `--pr-base` (default `main`) with a description table
   showing which metadata signal drove which tasks

The PR description lists every stage, its upstream, and which tags or glossary
terms triggered which checks — so reviewers can verify the agent's reasoning
without reading the DAG source.

## Airflow UI

Generate a DAG first, then start the local Airflow 3 stack:

```bash
datahub-dag --target mart_daily_summary --instance nyc_taxi
make airflow-start
```

Open `http://localhost:8081`. Authentication is disabled for this
localhost-only development container. The `dags/` directory is mounted
read-only so regenerated DAGs appear automatically without restarting.

```bash
make airflow-status
make airflow-logs
make airflow-stop
```

Airflow state is stored in a Docker volume and survives `make airflow-stop`.
This standalone stack is for local demos, not production.

## Configuration

Default models live in `src/dag_generator/llm_models.yaml`. `--model`
overrides the configured default for one run. Set `LLM_MODEL_CONFIG` to point
at a different YAML file without modifying the package.

`.env` is loaded automatically; a variable already exported in the shell takes
priority over the file value.

| Variable | Default | Purpose |
|---|---|---|
| `DATAHUB_SERVER` | `http://localhost:8080` | DataHub GMS API |
| `DATAHUB_TOKEN` | empty | Remote/authenticated DataHub |
| `OPENROUTER_API_KEY` | empty | Default LLM provider |
| `ANTHROPIC_API_KEY` | empty | Direct Anthropic option (`--provider anthropic`) |
| `LLM_MODEL_CONFIG` | packaged YAML | Optional model-config path |
| `DATAHUB_MCP_PACKAGE` | `mcp-server-datahub@0.6.0` | Pinned MCP server package |
| `MAX_AGENT_TURNS` | `20` | Agent cost/runaway guard |
| `MAX_TOOL_RESULT_CHARS` | `200000` | Per-tool context limit |
| `MAX_LINEAGE_NODES` | `200` | Maximum rendered lineage size |
| `DAG_DATABASE_PATH_TEMPLATE` | `/opt/airflow/demo-data/{instance}.db` | SQLite path inside Airflow container |

## Verification

```bash
make test   # 27 offline unit checks — no DataHub or LLM required
make check  # tests + Python syntax check across the whole package
```
