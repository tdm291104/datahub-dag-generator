# DataHub DAG Generator

Generates an Airflow DAG skeleton from lineage and metadata stored in DataHub.
Agent mode uses DataHub MCP plus an LLM; script mode runs the same core renderer
without an LLM.

The generated processing tasks are placeholders. Replace their `printf`
commands with real SQL, dbt, Spark, or Python ETL commands before production.
The local Airflow stack mounts the nyc-taxi SQLite files for demo checks.

## Flow

```text
CLI
 ├─ agent mode  → OpenRouter/Anthropic → DataHub MCP
 └─ script mode → DataHub Python SDK
                        │
                        ▼
              validated generation plan
                        │
                        ▼
              deterministic DAG renderer
                        │
                        ├─ output/<dag_id>.py
                        └─ optional DataHub write-back
```

The renderer derives freshness gates from `daily_refresh`, `hourly_refresh`,
`weekly_refresh`, or freshness glossary terms. It only accepts predefined
LLM-planned checks:

- `row_count` for Empty Load metadata
- `pii_audit` for PII metadata

The LLM cannot provide arbitrary shell commands. DAG ID, schedule, output
location, write-back, and executable templates stay under application control.

## Project structure

```text
src/dag_generator/
  cli.py          command-line interface
  config.py       .env and runtime limits
  models.py       provider-independent data contracts
  datahub.py      DataHub SDK queries and write-back
  mcp.py          DataHub MCP bridge
  llm.py          OpenRouter and Anthropic adapters
  agent_loop.py   bounded agent orchestration
  lineage.py      topological sorting
  policies.py     metadata-driven quality rules
  airflow.py      deterministic Airflow renderer
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

See [SETUP.md](./SETUP.md) for DataHub quickstart and the nyc-taxi demo dataset.
After ingesting the demo data:

```bash
make setup-demo-metadata
```

## Usage

```bash
# Agent mode: OpenRouter + configured default model
datahub-dag --target mart_daily_summary --instance nyc_taxi

# Preview only
datahub-dag --target mart_daily_summary --instance nyc_taxi --dry-run

# Deterministic mode without an LLM
datahub-dag --mode script --target mart_daily_summary --instance nyc_taxi

# One-run model override through OpenRouter
datahub-dag --model deepseek/deepseek-v4-flash \
  --target mart_daily_summary --instance nyc_taxi

# Direct Anthropic provider
datahub-dag --provider anthropic \
  --target mart_daily_summary --instance nyc_taxi

# Explicitly allow provenance updates in DataHub
datahub-dag --writeback \
  --target mart_daily_summary --instance nyc_taxi
```

Write-back is disabled by default. `--dry-run` never creates a file or changes
DataHub metadata, even when `--writeback` is also present.

The previous invocation remains available after `make install`:

```bash
python generate_dag.py --target mart_daily_summary --instance nyc_taxi
```

## Airflow UI

Generate a DAG first, then start the optional local Airflow 3 stack:

```bash
datahub-dag --target mart_daily_summary --instance nyc_taxi
make airflow-start
```

Open `http://localhost:8081`. Authentication is disabled for this localhost-only
development container. Generated files under `output/` are mounted read-only as
Airflow's DAG folder, so regenerated DAGs appear automatically.

```bash
make airflow-status
make airflow-logs
make airflow-stop
```

Airflow state is stored in a Docker volume and survives `make airflow-stop`.
This standalone setup is for local demos, not production.

## Configuration

Default models are editable in `src/dag_generator/llm_models.yaml`.
Command-line `--model` overrides the configured model for one run. Set
`LLM_MODEL_CONFIG` to load another YAML file without modifying the package.

`.env` is loaded automatically; an already-exported shell variable has priority.

| Variable | Default | Purpose |
|---|---|---|
| `DATAHUB_SERVER` | `http://localhost:8080` | DataHub GMS API |
| `DATAHUB_TOKEN` | empty | Remote/authenticated DataHub |
| `OPENROUTER_API_KEY` | empty | Default LLM provider |
| `ANTHROPIC_API_KEY` | empty | Direct Anthropic option |
| `LLM_MODEL_CONFIG` | packaged YAML | Optional model-config path |
| `DATAHUB_MCP_PACKAGE` | `mcp-server-datahub@0.6.0` | Pinned MCP server |
| `MAX_AGENT_TURNS` | `20` | Agent cost/runaway limit |
| `MAX_TOOL_RESULT_CHARS` | `200000` | Per-tool context limit |
| `MAX_LINEAGE_NODES` | `200` | Maximum rendered lineage size |
| `DAG_DATABASE_PATH_TEMPLATE` | `/opt/airflow/demo-data/{instance}.db` | Database path visible inside Airflow |

## Verification

```bash
make test   # offline unit checks
make check  # tests plus Python syntax checks
```
