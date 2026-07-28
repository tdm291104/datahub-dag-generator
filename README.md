# DataHub DAG Generator Agent

Agent that generates Airflow DAGs based on real lineage from DataHub — using the DataHub MCP Server to understand actual table dependencies instead of guessing them.

## Track
Build with DataHub: The Agent Hackathon — Metadata-Aware Code Generation & Development

## Status
🚧 Work in progress

## Setup
See [SETUP.md](./SETUP.md) for full local environment setup instructions (macOS, Linux, Windows/WSL2).

Quick start:
```bash
git clone git@github.com:tdm291104/datahub-dag-generator.git
cd datahub-dag-generator
uv venv datahub-env --python 3.11 && source datahub-env/bin/activate
uv pip install 'acryl-datahub[sqlalchemy]==1.5.0.6'
datahub docker quickstart
```

Stop DataHub while keeping its data:
```bash
docker compose -p datahub -f ~/.datahub/quickstart/docker-compose.yml --profile quickstart down
```

Delete DataHub and its local data:
```bash
datahub docker nuke
```

## Demo
(video link to be added)
