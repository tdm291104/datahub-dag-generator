VENV ?= datahub-env
PYTHON ?= python3.11
DATAHUB_COMPOSE ?= $(HOME)/.datahub/quickstart/docker-compose.yml
VENV_PYTHON := $(VENV)/bin/python

.DEFAULT_GOAL := help

.PHONY: help venv install require-install start start-datahub stop stop-datahub status-datahub test check setup-demo-metadata

help:
	@printf '%s\n' \
	  'make venv            Create the Python 3.11 virtual environment' \
	  'make install         Install project dependencies into the virtual environment' \
	  'make start           Start DataHub quickstart' \
	  'make stop            Stop DataHub while preserving its Docker volumes/data' \
	  'make status-datahub  Show DataHub container status' \
	  'make test            Run offline tests' \
	  'make check           Run tests and Python syntax checks' \
	  'make setup-demo-metadata  Attach tags/terms to the nyc-taxi demo'

venv:
	@test -x "$(VENV)/bin/python" || uv venv "$(VENV)" --python "$(PYTHON)"

install: venv
	UV_PROJECT_ENVIRONMENT="$(VENV)" uv sync --locked

require-install:
	@test -x "$(VENV)/bin/datahub-dag" || { printf '%s\n' 'Run make install first.'; exit 1; }

start: start-datahub

start-datahub: require-install
	"$(VENV)/bin/datahub" docker quickstart

stop: stop-datahub

stop-datahub:
	docker compose -p datahub -f "$(DATAHUB_COMPOSE)" --profile quickstart down

status-datahub:
	docker compose -p datahub -f "$(DATAHUB_COMPOSE)" --profile quickstart ps

test: require-install
	"$(VENV_PYTHON)" tests/test_offline.py
	"$(VENV_PYTHON)" tests/test_llm_provider.py
	"$(VENV_PYTHON)" tests/test_settings.py
	"$(VENV_PYTHON)" tests/test_agent_safety.py

check: test
	"$(VENV_PYTHON)" -m compileall -q src scripts generate_dag.py

setup-demo-metadata: require-install
	"$(VENV_PYTHON)" scripts/setup_demo_metadata.py
