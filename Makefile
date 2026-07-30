VENV ?= datahub-env
PYTHON ?= python3.11
DATAHUB_COMPOSE ?= $(HOME)/.datahub/quickstart/docker-compose.yml

.DEFAULT_GOAL := help

.PHONY: help venv install start start-datahub stop stop-datahub status-datahub test

help:
	@printf '%s\n' \
	  'make venv            Create the Python 3.11 virtual environment' \
	  'make install         Install project dependencies into the virtual environment' \
	  'make start           Start DataHub quickstart' \
	  'make stop            Stop DataHub while preserving its Docker volumes/data' \
	  'make status-datahub  Show DataHub container status' \
	  'make test            Run offline tests'

venv:
	@test -x "$(VENV)/bin/python" || uv venv "$(VENV)" --python "$(PYTHON)"

install: venv
	uv pip install --python "$(VENV)/bin/python" -r requirements.txt

start: start-datahub

start-datahub:
	"$(VENV)/bin/datahub" docker quickstart

stop: stop-datahub

stop-datahub:
	docker compose -p datahub -f "$(DATAHUB_COMPOSE)" --profile quickstart down

status-datahub:
	docker compose -p datahub -f "$(DATAHUB_COMPOSE)" --profile quickstart ps

test:
	"$(VENV)/bin/python" tests/test_offline.py
	"$(VENV)/bin/python" tests/test_llm_provider.py
