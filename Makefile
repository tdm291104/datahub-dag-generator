VENV ?= datahub-env
PYTHON ?= python3.11
DATAHUB_COMPOSE ?= $(HOME)/.datahub/quickstart/docker-compose.yml
AIRFLOW_COMPOSE ?= docker-compose.airflow.yml
AIRFLOW_PROJECT ?= datahub-dag-airflow
AIRFLOW_UI_PORT ?= 8081
VENV_PYTHON := $(VENV)/bin/python

.DEFAULT_GOAL := help

.PHONY: help venv install require-install start start-datahub stop stop-datahub status-datahub airflow-start airflow-stop airflow-status airflow-logs test check setup-demo-metadata architecture-png

help:
	@printf '%s\n' \
	  'make venv            Create the Python 3.11 virtual environment' \
	  'make install         Install project dependencies into the virtual environment' \
	  'make start           Start DataHub quickstart' \
	  'make stop            Stop DataHub while preserving its Docker volumes/data' \
	  'make status-datahub  Show DataHub container status' \
	  'make airflow-start   Start the local Airflow UI on port 8081' \
	  'make airflow-stop    Stop Airflow while preserving its local state' \
	  'make airflow-status  Show Airflow container status' \
	  'make airflow-logs    Follow Airflow logs' \
	  'make test            Run offline tests' \
	  'make check           Run tests and Python syntax checks' \
	  'make setup-demo-metadata  Attach tags/terms to the nyc-taxi demo' \
	  'make architecture-png  Re-render the architecture diagram PNG from its SVG'

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

airflow-start:
	@mkdir -p dags
	AIRFLOW_UI_PORT="$(AIRFLOW_UI_PORT)" docker compose -p "$(AIRFLOW_PROJECT)" -f "$(AIRFLOW_COMPOSE)" up -d
	@printf '%s\n' "Airflow is starting at http://localhost:$(AIRFLOW_UI_PORT)"

airflow-stop:
	docker compose -p "$(AIRFLOW_PROJECT)" -f "$(AIRFLOW_COMPOSE)" down

airflow-status:
	docker compose -p "$(AIRFLOW_PROJECT)" -f "$(AIRFLOW_COMPOSE)" ps

airflow-logs:
	docker compose -p "$(AIRFLOW_PROJECT)" -f "$(AIRFLOW_COMPOSE)" logs -f airflow

test: require-install
	"$(VENV_PYTHON)" tests/test_offline.py
	"$(VENV_PYTHON)" tests/test_llm_provider.py
	"$(VENV_PYTHON)" tests/test_settings.py
	"$(VENV_PYTHON)" tests/test_agent_safety.py

check: test
	"$(VENV_PYTHON)" -m compileall -q src scripts generate_dag.py

setup-demo-metadata: require-install
	"$(VENV_PYTHON)" scripts/setup_demo_metadata.py

# The README embeds the PNG, not the SVG: GitHub's SVG sanitizer may strip the
# <style> block this diagram relies on for every fill, stroke, and font.
ARCH_DIAGRAM := docs/datahub-dag-generator-architecture
architecture-png:
	@command -v google-chrome >/dev/null 2>&1 || { \
	  echo "google-chrome not found on PATH; install it or export a headless Chromium as google-chrome"; exit 1; }
	google-chrome --headless --disable-gpu --no-sandbox --hide-scrollbars \
	  --window-size=1920,1080 --screenshot="$(ARCH_DIAGRAM).png" \
	  "file://$(CURDIR)/$(ARCH_DIAGRAM).svg"
	@echo "Wrote $(ARCH_DIAGRAM).png"
