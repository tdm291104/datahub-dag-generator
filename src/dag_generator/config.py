"""Application settings loaded from the project's .env file."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

MODEL_CONFIG_FILE = Path(__file__).with_name("llm_models.yaml")


def load_env_file(path: Path | None = None) -> None:
    """Load the nearest .env without overwriting exported variables."""
    env_file = str(path) if path else find_dotenv(usecwd=True)
    if env_file:
        load_dotenv(env_file, override=False)


def _positive_int(name: str, default: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw_value!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


@dataclass(frozen=True)
class Settings:
    datahub_server: str
    datahub_token: str | None
    openrouter_api_key: str | None
    anthropic_api_key: str | None
    model_config_path: Path
    mcp_server_package: str
    max_agent_turns: int
    max_tool_result_chars: int
    max_lineage_nodes: int
    dag_database_path_template: str

    def api_key_for(self, provider: str) -> str | None:
        if provider == "openrouter":
            return self.openrouter_api_key
        if provider == "anthropic":
            return self.anthropic_api_key
        return None


def load_settings(env_file: Path | None = None) -> Settings:
    load_env_file(env_file)
    return Settings(
        datahub_server=os.environ.get("DATAHUB_SERVER", "http://localhost:8080"),
        datahub_token=os.environ.get("DATAHUB_TOKEN") or None,
        openrouter_api_key=os.environ.get("OPENROUTER_API_KEY") or None,
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY") or None,
        model_config_path=Path(
            os.environ.get("LLM_MODEL_CONFIG", str(MODEL_CONFIG_FILE))
        ).expanduser(),
        mcp_server_package=os.environ.get(
            "DATAHUB_MCP_PACKAGE", "mcp-server-datahub@0.6.0"
        ),
        max_agent_turns=_positive_int("MAX_AGENT_TURNS", 20),
        max_tool_result_chars=_positive_int("MAX_TOOL_RESULT_CHARS", 200_000),
        max_lineage_nodes=_positive_int("MAX_LINEAGE_NODES", 200),
        dag_database_path_template=os.environ.get(
            "DAG_DATABASE_PATH_TEMPLATE",
            "/opt/airflow/demo-data/{instance}.db",
        ),
    )
