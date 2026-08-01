"""Render safe, deterministic Airflow DAG source from a validated plan."""
from __future__ import annotations

import hashlib
import keyword
import re
import shlex

from dag_generator.models import DatasetNode, QualityCheck
from dag_generator.policies import freshness_reason, requires_freshness_check

SUPPORTED_CHECKS = {"row_count", "pii_audit"}


def _identifier(value: str) -> str:
    identifier = re.sub(r"\W+", "_", value, flags=re.ASCII).strip("_")
    if not identifier:
        identifier = "dataset"
    if identifier[0].isdigit() or keyword.iskeyword(identifier):
        identifier = f"dataset_{identifier}"
    return identifier


def _task_suffixes(nodes: list[DatasetNode]) -> dict[str, str]:
    base_by_urn = {node.urn: _identifier(node.simple_name) for node in nodes}
    counts: dict[str, int] = {}
    for base in base_by_urn.values():
        counts[base] = counts.get(base, 0) + 1
    return {
        urn: (
            base
            if counts[base] == 1
            else f"{base}_{hashlib.sha1(urn.encode()).hexdigest()[:8]}"
        )
        for urn, base in base_by_urn.items()
    }


def _stage_verb(node: DatasetNode, known_urns: set[str], is_target: bool) -> str:
    if not any(upstream in known_urns for upstream in node.upstream_urns):
        return "ingest"
    return "aggregate" if is_target else "transform"


def _comment(value: str) -> str:
    return " ".join(value.splitlines())


def _freshness_command(
    table: str,
    database: str,
    upstream_tables: list[str] | None = None,
) -> str:
    # ponytail: roots define the relative baseline; add SLA-aware wall-clock checks
    # when DataHub provides explicit freshness thresholds.
    upstream_tables = upstream_tables or []
    return (
        "python - <<'PY'\n"
        "import datetime\n"
        "import sqlite3\n"
        "import sys\n"
        f"connection = sqlite3.connect({database!r})\n"
        f"table = {table!r}\n"
        f"upstream_tables = {tuple(upstream_tables)!r}\n"
        "candidates = ('trip_date', 'tpep_pickup_datetime', 'event_time', 'updated_at', 'created_at')\n"
        "def latest_date(table_name):\n"
        "    quoted_table = table_name.replace('\"', '\"\"')\n"
        "    columns = {row[1] for row in connection.execute(f'PRAGMA table_info(\"{quoted_table}\")')}\n"
        "    timestamp_column = next((column for column in candidates if column in columns), None)\n"
        "    if timestamp_column is None:\n"
        "        raise ValueError(f'No supported freshness column found in {table_name}: {sorted(columns)}')\n"
        "    query = f'SELECT MAX(\"{timestamp_column}\") FROM \"{quoted_table}\"'\n"
        "    row = connection.execute(query).fetchone()\n"
        "    latest = row[0] if row else None\n"
        "    if latest is None:\n"
        "        raise ValueError(f'No timestamped records found in {table_name}')\n"
        "    return datetime.datetime.fromisoformat(str(latest).replace('Z', '+00:00')).date()\n"
        "try:\n"
        "    downstream_latest = latest_date(table)\n"
        "    upstream_latest = max((latest_date(name) for name in upstream_tables), default=None)\n"
        "except (TypeError, ValueError) as exc:\n"
        "    connection.close()\n"
        "    print(exc)\n"
        "    sys.exit(2)\n"
        "connection.close()\n"
        "print(f'Latest record in {table}:', downstream_latest)\n"
        "if upstream_latest is None:\n"
        "    print('Root dataset establishes the freshness baseline')\n"
        "    sys.exit(0)\n"
        "lag_days = (upstream_latest - downstream_latest).days\n"
        "print('Latest direct upstream record:', upstream_latest)\n"
        "print('Downstream lag in days:', max(lag_days, 0))\n"
        "sys.exit(0 if lag_days <= 0 else 1)\n"
        "PY"
    )


def _quality_command(kind: str, table: str, database: str) -> str:
    if kind == "pii_audit":
        return f"printf '%s\\n' {shlex.quote(f'Audit log: PII access for {table}')}"

    query = f'SELECT COUNT(*) FROM "{table.replace(chr(34), chr(34) * 2)}"'
    return (
        "python - <<'PY'\n"
        "import sqlite3\n"
        "import sys\n"
        f"connection = sqlite3.connect({database!r})\n"
        f"count = connection.execute({query!r}).fetchone()[0]\n"
        "connection.close()\n"
        "print('Row count:', count)\n"
        "sys.exit(0 if count > 0 else 1)\n"
        "PY"
    )


def render_dag(
    sorted_nodes: list[DatasetNode],
    dag_id: str,
    schedule: str = "@daily",
    owner: str = "data_platform_team",
    target_table: str = "",
    platform_instance: str = "",
    quality_checks: list[QualityCheck] | None = None,
    database_path: str = "/opt/airflow/demo-data/data.db",
) -> str:
    """Return Airflow DAG Python source; reject invalid quality-check plans."""
    if not sorted_nodes:
        raise ValueError("Cannot render a DAG without dataset nodes")

    quality_checks = quality_checks or []
    nodes_by_urn = {node.urn: node for node in sorted_nodes}
    if len(nodes_by_urn) != len(sorted_nodes):
        raise ValueError("Dataset URNs must be unique")

    seen_checks: set[tuple[str, str]] = set()
    checks_by_urn: dict[str, list[QualityCheck]] = {}
    for check in quality_checks:
        if check.kind not in SUPPORTED_CHECKS:
            raise ValueError(f"Unsupported quality check: {check.kind}")
        if check.after_urn not in nodes_by_urn:
            raise ValueError(f"Quality check references unknown URN: {check.after_urn}")
        key = (check.kind, check.after_urn)
        if key in seen_checks:
            raise ValueError(f"Duplicate quality check: {check.kind} for {check.after_urn}")
        seen_checks.add(key)
        checks_by_urn.setdefault(check.after_urn, []).append(check)

    suffixes = _task_suffixes(sorted_nodes)
    known_urns = set(nodes_by_urn)
    task_ids: dict[str, str] = {}
    gate_ids: dict[str, str] = {}
    lines: list[str] = []

    regen_parts = ["datahub-dag"]
    if target_table:
        regen_parts.extend(["--target", target_table])
    if platform_instance:
        regen_parts.extend(["--instance", platform_instance])
    regen_command = shlex.join(regen_parts)

    lines += [
        "# Generated by DataHub DAG Generator",
        f"# Source: DataHub lineage graph — {_comment(dag_id)}",
        "# DO NOT EDIT MANUALLY — regenerate with:",
        f"#   {regen_command}",
        "",
        "from datetime import datetime, timedelta, timezone",
        "",
        "from airflow.providers.standard.operators.bash import BashOperator",
        "from airflow.sdk import DAG",
        "",
        "default_args = {",
        f"    'owner': {owner!r},",
        "    'depends_on_past': True,",
        "    'email_on_failure': True,",
        "    'retries': 1,",
        "    'retry_delay': timedelta(minutes=5),",
        "}",
        "",
        "with DAG(",
        f"    dag_id={dag_id!r},",
        "    default_args=default_args,",
        f"    schedule={schedule!r},",
        "    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),",
        "    catchup=False,",
        f"    tags={['generated_by_datahub_agent', platform_instance]!r},",
        ") as dag:",
    ]

    for index, node in enumerate(sorted_nodes):
        suffix = suffixes[node.urn]
        verb = _stage_verb(node, known_urns, index == len(sorted_nodes) - 1)
        main_id = f"{verb}_{suffix}"
        task_ids[node.urn] = main_id
        stage_gates = [main_id]

        lines += [
            "",
            f"    # Stage {index}: {_comment(node.simple_name)}",
            f"    # DataHub URN: {_comment(node.urn)}",
        ]
        if node.tags:
            lines.append(f"    # Tags: {_comment(', '.join(node.tags))}")
        if node.glossary_terms:
            lines.append(f"    # Glossary: {_comment(', '.join(node.glossary_terms))}")

        main_command = f"printf '%s\\n' {shlex.quote(f'Running {verb} for {node.simple_name}...')}"
        lines += [
            f"    {main_id} = BashOperator(",
            f"        task_id={main_id!r},",
            f"        bash_command={main_command!r},",
            f"        doc_md={'Managed by DataHub DAG Generator. URN: ' + node.urn!r},",
            "    )",
        ]

        if requires_freshness_check(node):
            freshness_id = f"freshness_check_{suffix}"
            upstream_tables = [
                nodes_by_urn[urn].simple_name
                for urn in node.upstream_urns
                if urn in nodes_by_urn
            ]
            lines += [
                "",
                f"    # Freshness gate: {_comment(freshness_reason(node))}",
                f"    {freshness_id} = BashOperator(",
                f"        task_id={freshness_id!r},",
                f"        bash_command={_freshness_command(node.simple_name, database_path, upstream_tables)!r},",
                f"        doc_md={'Freshness gate for ' + node.simple_name!r},",
                "    )",
            ]
            stage_gates.append(freshness_id)

        for check in checks_by_urn.get(node.urn, []):
            check_prefix = "validate_row_count" if check.kind == "row_count" else "data_audit"
            check_id = f"{check_prefix}_{suffix}"
            lines += [
                "",
                f"    {check_id} = BashOperator(",
                f"        task_id={check_id!r},",
                f"        bash_command={_quality_command(check.kind, node.simple_name, database_path)!r},",
                f"        doc_md={'Metadata policy: ' + check.kind!r},",
                "    )",
            ]
            stage_gates.append(check_id)

        for upstream_gate, downstream_gate in zip(stage_gates, stage_gates[1:]):
            lines.append(f"    {upstream_gate} >> {downstream_gate}")
        gate_ids[node.urn] = stage_gates[-1]

    lines += ["", "    # Dependencies derived from DataHub lineage"]
    for node in sorted_nodes:
        for upstream_urn in node.upstream_urns:
            if upstream_urn in gate_ids:
                lines.append(f"    {gate_ids[upstream_urn]} >> {task_ids[node.urn]}")

    lines.append("")
    return "\n".join(lines)
