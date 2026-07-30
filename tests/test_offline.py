"""
Offline tests — no DataHub connection required.
Tests the core logic: lineage sort + DAG rendering using mock data.

Run:
    python -m pytest tests/test_offline.py -v
    # or without pytest:
    python tests/test_offline.py
"""
from __future__ import annotations

import datetime
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

from dag_generator.airflow import _freshness_command, render_dag
from dag_generator.lineage import topological_sort
from dag_generator.models import DatasetNode
from dag_generator.policies import (
    freshness_reason,
    recommended_quality_checks,
    requires_freshness_check,
)

# ── Mock data: nyc-taxi 3-stage pipeline ────────────────────────────────────

RAW_URN = "urn:li:dataset:(urn:li:dataPlatform:sqlite,nyc_taxi.raw_trips,PROD)"
STAGING_URN = "urn:li:dataset:(urn:li:dataPlatform:sqlite,nyc_taxi.staging_trips,PROD)"
MART_URN = "urn:li:dataset:(urn:li:dataPlatform:sqlite,nyc_taxi.mart_daily_summary,PROD)"

MOCK_NODES: dict[str, DatasetNode] = {
    RAW_URN: DatasetNode(
        urn=RAW_URN,
        simple_name="raw_trips",
        upstream_urns=[],
        tags=["daily_refresh", "time_series", "pii", "pipeline_stage"],
        glossary_terms=["FreshnessSLA", "PipelineStage"],
    ),
    STAGING_URN: DatasetNode(
        urn=STAGING_URN,
        simple_name="staging_trips",
        upstream_urns=[RAW_URN],
        tags=["daily_refresh", "time_series", "pii", "pipeline_stage"],
        glossary_terms=["FreshnessSLA", "PipelineStage"],
    ),
    MART_URN: DatasetNode(
        urn=MART_URN,
        simple_name="mart_daily_summary",
        upstream_urns=[STAGING_URN],
        tags=["daily_refresh", "pipeline_stage"],
        glossary_terms=["FreshnessSLA", "EmptyLoad", "PipelineStage"],
    ),
}


def test_topological_sort_order():
    sorted_nodes = topological_sort(MOCK_NODES)
    names = [n.simple_name for n in sorted_nodes]
    assert names == ["raw_trips", "staging_trips", "mart_daily_summary"], (
        f"Wrong order: {names}"
    )
    print("  PASS: topological sort order")


def test_topological_sort_root_has_no_upstreams():
    sorted_nodes = topological_sort(MOCK_NODES)
    root = sorted_nodes[0]
    assert root.upstream_urns == [], f"Root should have no upstreams, got: {root.upstream_urns}"
    print("  PASS: root has no upstreams")


def test_freshness_detection_by_tag():
    raw = MOCK_NODES[RAW_URN]
    assert requires_freshness_check(raw), "raw_trips has daily_refresh tag → should need freshness check"
    print("  PASS: freshness detected via tag")


def test_freshness_detection_by_glossary():
    # Node with only glossary term, no tag
    node = DatasetNode(
        urn="urn:li:dataset:(x,x.test_table,PROD)",
        simple_name="test_table",
        tags=[],
        glossary_terms=["FreshnessSLA"],
    )
    assert requires_freshness_check(node), "FreshnessSLA glossary term → should need freshness check"
    print("  PASS: freshness detected via glossary term")


def test_no_freshness_for_plain_table():
    node = DatasetNode(
        urn="urn:li:dataset:(x,x.plain,PROD)",
        simple_name="plain",
        tags=["pii"],
        glossary_terms=["SomeOtherTerm"],
    )
    assert not requires_freshness_check(node), "No freshness signals → should NOT need freshness check"
    print("  PASS: no freshness check for plain table")


def test_dag_render_contains_all_tables():
    sorted_nodes = topological_sort(MOCK_NODES)
    dag = render_dag(sorted_nodes, dag_id="nyc_taxi_pipeline", platform_instance="nyc_taxi",
                     target_table="mart_daily_summary")
    for table in ("raw_trips", "staging_trips", "mart_daily_summary"):
        assert table in dag, f"Table '{table}' not found in generated DAG"
    print("  PASS: all tables present in DAG")


def test_dag_render_has_freshness_tasks():
    sorted_nodes = topological_sort(MOCK_NODES)
    dag = render_dag(sorted_nodes, dag_id="nyc_taxi_pipeline", platform_instance="nyc_taxi",
                     target_table="mart_daily_summary")
    for table in ("raw_trips", "staging_trips", "mart_daily_summary"):
        assert f"freshness_check_{table}" in dag, (
            f"freshness_check_{table} not found in DAG — all 3 tables have daily_refresh"
        )
    print("  PASS: freshness_check tasks present for all tables")


def test_dag_render_dependency_wiring():
    sorted_nodes = topological_sort(MOCK_NODES)
    dag = render_dag(sorted_nodes, dag_id="nyc_taxi_pipeline", platform_instance="nyc_taxi",
                     target_table="mart_daily_summary")
    # freshness_check_raw_trips gates transform_staging_trips
    assert "freshness_check_raw_trips >> transform_staging_trips" in dag, (
        "Missing dependency: freshness_check_raw_trips >> transform_staging_trips"
    )
    # freshness_check_staging_trips gates aggregate_mart_daily_summary
    assert "freshness_check_staging_trips >> aggregate_mart_daily_summary" in dag, (
        "Missing dependency: freshness_check_staging_trips >> aggregate_mart_daily_summary"
    )
    print("  PASS: dependency wiring correct")


def test_dag_render_valid_python_syntax():
    import ast
    sorted_nodes = topological_sort(MOCK_NODES)
    dag = render_dag(sorted_nodes, dag_id="nyc_taxi_pipeline", platform_instance="nyc_taxi",
                     target_table="mart_daily_summary")
    try:
        ast.parse(dag)
        print("  PASS: generated DAG is valid Python syntax")
    except SyntaxError as e:
        raise AssertionError(f"Generated DAG has invalid Python syntax: {e}\n\n{dag}")


def test_dag_targets_airflow_3_public_api():
    sorted_nodes = topological_sort(MOCK_NODES)
    dag = render_dag(
        sorted_nodes,
        dag_id="nyc_taxi_pipeline",
        platform_instance="nyc_taxi",
        target_table="mart_daily_summary",
        database_path="/opt/airflow/demo-data/nyc_taxi.db",
    )
    assert "from airflow.sdk import DAG" in dag
    assert "from airflow.providers.standard.operators.bash import BashOperator" in dag
    assert "schedule='@daily'" in dag
    assert "schedule_interval" not in dag
    assert "/opt/airflow/demo-data/nyc_taxi.db" in dag
    print("  PASS: Airflow 3 public API and mounted database path")


def test_freshness_command_detects_available_timestamp_column():
    with tempfile.TemporaryDirectory() as directory:
        database = Path(directory) / "demo.db"
        connection = sqlite3.connect(database)
        connection.execute("CREATE TABLE raw_trips (tpep_pickup_datetime TEXT)")
        connection.execute(
            "INSERT INTO raw_trips VALUES (?)",
            (datetime.datetime.now().isoformat(),),
        )
        connection.commit()
        connection.close()
        result = subprocess.run(
            ["bash", "-c", _freshness_command("raw_trips", str(database))],
            capture_output=True,
            env={
                **os.environ,
                "PATH": f"{Path(sys.executable).parent}:{os.environ.get('PATH', '')}",
            },
            text=True,
        )
        assert result.returncode == 0, result.stderr or result.stdout
    print("  PASS: freshness command detects an available timestamp column")


def test_dag_render_stage_verbs():
    sorted_nodes = topological_sort(MOCK_NODES)
    dag = render_dag(sorted_nodes, dag_id="nyc_taxi_pipeline", platform_instance="nyc_taxi",
                     target_table="mart_daily_summary")
    assert "ingest_raw_trips" in dag, "First stage should use 'ingest' verb"
    assert "transform_staging_trips" in dag, "Middle stage should use 'transform' verb"
    assert "aggregate_mart_daily_summary" in dag, "Last stage should use 'aggregate' verb"
    print("  PASS: stage verbs ingest/transform/aggregate assigned correctly")


def test_metadata_quality_checks_are_deterministic():
    sorted_nodes = topological_sort(MOCK_NODES)
    checks = recommended_quality_checks(sorted_nodes)
    dag = render_dag(
        sorted_nodes,
        dag_id="nyc_taxi_pipeline",
        platform_instance="nyc_taxi",
        target_table="mart_daily_summary",
        quality_checks=checks,
    )
    assert "data_audit_raw_trips" in dag
    assert "validate_row_count_mart_daily_summary" in dag
    assert (
        "freshness_check_mart_daily_summary >> "
        "validate_row_count_mart_daily_summary"
    ) in dag
    print("  PASS: deterministic metadata quality checks")


def run_all():
    tests = [
        test_topological_sort_order,
        test_topological_sort_root_has_no_upstreams,
        test_freshness_detection_by_tag,
        test_freshness_detection_by_glossary,
        test_no_freshness_for_plain_table,
        test_dag_render_contains_all_tables,
        test_dag_render_has_freshness_tasks,
        test_dag_render_dependency_wiring,
        test_dag_render_valid_python_syntax,
        test_dag_targets_airflow_3_public_api,
        test_freshness_command_detects_available_timestamp_column,
        test_dag_render_stage_verbs,
        test_metadata_quality_checks_are_deterministic,
    ]

    print(f"\nRunning {len(tests)} offline tests...\n")
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {test.__name__}\n    {e}")
            failed += 1

    print(f"\n{'='*40}")
    print(f"Results: {passed} passed, {failed} failed")
    print(f"{'='*40}\n")
    return failed


if __name__ == "__main__":
    sys.exit(run_all())
