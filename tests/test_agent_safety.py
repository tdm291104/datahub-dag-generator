"""Focused checks for the LLM-to-code trust boundary."""
from __future__ import annotations

import ast
import argparse
import shlex
import tempfile

from dag_generator.agent_loop import _execute_custom_tool, _parse_plan
from dag_generator.airflow import render_dag
from dag_generator.config import MODEL_CONFIG_FILE, Settings
from dag_generator.cli import _emit_result
from dag_generator.models import DatasetNode, QualityCheck


URN = "urn:li:dataset:(urn:li:dataPlatform:sqlite,demo.table,PROD)"


def test_renderer_quotes_untrusted_metadata():
    node = DatasetNode(
        urn=URN + "\n# injected comment",
        simple_name='table"; touch /tmp/unsafe; #',
    )
    source = render_dag([node], dag_id='dag"; broken', platform_instance="../demo")
    tree = ast.parse(source)
    assert "\n# injected comment\n" not in source
    commands = [
        keyword.value.value
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        for keyword in call.keywords
        if keyword.arg == "bash_command" and isinstance(keyword.value, ast.Constant)
    ]
    assert shlex.split(commands[0]) == [
        "printf",
        r"%s\n",
        'Running ingest for table"; touch /tmp/unsafe; #...',
    ]


def test_plan_rejects_unknown_upstream_and_arbitrary_check():
    base_plan = {
        "nodes": [
            {
                "urn": URN,
                "simple_name": "table",
                "upstream_urns": ["urn:unknown"],
                "tags": [],
                "glossary_terms": [],
            }
        ],
        "quality_checks": [],
    }
    try:
        _parse_plan(base_plan, max_nodes=10)
        raise AssertionError("Unknown upstream should be rejected")
    except ValueError as exc:
        assert "undiscovered upstream" in str(exc)

    node = DatasetNode(urn=URN, simple_name="table")
    try:
        render_dag(
            [node],
            dag_id="demo",
            quality_checks=[QualityCheck(kind="arbitrary_shell", after_urn=URN)],
        )
        raise AssertionError("Arbitrary quality check should be rejected")
    except ValueError as exc:
        assert "Unsupported quality check" in str(exc)


def test_plan_normalizes_metadata_urns():
    nodes, _ = _parse_plan(
        {
            "nodes": [
                {
                    "urn": URN,
                    "simple_name": "table",
                    "upstream_urns": [],
                    "tags": ["urn:li:tag:daily_refresh", "plain_tag"],
                    "glossary_terms": [
                        "urn:li:glossaryTerm:freshness_sla",
                        "plain_term",
                    ],
                }
            ],
            "quality_checks": [],
        },
        max_nodes=10,
    )
    assert nodes[0].tags == ["daily_refresh", "plain_tag"]
    assert nodes[0].glossary_terms == ["freshness_sla", "plain_term"]


def test_writeback_is_limited_to_rendered_urns():
    class FakeClient:
        def __init__(self):
            self.written: list[tuple[str, str]] = []

        def write_dag_provenance(self, urn: str, dag_id: str) -> None:
            self.written.append((urn, dag_id))

    settings = Settings(
        datahub_server="http://localhost:8080",
        datahub_token=None,
        openrouter_api_key=None,
        anthropic_api_key=None,
        model_config_path=MODEL_CONFIG_FILE,
        mcp_server_package="mcp-server-datahub@0.6.0",
        max_agent_turns=2,
        max_tool_result_chars=100,
        max_lineage_nodes=10,
        dag_database_path_template="/opt/airflow/demo-data/{instance}.db",
    )
    client = FakeClient()
    plan = {
        "nodes": [
            {
                "urn": URN,
                "simple_name": "table",
                "upstream_urns": [],
                "tags": [],
                "glossary_terms": [],
            }
        ],
        "quality_checks": [],
    }
    _, _, rendered_urns, _, _ = _execute_custom_tool(
        "render_airflow_dag",
        plan,
        client,
        settings,
        "demo",
        "@daily",
        "demo",
        "table",
        True,
        False,
        None,
    )
    try:
        _execute_custom_tool(
            "datahub_write_back",
            {"urns": [URN, "urn:other"]},
            client,
            settings,
            "demo",
            "@daily",
            "demo",
            "table",
            True,
            False,
            rendered_urns,
        )
        raise AssertionError("Unexpected URNs should be rejected")
    except ValueError as exc:
        assert "exactly match" in str(exc)
    assert client.written == []


def test_dag_id_cannot_escape_output_directory():
    from dag_generator.models import GenerationResult

    with tempfile.TemporaryDirectory() as directory:
        args = argparse.Namespace(dry_run=False, output=directory)
        fake_result = GenerationResult(dag_source="source", sorted_nodes=[], quality_checks=[])
        try:
            _emit_result(fake_result, args, "../outside")
            raise AssertionError("Path traversal DAG ID should be rejected")
        except ValueError as exc:
            assert "path separators" in str(exc)


if __name__ == "__main__":
    test_renderer_quotes_untrusted_metadata()
    test_plan_rejects_unknown_upstream_and_arbitrary_check()
    test_plan_normalizes_metadata_urns()
    test_writeback_is_limited_to_rendered_urns()
    test_dag_id_cannot_escape_output_directory()
    print("5 agent safety tests passed")
