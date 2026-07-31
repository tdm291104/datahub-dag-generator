"""Command-line entry point for DAG generation."""
from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from dag_generator.airflow import render_dag
from dag_generator.config import Settings, load_settings
from dag_generator.datahub import DataHubClient, _urn_to_table_name
from dag_generator.lineage import topological_sort
from dag_generator.policies import recommended_quality_checks


def parse_args(settings: Settings) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate an Airflow DAG from DataHub lineage"
    )
    target_group = parser.add_mutually_exclusive_group(required=True)
    target_group.add_argument("--target", metavar="TABLE")
    target_group.add_argument("--urn", metavar="URN")
    parser.add_argument("--instance", default="nyc_taxi", metavar="INSTANCE")
    parser.add_argument("--dag-id", metavar="DAG_ID")
    parser.add_argument("--schedule", default="@daily")
    parser.add_argument("--output", default="output", metavar="DIR")
    parser.add_argument(
        "--server",
        default=settings.datahub_server,
        help="DataHub GMS URL (default: DATAHUB_SERVER or localhost:8080)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--writeback",
        action="store_true",
        help="Write dag_managed provenance to DataHub (disabled by default)",
    )
    parser.add_argument(
        "--no-writeback",
        action="store_false",
        dest="writeback",
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--mode", choices=["agent", "script"], default="agent")
    parser.add_argument(
        "--provider",
        choices=["openrouter", "anthropic"],
        default="openrouter",
    )
    parser.add_argument("--model", metavar="MODEL")
    parser.set_defaults(writeback=False)
    return parser.parse_args()


def _emit_result(dag_source: str, args: argparse.Namespace, dag_id: str) -> None:
    if args.dry_run:
        print(f"\n{'─' * 60}\n{dag_source}\n{'─' * 60}")
        print("[dry-run] No file written and no DataHub metadata changed.")
        return

    if Path(dag_id).name != dag_id or dag_id in {".", ".."} or "\\" in dag_id:
        raise ValueError("DAG ID cannot contain path separators")
    output_directory = Path(args.output)
    output_directory.mkdir(parents=True, exist_ok=True)
    output_path = output_directory / f"{dag_id}.py"
    output_path.write_text(dag_source)
    print(f"Done. DAG saved to: {output_path}")


def _run_agent(
    args: argparse.Namespace,
    settings: Settings,
    dag_id: str,
) -> str:
    if args.urn:
        raise ValueError("--urn is only supported with --mode script")

    from dag_generator.agent_loop import run_dag_agent

    return run_dag_agent(
        target_table=args.target,
        platform_instance=args.instance,
        dag_id=dag_id,
        schedule=args.schedule,
        dry_run=args.dry_run,
        writeback=args.writeback,
        provider=args.provider,
        model=args.model,
        settings=settings,
    )


def _run_script(
    args: argparse.Namespace,
    settings: Settings,
    dag_id: str,
) -> str:
    client = DataHubClient(settings.datahub_server, settings.datahub_token)
    if args.urn:
        root_urn = args.urn
        target_table = _urn_to_table_name(root_urn)
    else:
        print(f"[1/4] Searching for {args.target!r} in {args.instance!r}...")
        root_urn = client.find_urn(args.target, args.instance)
        if not root_urn:
            raise ValueError(
                f"Dataset {args.target!r} was not found in instance {args.instance!r}"
            )
        target_table = args.target

    print(f"[2/4] Traversing lineage from {target_table!r}...")
    nodes = client.get_lineage_graph(root_urn)
    if len(nodes) > settings.max_lineage_nodes:
        raise ValueError(
            f"Lineage has {len(nodes)} nodes; limit is {settings.max_lineage_nodes}"
        )
    sorted_nodes = topological_sort(nodes)
    print("  Execution order:", " → ".join(node.simple_name for node in sorted_nodes))

    print(f"[3/4] Rendering DAG {dag_id!r}...")
    dag_source = render_dag(
        sorted_nodes=sorted_nodes,
        dag_id=dag_id,
        schedule=args.schedule,
        target_table=target_table,
        platform_instance=args.instance,
        quality_checks=recommended_quality_checks(sorted_nodes),
        database_path=settings.dag_database_path_template.replace(
            "{instance}", args.instance
        ),
    )

    if args.writeback and not args.dry_run:
        print("[4/4] Writing provenance to DataHub...")
        for node in sorted_nodes:
            client.write_dag_provenance(node.urn, dag_id)
    else:
        print("[4/4] Write-back disabled")
    return dag_source


def main() -> int:
    try:
        settings = load_settings()
        args = parse_args(settings)
        settings = replace(settings, datahub_server=args.server)
        dag_id = args.dag_id or f"{args.instance}_pipeline"
        dag_source = (
            _run_agent(args, settings, dag_id)
            if args.mode == "agent"
            else _run_script(args, settings, dag_id)
        )
        _emit_result(dag_source, args, dag_id)
        return 0
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}")
        return 1
