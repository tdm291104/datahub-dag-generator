#!/usr/bin/env python3
"""
DataHub DAG Generator Agent

Reads real lineage and metadata from DataHub, generates an Airflow DAG file,
then writes provenance back to DataHub (tags each dataset as dag_managed).

Usage:
    # Agent mode (default) — OpenRouter + GPT-5.2 reasons about metadata
    python generate_dag.py --target mart_daily_summary --instance nyc_taxi
    python generate_dag.py --target mart_daily_summary --instance nyc_taxi --dag-id nyc_taxi_pipeline
    python generate_dag.py --provider anthropic --target mart_daily_summary --instance nyc_taxi
    python generate_dag.py --model deepseek/deepseek-v4-flash --target mart_daily_summary --instance nyc_taxi

    # Script mode — deterministic pipeline, no LLM
    python generate_dag.py --target mart_daily_summary --instance nyc_taxi --mode script

    python generate_dag.py --target mart_daily_summary --instance nyc_taxi --dry-run
    python generate_dag.py --target mart_daily_summary --instance nyc_taxi --no-writeback
    python generate_dag.py --urn "urn:li:dataset:(urn:li:dataPlatform:sqlite,nyc_taxi.mart_daily_summary,PROD)"
"""
from __future__ import annotations

import argparse
import os
import sys

from agent.datahub_client import DataHubClient
from agent.dag_renderer import render_dag
from agent.lineage_graph import topological_sort


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Generate an Airflow DAG from DataHub lineage",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    target_group = p.add_mutually_exclusive_group(required=True)
    target_group.add_argument(
        "--target", metavar="TABLE",
        help="Target table name (most downstream table in the pipeline)",
    )
    target_group.add_argument(
        "--urn", metavar="URN",
        help="Full DataHub dataset URN of the target table",
    )
    p.add_argument(
        "--instance", metavar="INSTANCE", default="nyc_taxi",
        help="DataHub platform instance name (default: nyc_taxi)",
    )
    p.add_argument(
        "--dag-id", metavar="DAG_ID",
        help="Airflow DAG ID (default: <instance>_pipeline)",
    )
    p.add_argument(
        "--schedule", default="@daily",
        help="Airflow schedule interval (default: @daily)",
    )
    p.add_argument(
        "--output", metavar="DIR", default="output",
        help="Directory to write the generated DAG file (default: output/)",
    )
    p.add_argument(
        "--server", default="http://localhost:8080",
        help="DataHub GMS server URL (default: http://localhost:8080)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Print the generated DAG to stdout without saving or writing back to DataHub",
    )
    p.add_argument(
        "--no-writeback", action="store_true",
        help="Skip writing provenance back to DataHub",
    )
    p.add_argument(
        "--mode", choices=["agent", "script"], default="agent",
        help=(
            "Generation mode: 'agent' (default) uses an LLM for metadata reasoning; "
            "'script' uses the deterministic pipeline without an LLM"
        ),
    )
    p.add_argument(
        "--provider", choices=["openrouter", "anthropic"], default="openrouter",
        help="LLM provider for agent mode (default: openrouter)",
    )
    p.add_argument(
        "--model", metavar="MODEL",
        help=(
            "LLM model override. Use an OpenRouter slug by default, or an Anthropic model ID "
            "with --provider anthropic."
        ),
    )
    return p.parse_args()


def _run_agent_mode(args: argparse.Namespace, dag_id: str) -> int:
    """Run the LLM agentic pipeline."""
    from agent.llm_agent import run_dag_agent

    api_key_name = "OPENROUTER_API_KEY" if args.provider == "openrouter" else "ANTHROPIC_API_KEY"
    if not os.environ.get(api_key_name):
        print(f"ERROR: {api_key_name} is not set.")
        print(f"  export {api_key_name}=<your-key>")
        return 1

    if args.urn:
        print("ERROR: --urn is not supported in agent mode. Use --target instead.")
        return 1

    try:
        dag_content = run_dag_agent(
            target_table=args.target,
            platform_instance=args.instance,
            server=args.server,
            dag_id=dag_id,
            schedule=args.schedule,
            dry_run=args.dry_run,
            no_writeback=args.no_writeback,
            verbose=True,
            provider=args.provider,
            model=args.model,
        )
    except Exception as e:
        if "Connection refused" in str(e) or "ConnectionError" in type(e).__name__:
            print("ERROR: Cannot connect to DataHub at", args.server)
            print("  Start DataHub with: datahub docker quickstart")
        else:
            print(f"ERROR: {e}")
        return 1

    if args.dry_run:
        print(f"\n{'─' * 60}")
        print(dag_content)
        print(f"{'─' * 60}")
        print("[dry-run] No file written.")
        return 0

    os.makedirs(args.output, exist_ok=True)
    out_path = os.path.join(args.output, f"{dag_id}.py")
    with open(out_path, "w") as f:
        f.write(dag_content)
    print(f"Done. DAG saved to: {out_path}")
    return 0


def main() -> int:
    args = parse_args()

    dag_id = args.dag_id or f"{args.instance}_pipeline"

    if args.mode == "agent":
        return _run_agent_mode(args, dag_id)

    client = DataHubClient(server=args.server)

    # ── Step 1: Resolve target URN ───────────────────────────────────────────
    if args.urn:
        root_urn = args.urn
        target_table = root_urn.split(".")[-2] if "." in root_urn else "unknown"
    else:
        print(f"[1/4] Searching DataHub for '{args.target}' in instance '{args.instance}'...")
        try:
            root_urn = client.find_urn(args.target, args.instance)
        except Exception as e:
            if "Connection refused" in str(e) or "ConnectionError" in type(e).__name__:
                print("  ERROR: Cannot connect to DataHub at", args.server)
                print("  Start DataHub with: datahub docker quickstart")
            else:
                print(f"  ERROR: {e}")
            return 1
        if not root_urn:
            print(f"  ERROR: '{args.target}' not found in instance '{args.instance}'.")
            print("  Tip: check that DataHub is running and the dataset has been ingested.")
            return 1
        target_table = args.target
        print(f"  Found: {root_urn}")

    # ── Step 2: Traverse upstream lineage ────────────────────────────────────
    print(f"[2/4] Traversing upstream lineage from '{target_table}'...")
    nodes = client.get_lineage_graph(root_urn)
    print(f"  Found {len(nodes)} dataset(s) in the pipeline")

    sorted_nodes = topological_sort(nodes)
    pipeline_str = " → ".join(n.simple_name for n in sorted_nodes)
    print(f"  Execution order: {pipeline_str}")

    # ── Step 3: Render DAG ───────────────────────────────────────────────────
    print(f"[3/4] Generating Airflow DAG '{dag_id}'...")

    for node in sorted_nodes:
        freshness_signals = []
        from agent.freshness_inspector import requires_freshness_check, freshness_reason
        if requires_freshness_check(node):
            freshness_signals.append(freshness_reason(node))

        status = f"+ freshness_check  [{', '.join(freshness_signals)}]" if freshness_signals else ""
        print(f"  {node.simple_name:30s} {status}")

    dag_content = render_dag(
        sorted_nodes=sorted_nodes,
        dag_id=dag_id,
        schedule=args.schedule,
        target_table=target_table,
        platform_instance=args.instance,
    )

    if args.dry_run:
        print(f"\n{'─' * 60}")
        print(dag_content)
        print(f"{'─' * 60}")
        print("[dry-run] No file written, no write-back to DataHub.")
        return 0

    # ── Step 4: Save DAG file ────────────────────────────────────────────────
    os.makedirs(args.output, exist_ok=True)
    out_path = os.path.join(args.output, f"{dag_id}.py")
    with open(out_path, "w") as f:
        f.write(dag_content)
    print(f"  Saved → {out_path}")

    # ── Step 5: Write provenance back to DataHub ─────────────────────────────
    if not args.no_writeback:
        print(f"[4/4] Writing provenance back to DataHub...")
        for node in sorted_nodes:
            client.write_dag_provenance(node.urn, dag_id)
            print(f"  Tagged '{node.simple_name}' with dag_managed + description updated")
    else:
        print("[4/4] Skipping write-back (--no-writeback)")

    print(f"\nDone. DAG saved to: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
