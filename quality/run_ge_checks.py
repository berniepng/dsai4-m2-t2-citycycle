"""
run_ge_checks.py
================
Runs Great Expectations checkpoints against BigQuery tables.
Called by Dagster after each pipeline stage.

Usage:
    python quality/run_ge_checks.py --checkpoint post_ingest
    python quality/run_ge_checks.py --checkpoint post_transform
    python quality/run_ge_checks.py --checkpoint all

Requirements:
    pip install great-expectations sqlalchemy-bigquery
"""

import argparse
import sys
from pathlib import Path

# ── GE context ───────────────────────────────────────────────────
GE_DIR = Path(__file__).resolve().parent

CHECKPOINTS = {
    "post_ingest": ["raw_cycle_hire_suite", "raw_cycle_stations_suite"],
    "post_transform": ["fact_rides_suite", "dim_stations_suite"],
}


def run_checkpoint(checkpoint_name: str) -> bool:
    """
    Runs a GE checkpoint.
    Returns True if all expectations pass, False otherwise.
    """
    import great_expectations as gx

    print(f"\n[GE] Running checkpoint: {checkpoint_name}")
    print("=" * 55)

    context = gx.get_context(project_root_dir=str(GE_DIR))

    try:
        result = context.run_checkpoint(checkpoint_name=checkpoint_name)
        success = result["success"]

        if success:
            print(f"  [PASS] Checkpoint '{checkpoint_name}' — all expectations met")
        else:
            print(f"  [FAIL] Checkpoint '{checkpoint_name}' — expectation(s) failed")
            # Print failing expectations summary
            for run_result in result.run_results.values():
                stats = run_result["validation_result"]["statistics"]
                print(f"    Evaluated : {stats['evaluated_expectations']}")
                print(f"    Successful: {stats['successful_expectations']}")
                print(f"    Failed    : {stats['unsuccessful_expectations']}")

        return success

    except Exception as e:
        print(f"  [ERROR] {checkpoint_name}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="CityCycle GE quality checks")
    parser.add_argument(
        "--checkpoint",
        choices=["post_ingest", "post_transform", "all"],
        default="all",
        help="Which checkpoint to run (default: all)",
    )
    args = parser.parse_args()

    checkpoints_to_run = (
        list(CHECKPOINTS.keys()) if args.checkpoint == "all" else [args.checkpoint]
    )

    all_passed = True
    for cp in checkpoints_to_run:
        passed = run_checkpoint(cp)
        if not passed:
            all_passed = False

    if all_passed:
        print("\n[SUCCESS] All quality checks passed. Pipeline may continue.")
        sys.exit(0)
    else:
        print("\n[BLOCKED] Quality checks failed. Pipeline halted.")
        print("  Review GE data docs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
