"""
dashboard/utils/bq_client.py
==============================
BigQuery client with cost guard built in.
Every live query is dry-run checked before execution.
"""

from functools import lru_cache
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


@lru_cache(maxsize=1)
def _get_guard():
    from ingestion.bq_cost_guard import BQCostGuard

    return BQCostGuard()


def run_query(sql: str, label: str = "dashboard query"):
    """Execute a BigQuery query via the cost guard. Blocks if over budget."""
    guard = _get_guard()
    project = guard.project_id
    sql = sql.format(project=project)
    return guard.run_query(sql, label=label)


def get_station_imbalance(days: int = 7):
    from ingestion.bq_cost_guard import safe_station_imbalance_query

    guard = _get_guard()
    sql = safe_station_imbalance_query(guard.project_id, days=days)
    return guard.run_query(sql, label=f"station_imbalance_{days}d")


def get_hourly_demand(days: int = 30):
    from ingestion.bq_cost_guard import safe_hourly_demand_query

    guard = _get_guard()
    sql = safe_hourly_demand_query(guard.project_id, days=days)
    return guard.run_query(sql, label=f"hourly_demand_{days}d")
