"""
dashboard/utils/bq_client.py
==============================
BigQuery connection utility used by dashboard pages when
USE_MOCK_DATA is toggled OFF.

Uses SQLAlchemy + bigquery-sqlalchemy adapter.
All queries include a LIMIT guard — never run unbounded scans.

Requirements:
    pip install google-cloud-bigquery sqlalchemy-bigquery
"""

import os
from functools import lru_cache

import pandas as pd


@lru_cache(maxsize=1)
def _get_project() -> str:
    project = os.getenv("GCP_PROJECT_ID")
    if not project:
        raise ValueError(
            "GCP_PROJECT_ID environment variable not set. " "Add it to your .env file."
        )
    return project


def run_query(sql: str, max_bytes: int = 500_000_000) -> pd.DataFrame:
    """
    Execute a BigQuery SQL query and return a DataFrame.

    Cost guard: dry-runs the query first.
    If estimated bytes > max_bytes (default 500MB), raises an error
    rather than running an expensive scan.

    Args:
        sql: BigQuery SQL. Use {project} placeholder for project ID.
        max_bytes: Refuse queries that will scan more than this many bytes.

    Returns:
        pd.DataFrame of results.
    """
    from google.cloud import bigquery

    project = _get_project()
    sql = sql.format(project=project)

    client = bigquery.Client(project=project)

    # ── Dry run: check cost before executing ──────────────────────
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_job = client.query(sql, job_config=job_config)

    estimated_bytes = dry_job.total_bytes_processed
    estimated_gb = estimated_bytes / 1e9
    print(f"[BQ dry-run] Estimated scan: {estimated_gb:.3f} GB")

    if estimated_bytes > max_bytes:
        raise ValueError(
            f"Query would scan {estimated_gb:.2f} GB "
            f"(limit: {max_bytes/1e9:.2f} GB). "
            "Add LIMIT or WHERE clauses to reduce scan size."
        )

    # ── Execute ───────────────────────────────────────────────────
    print("[BQ] Running query...")
    result = client.query(sql).to_dataframe()
    print(f"[BQ] Done. {len(result):,} rows returned.")
    return result


def get_station_imbalance(days: int = 7) -> pd.DataFrame:
    """Get station imbalance scores for the last N days."""
    return run_query(
        f"""
        SELECT
            start_station_id        AS station_id,
            start_station_name      AS station_name,
            start_lat               AS latitude,
            start_lon               AS longitude,
            AVG(start_station_imbalance_score)      AS avg_imbalance_score,
            AVG(CAST(start_station_is_imbalanced AS INT64)) AS imbalanced_rate,
            SUM(CASE WHEN start_station_imbalance_direction = 'draining'
                     THEN 1 ELSE 0 END)             AS draining_days,
            COUNT(*)                                 AS total_rides
        FROM `{{project}}.citycycle_marts.fact_rides`
        WHERE hire_date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY 1, 2, 3, 4
        ORDER BY avg_imbalance_score DESC
        LIMIT 1000
    """
    )


def get_hourly_demand(station_id: int = None) -> pd.DataFrame:
    """Get hourly ride distribution (all stations or one specific station)."""
    where = f"AND start_station_id = {station_id}" if station_id else ""
    return run_query(
        f"""
        SELECT
            start_hour,
            COUNT(*) AS ride_count,
            AVG(duration_minutes) AS avg_duration_mins
        FROM `{{project}}.citycycle_marts.fact_rides`
        WHERE hire_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        {where}
        GROUP BY 1
        ORDER BY 1
        LIMIT 24
    """
    )
