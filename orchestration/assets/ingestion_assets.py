"""
orchestration/assets/ingestion_assets.py
==========================================
Dagster software-defined assets for the ingestion layer.

Mock mode (default):
  mock_data_asset  — generates synthetic CSV data using mock_data_generator.py
  mock_bq_load_asset — dry-run validates the CSV schema (no BQ cost)

Production mode (future):
  meltano_ingest_asset — runs Meltano tap-bigquery → target-bigquery
"""

import subprocess
import sys
from pathlib import Path

from dagster import AssetExecutionContext, AssetIn, Output, asset, get_dagster_logger

ROOT = Path(__file__).resolve().parents[2]


@asset(
    group_name="ingestion",
    description="Generate synthetic mock data matching BQ london_bicycles schema.",
    tags={"layer": "ingestion", "cost": "zero"},
)
def mock_data_asset(context: AssetExecutionContext) -> Output[dict]:
    log = get_dagster_logger()
    log.info("Generating mock data...")

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "dashboard" / "utils" / "mock_data_generator.py"),
            "--rides",
            "10000",
            "--seed",
            "42",
        ],
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )

    if result.returncode != 0:
        raise RuntimeError(f"Mock data generation failed:\n{result.stderr}")

    log.info(result.stdout)

    stations_path = ROOT / "data" / "mock" / "cycle_stations_mock.csv"
    rides_path = ROOT / "data" / "mock" / "cycle_hire_mock.csv"

    stations_rows = sum(1 for _ in open(stations_path)) - 1
    rides_rows = sum(1 for _ in open(rides_path)) - 1

    log.info(f"Generated {rides_rows} rides and {stations_rows} stations")

    return Output(
        value={
            "stations_rows": stations_rows,
            "rides_rows": rides_rows,
            "stations_path": str(stations_path),
            "rides_path": str(rides_path),
        },
        metadata={
            "stations_rows": stations_rows,
            "rides_rows": rides_rows,
        },
    )


@asset(
    group_name="ingestion",
    ins={"mock_data": AssetIn("mock_data_asset")},
    description="Validate mock CSV schema matches BigQuery raw table structure.",
    tags={"layer": "ingestion", "cost": "zero"},
)
def mock_bq_load_asset(
    context: AssetExecutionContext,
    mock_data: dict,
) -> Output[dict]:
    log = get_dagster_logger()
    log.info("Validating mock CSV schema (dry-run — no BQ cost)...")

    import pandas as pd

    rides_path = ROOT / "data" / "mock" / "cycle_hire_mock.csv"
    stations_path = ROOT / "data" / "mock" / "cycle_stations_mock.csv"

    rides = pd.read_csv(rides_path, nrows=5)
    stations = pd.read_csv(stations_path, nrows=5)

    expected_ride_cols = {
        "rental_id",
        "bike_id",
        "duration",
        "start_date",
        "start_station_id",
        "start_station_name",
        "end_date",
        "end_station_id",
        "end_station_name",
    }
    expected_station_cols = {
        "id",
        "name",
        "latitude",
        "longitude",
        "nbdocks",
        "terminal_name",
    }

    missing_ride = expected_ride_cols - set(rides.columns)
    missing_station = expected_station_cols - set(stations.columns)

    if missing_ride:
        raise RuntimeError(f"cycle_hire_mock.csv missing columns: {missing_ride}")
    if missing_station:
        raise RuntimeError(
            f"cycle_stations_mock.csv missing columns: {missing_station}"
        )

    log.info(
        f"Schema validation passed — "
        f"{len(rides.columns)} ride cols, {len(stations.columns)} station cols"
    )

    return Output(
        value={
            "status": "validated",
            "mode": "mock_dry_run",
            "rides_rows": mock_data["rides_rows"],
            "stations_rows": mock_data["stations_rows"],
        },
        metadata={
            "mode": "mock_dry_run (no BQ API calls)",
            "rides_columns": list(rides.columns),
            "stations_columns": list(stations.columns),
            "rides_rows": mock_data["rides_rows"],
        },
    )
