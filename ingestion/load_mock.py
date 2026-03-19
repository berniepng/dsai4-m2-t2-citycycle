"""
load_mock.py
============
Loads mock CSV data into BigQuery raw dataset.
Run this BEFORE switching to live Meltano ingest.

Usage:
    python ingestion/load_mock.py                  # dry-run (prints schema, no upload)
    python ingestion/load_mock.py --mode=mock      # upload mock CSVs to BQ
    python ingestion/load_mock.py --mode=mock --project=my-gcp-project

Requirements:
    pip install google-cloud-bigquery pandas pyarrow

Environment variables (set in .env or export):
    GCP_PROJECT_ID   — your Google Cloud project ID
    BQ_RAW_DATASET   — target dataset (default: citycycle_raw)
    GOOGLE_APPLICATION_CREDENTIALS — path to service account JSON
                                     (or use `gcloud auth application-default login`)
"""

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parents[1]
MOCK_DIR = ROOT / "data" / "mock"
STATIONS_CSV = MOCK_DIR / "cycle_stations_mock.csv"
RIDES_CSV = MOCK_DIR / "cycle_hire_mock.csv"

# ── Config (override via env or CLI) ──────────────────────────────
DEFAULT_PROJECT = os.getenv("GCP_PROJECT_ID", "YOUR_GCP_PROJECT_ID")
DEFAULT_DATASET = os.getenv("BQ_RAW_DATASET", "citycycle_raw")

# ── BigQuery schemas — mirror the public dataset exactly ──────────
STATIONS_SCHEMA = [
    {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "installed", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "locked", "type": "BOOLEAN", "mode": "NULLABLE"},
    {"name": "install_date", "type": "DATE", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "terminal_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "latitude", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "longitude", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "nbdocks", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "temporary", "type": "BOOLEAN", "mode": "NULLABLE"},
]

RIDES_SCHEMA = [
    {"name": "rental_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "duration", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "bike_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "end_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "end_station_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "end_station_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "start_date", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "start_station_id", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "start_station_name", "type": "STRING", "mode": "NULLABLE"},
]


# ═════════════════════════════════════════════════════════════════
# DRY-RUN  — no BQ client needed
# ═════════════════════════════════════════════════════════════════


def dry_run() -> None:
    """Print schema and row counts — no network calls."""
    print("=" * 60)
    print("DRY RUN — no BigQuery calls will be made")
    print("=" * 60)

    for csv_path, schema, table in [
        (STATIONS_CSV, STATIONS_SCHEMA, "cycle_stations"),
        (RIDES_CSV, RIDES_SCHEMA, "cycle_hire"),
    ]:
        if not csv_path.exists():
            print(f"\n  [MISSING] {csv_path}")
            print("  → Run: python dashboard/utils/mock_data_generator.py")
            continue

        df = pd.read_csv(csv_path)
        print(f"\nTable: raw.{table}")
        print(f"  File  : {csv_path}")
        print(f"  Rows  : {len(df):,}")
        print(f"  Cols  : {list(df.columns)}")
        print("  Schema:")
        for col in schema:
            present = "✓" if col["name"] in df.columns else "✗ MISSING"
            print(f"    {present}  {col['name']:<25} {col['type']}")

    print("\nTo upload: python ingestion/load_mock.py --mode=mock")


# ═════════════════════════════════════════════════════════════════
# MOCK UPLOAD — CSVs → BigQuery raw dataset
# ═════════════════════════════════════════════════════════════════


def upload_mock(project_id: str, dataset_id: str) -> None:
    """
    Upload mock CSVs to BigQuery.
    Uses WRITE_TRUNCATE — safe to re-run, always replaces.
    Cost: BQ data loading from local files is FREE (no query scan).
    """
    try:
        from google.cloud import bigquery
        from google.cloud.bigquery import SchemaField, LoadJobConfig, WriteDisposition
    except ImportError:
        print("[ERROR] google-cloud-bigquery not installed.")
        print("  Run: pip install google-cloud-bigquery pyarrow")
        sys.exit(1)

    # Check files exist — generate if missing
    for path in [STATIONS_CSV, RIDES_CSV]:
        if not path.exists():
            print(f"[WARN] {path} not found — generating mock data first...")
            import subprocess

            subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "dashboard" / "utils" / "mock_data_generator.py"),
                ],
                check=True,
            )
            break

    client = bigquery.Client(project=project_id)

    # Ensure dataset exists
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        print(f"[OK] Dataset {dataset_ref} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "EU"  # london_bicycles is in EU region
        client.create_dataset(dataset, exists_ok=True)
        print(f"[CREATED] Dataset {dataset_ref} (location=EU)")

    # Upload tables
    uploads = [
        (STATIONS_CSV, "cycle_stations", STATIONS_SCHEMA),
        (RIDES_CSV, "cycle_hire", RIDES_SCHEMA),
    ]

    for csv_path, table_name, schema_def in uploads:
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        print(f"\nUploading {csv_path.name} → {table_id} ...")

        df = pd.read_csv(csv_path)

        # Cast types to match BQ schema
        if table_name == "cycle_hire":
            df["start_date"] = pd.to_datetime(df["start_date"])
            df["end_date"] = pd.to_datetime(df["end_date"])
        if table_name == "cycle_stations":
            df["install_date"] = pd.to_datetime(df["install_date"]).dt.date

        schema = [
            SchemaField(col["name"], col["type"], mode=col["mode"])
            for col in schema_def
        ]

        job_config = LoadJobConfig(
            schema=schema,
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
        )

        # Estimate bytes (for logging only — loading is free)
        file_size_kb = csv_path.stat().st_size / 1024
        print(f"  File size : {file_size_kb:.1f} KB")
        print(f"  Rows      : {len(df):,}")
        print("  Write mode: WRITE_TRUNCATE (safe to re-run)")

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # wait for completion

        dest = client.get_table(table_id)
        print(f"  [DONE] {dest.num_rows:,} rows in {table_id}")

    print("\n[SUCCESS] Mock data loaded into BigQuery raw dataset.")
    print(f"  Dataset : {dataset_ref}")
    print("  Tables  : cycle_stations, cycle_hire")
    print("\nNext: run `dbt run` from the transform/ directory")


# ═════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════


def main() -> None:
    parser = argparse.ArgumentParser(
        description="CityCycle — load mock data into BigQuery"
    )
    parser.add_argument(
        "--mode",
        choices=["dry-run", "mock"],
        default="dry-run",
        help="dry-run: print schema only | mock: upload CSVs to BQ (default: dry-run)",
    )
    parser.add_argument("--project", default=DEFAULT_PROJECT, help="GCP project ID")
    parser.add_argument("--dataset", default=DEFAULT_DATASET, help="BQ dataset name")
    args = parser.parse_args()

    if args.mode == "dry-run":
        dry_run()
    elif args.mode == "mock":
        if args.project == "YOUR_GCP_PROJECT_ID":
            print(
                "[ERROR] Set GCP_PROJECT_ID in your .env or pass --project=my-project"
            )
            sys.exit(1)
        upload_mock(project_id=args.project, dataset_id=args.dataset)


if __name__ == "__main__":
    main()
