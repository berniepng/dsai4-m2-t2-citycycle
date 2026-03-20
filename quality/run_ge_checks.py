"""
quality/run_ge_checks.py
========================
Data quality checks against the CityCycle BigQuery data warehouse.

Implements Great Expectations-style checks using custom SQL queries:
  - Null value checks
  - Duplicate checks
  - Referential integrity
  - Business logic validation
  - Value range checks

Usage:
    python quality/run_ge_checks.py                    # all checkpoints
    python quality/run_ge_checks.py --checkpoint post_ingest
    python quality/run_ge_checks.py --checkpoint post_transform
    python quality/run_ge_checks.py --report           # show last results

Requirements:
    pip install google-cloud-bigquery
    export GCP_PROJECT_ID=citycycle-dsai4
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

PROJECT  = os.environ.get("GCP_PROJECT_ID", "citycycle-dsai4")
RAW      = f"`{PROJECT}.citycycle_raw`"
MARTS    = f"`{PROJECT}.citycycle_dev_marts`"
RESULTS_PATH = Path(__file__).parent / "ge_results.json"

PASS = "PASS"
FAIL = "FAIL"
WARN = "WARN"


def get_client():
    from google.cloud import bigquery
    return bigquery.Client(project=PROJECT)


def run_scalar(client, sql):
    result = client.query(sql).result()
    return list(result)[0][0]


class CheckRunner:
    def __init__(self, name):
        self.name    = name
        self.results = []
        self.passed  = 0
        self.failed  = 0
        self.warned  = 0

    def check(self, client, description, sql, condition_fn,
              severity="error", threshold=None):
        try:
            value  = run_scalar(client, sql)
            passed = condition_fn(value)
            status = PASS if passed else (FAIL if severity == "error" else WARN)
        except Exception as e:
            value  = str(e)[:120]
            status = FAIL
            passed = False

        if status == PASS:
            self.passed += 1
        elif status == FAIL:
            self.failed += 1
        else:
            self.warned += 1

        sym = "✓" if status == PASS else ("✗" if status == FAIL else "⚠")
        print(f"  [{status}] {sym} {description}")
        if status != PASS:
            print(f"         Value: {value}")

        self.results.append({
            "check":     description,
            "status":    status,
            "value":     str(value),
            "threshold": str(threshold),
            "severity":  severity,
        })

    def summary(self):
        total = self.passed + self.failed + self.warned
        print(f"\n  {'─'*50}")
        print(f"  {self.name}: {self.passed}/{total} passed"
              f"  |  {self.failed} failed  |  {self.warned} warnings")
        return self.failed == 0


def checkpoint_post_ingest(client):
    print("\n" + "═"*56)
    print("  CHECKPOINT 1: Post-Ingest (citycycle_raw)")
    print("═"*56)
    r = CheckRunner("Post-ingest")

    print("\n  [cycle_hire]")
    r.check(client, "rental_id — not null",
        f"SELECT COUNTIF(rental_id IS NULL) FROM {RAW}.cycle_hire",
        lambda v: v == 0)
    r.check(client, "rental_id — unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT rental_id) FROM {RAW}.cycle_hire",
        lambda v: v == 0)
    r.check(client, "start_date — not null",
        f"SELECT COUNTIF(start_date IS NULL) FROM {RAW}.cycle_hire",
        lambda v: v == 0)
    r.check(client, "start_date — after 2010-01-01",
        f"SELECT COUNTIF(start_date < '2010-01-01') FROM {RAW}.cycle_hire",
        lambda v: v == 0)
    r.check(client, "duration — not null",
        f"SELECT COUNTIF(duration IS NULL) FROM {RAW}.cycle_hire",
        lambda v: v == 0)
    r.check(client, "duration — between 60 and 86400 seconds (source data)",
        f"SELECT COUNTIF(duration < 60 OR duration > 86400) FROM {RAW}.cycle_hire",
        lambda v: v == 0, severity="warning", threshold="Known: ~27K docking errors in public dataset")
    r.check(client, "start_station_id — not null",
        f"SELECT COUNTIF(start_station_id IS NULL) FROM {RAW}.cycle_hire",
        lambda v: v == 0)
    r.check(client, "end_station_id — not null (source data)",
        f"SELECT COUNTIF(end_station_id IS NULL) FROM {RAW}.cycle_hire",
        lambda v: v == 0, severity="warning", threshold="Known: ~312K lost/unreturned bikes in public dataset")
    r.check(client, "end_date — after start_date (source data)",
        f"SELECT COUNTIF(end_date <= start_date) FROM {RAW}.cycle_hire",
        lambda v: v == 0, severity="warning", threshold="Known: ~10K timestamp errors in public dataset")
    r.check(client, "total row count — at least 1M rows",
        f"SELECT COUNT(*) FROM {RAW}.cycle_hire",
        lambda v: v >= 1_000_000, threshold="1,000,000")

    print("\n  [cycle_stations]")
    r.check(client, "station id — not null",
        f"SELECT COUNTIF(id IS NULL) FROM {RAW}.cycle_stations",
        lambda v: v == 0)
    r.check(client, "station id — unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT id) FROM {RAW}.cycle_stations",
        lambda v: v == 0)
    r.check(client, "latitude — valid London range (51.3–51.7)",
        f"SELECT COUNTIF(latitude < 51.3 OR latitude > 51.7) FROM {RAW}.cycle_stations",
        lambda v: v == 0)
    r.check(client, "longitude — valid London range (-0.6–0.3)",
        f"SELECT COUNTIF(longitude < -0.6 OR longitude > 0.3) FROM {RAW}.cycle_stations",
        lambda v: v == 0)
    r.check(client, "nb_docks — positive",
        f"SELECT COUNTIF(docks_count <= 0) FROM {RAW}.cycle_stations",
        lambda v: v == 0)
    r.check(client, "station name — not null",
        f"SELECT COUNTIF(name IS NULL) FROM {RAW}.cycle_stations",
        lambda v: v == 0)

    return r.summary(), r.results


def checkpoint_post_transform(client):
    print("\n" + "═"*56)
    print("  CHECKPOINT 2: Post-Transform (citycycle_dev_marts)")
    print("═"*56)
    r = CheckRunner("Post-transform")

    print("\n  [fact_rides]")
    r.check(client, "rental_id — not null",
        f"SELECT COUNTIF(rental_id IS NULL) FROM {MARTS}.fact_rides",
        lambda v: v == 0)
    r.check(client, "ride_sk — unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT ride_sk) FROM {MARTS}.fact_rides",
        lambda v: v == 0)
    r.check(client, "duration_minutes — between 1 and 1440",
        f"SELECT COUNTIF(duration_minutes < 1 OR duration_minutes > 1440) FROM {MARTS}.fact_rides",
        lambda v: v == 0)
    r.check(client, "start_station_id — not null",
        f"SELECT COUNTIF(start_station_id IS NULL) FROM {MARTS}.fact_rides",
        lambda v: v == 0)
    r.check(client, "hire_date — not null",
        f"SELECT COUNTIF(hire_date IS NULL) FROM {MARTS}.fact_rides",
        lambda v: v == 0)
    r.check(client, "start_hour — between 0 and 23",
        f"SELECT COUNTIF(start_hour < 0 OR start_hour > 23) FROM {MARTS}.fact_rides",
        lambda v: v == 0)
    r.check(client, "duration_band — valid values only",
        f"""SELECT COUNTIF(duration_band NOT IN ('short','medium','long','extended'))
            FROM {MARTS}.fact_rides WHERE duration_band IS NOT NULL""",
        lambda v: v == 0)
    r.check(client, "null rate on key columns — under 5%",
        f"""SELECT
            (COUNTIF(start_station_name IS NULL) +
             COUNTIF(hire_date IS NULL) +
             COUNTIF(duration_minutes IS NULL)) / (COUNT(*) * 3.0)
            FROM {MARTS}.fact_rides""",
        lambda v: v < 0.05, severity="warning", threshold="5%")
    r.check(client, "end_datetime — after start_datetime",
        f"SELECT COUNTIF(end_datetime <= start_datetime) FROM {MARTS}.fact_rides",
        lambda v: v == 0)
    r.check(client, "imbalance_score — between 0 and 1",
        f"""SELECT COUNTIF(start_station_imbalance_score < 0
                           OR start_station_imbalance_score > 1)
            FROM {MARTS}.fact_rides""",
        lambda v: v == 0)
    r.check(client, "total row count — at least 1M rows",
        f"SELECT COUNT(*) FROM {MARTS}.fact_rides",
        lambda v: v >= 1_000_000, threshold="1,000,000")

    print("\n  [dim_stations]")
    r.check(client, "station_id — not null",
        f"SELECT COUNTIF(station_id IS NULL) FROM {MARTS}.dim_stations",
        lambda v: v == 0)
    r.check(client, "station_id — unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT station_id) FROM {MARTS}.dim_stations",
        lambda v: v == 0)
    r.check(client, "latitude — valid London range",
        f"SELECT COUNTIF(latitude < 51.0 OR latitude > 52.0) FROM {MARTS}.dim_stations",
        lambda v: v == 0)
    r.check(client, "nb_docks — positive",
        f"SELECT COUNTIF(nb_docks <= 0) FROM {MARTS}.dim_stations",
        lambda v: v == 0)

    print("\n  [referential integrity]")
    r.check(client, "fact_rides.start_station_id → dim_stations (FK)",
        f"""SELECT COUNT(DISTINCT f.start_station_id)
            FROM {MARTS}.fact_rides f
            LEFT JOIN {MARTS}.dim_stations s ON f.start_station_id = s.station_id
            WHERE s.station_id IS NULL""",
        lambda v: v == 0, severity="warning")

    print("\n  [dim_date]")
    r.check(client, "dim_date — at least 3650 rows",
        f"SELECT COUNT(*) FROM {MARTS}.dim_date",
        lambda v: v >= 3650, threshold="3,650")
    r.check(client, "dim_date.date_day — unique",
        f"SELECT COUNT(*) - COUNT(DISTINCT date_id) FROM {MARTS}.dim_date",
        lambda v: v == 0)

    return r.summary(), r.results


def save_results(all_results, all_passed):
    output = {
        "run_timestamp":  datetime.utcnow().isoformat() + "Z",
        "project":        PROJECT,
        "overall_status": PASS if all_passed else FAIL,
        "checkpoints":    all_results,
    }
    RESULTS_PATH.write_text(json.dumps(output, indent=2))
    print(f"\n  Results saved → {RESULTS_PATH}")


def main():
    parser = argparse.ArgumentParser(description="CityCycle data quality checks")
    parser.add_argument("--checkpoint",
                        choices=["post_ingest", "post_transform", "all"],
                        default="all")
    parser.add_argument("--report", action="store_true")
    args = parser.parse_args()

    if args.report:
        if RESULTS_PATH.exists():
            data = json.loads(RESULTS_PATH.read_text())
            print(f"\nLast run: {data['run_timestamp']}")
            print(f"Overall:  {data['overall_status']}")
            for cp in data["checkpoints"]:
                print(f"\n  {cp['checkpoint']}")
                for c in cp["checks"]:
                    sym = "✓" if c["status"] == PASS else "✗"
                    print(f"    [{c['status']}] {sym} {c['check']}")
        else:
            print("No results found. Run checks first.")
        return

    print("=" * 56)
    print("  CityCycle — Data Quality Checks")
    print(f"  Project: {PROJECT}")
    print(f"  Run at:  {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print("=" * 56)

    client      = get_client()
    all_results = []
    all_passed  = True

    if args.checkpoint in ("post_ingest", "all"):
        passed, results = checkpoint_post_ingest(client)
        all_results.append({"checkpoint": "post_ingest", "checks": results})
        if not passed:
            all_passed = False

    if args.checkpoint in ("post_transform", "all"):
        passed, results = checkpoint_post_transform(client)
        all_results.append({"checkpoint": "post_transform", "checks": results})
        if not passed:
            all_passed = False

    save_results(all_results, all_passed)

    print("\n" + "=" * 56)
    if all_passed:
        print("  OVERALL: PASS — all checks passed ✓")
    else:
        print("  OVERALL: FAIL — one or more checks failed ✗")
    print("=" * 56)
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
