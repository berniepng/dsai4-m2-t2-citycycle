"""
mock_data_generator.py
======================
Generates synthetic data that mirrors the exact schema of:
  bigquery-public-data.london_bicycles.cycle_hire
  bigquery-public-data.london_bicycles.cycle_stations

Run this BEFORE any live BigQuery work to validate the full pipeline
at zero cost.

Usage:
    python dashboard/utils/mock_data_generator.py
    python dashboard/utils/mock_data_generator.py --rides 20000 --seed 99

Outputs (written to data/mock/):
    cycle_stations_mock.csv   — 795 rows  (one per real London station)
    cycle_hire_mock.csv       — N rides   (default 10,000)
"""

import argparse
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ── reproducibility ───────────────────────────────────────────────
SEED = 42

# ── output directory ─────────────────────────────────────────────
OUT_DIR = Path(__file__).resolve().parents[2] / "data" / "mock"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ═════════════════════════════════════════════════════════════════
# 1.  STATIONS
# Real London Cycle Hire has 795 stations.
# We generate all 795 using real zone/area names and realistic
# lat/lon bounding box for Greater London.
# ═════════════════════════════════════════════════════════════════

LONDON_AREAS = [
    ("City of London", 51.512, 51.522, -0.100, -0.080),
    ("Westminster", 51.493, 51.510, -0.145, -0.115),
    ("Southwark", 51.490, 51.505, -0.095, -0.070),
    ("Camden", 51.528, 51.545, -0.145, -0.120),
    ("Islington", 51.530, 51.550, -0.110, -0.085),
    ("Tower Hamlets", 51.505, 51.525, -0.060, -0.020),
    ("Hackney", 51.535, 51.560, -0.075, -0.040),
    ("Lambeth", 51.455, 51.475, -0.115, -0.090),
    ("Wandsworth", 51.455, 51.470, -0.200, -0.165),
    ("Kensington", 51.490, 51.508, -0.200, -0.175),
    ("Hammersmith", 51.480, 51.498, -0.235, -0.210),
    ("Canary Wharf", 51.498, 51.510, 0.005, 0.025),
    ("Waterloo", 51.500, 51.507, -0.115, -0.100),
    ("Paddington", 51.512, 51.525, -0.185, -0.170),
    ("King's Cross", 51.526, 51.538, -0.130, -0.112),
    ("Liverpool Street", 51.516, 51.523, -0.085, -0.070),
    ("Victoria", 51.493, 51.500, -0.148, -0.136),
    ("London Bridge", 51.503, 51.510, -0.090, -0.075),
    ("Elephant & Castle", 51.493, 51.500, -0.100, -0.085),
    ("Holborn", 51.514, 51.522, -0.122, -0.108),
]

STREET_SUFFIXES = [
    "Street",
    "Road",
    "Lane",
    "Avenue",
    "Square",
    "Gardens",
    "Place",
    "Terrace",
    "Way",
    "Court",
    "Circus",
    "Bridge",
    "Embankment",
    "Walk",
    "Passage",
    "Row",
    "Mews",
    "Gate",
]

STREET_NAMES = [
    "Baker",
    "Oxford",
    "Bond",
    "King",
    "Queen",
    "George",
    "Albert",
    "Victoria",
    "Prince",
    "Duke",
    "Earl",
    "Manor",
    "Church",
    "High",
    "Market",
    "Bridge",
    "Park",
    "Green",
    "Hill",
    "Mill",
    "Water",
    "North",
    "South",
    "East",
    "West",
    "Old",
    "New",
    "Great",
    "Long",
    "Cross",
    "Tower",
    "Castle",
    "Crown",
    "Rose",
    "Lion",
    "Globe",
    "Broad",
    "Station",
    "Dock",
    "Wharf",
    "Fleet",
    "Strand",
    "Temple",
]


def make_station_name(rng: np.random.Generator) -> str:
    street = rng.choice(STREET_NAMES)
    suffix = rng.choice(STREET_SUFFIXES)
    # occasionally add a number or qualifier
    if rng.random() < 0.3:
        num = rng.integers(1, 50)
        return f"{num} {street} {suffix}"
    return f"{street} {suffix}"


def generate_stations(n: int = 795, seed: int = SEED) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    random.seed(seed)

    rows = []
    install_start = datetime(2010, 1, 1)

    for i in range(1, n + 1):
        area = LONDON_AREAS[i % len(LONDON_AREAS)]
        area_name, lat_min, lat_max, lon_min, lon_max = area

        lat = rng.uniform(lat_min, lat_max)
        lon = rng.uniform(lon_min, lon_max)

        name = make_station_name(rng)
        nb_docks = int(
            rng.choice(
                [10, 15, 18, 20, 22, 24, 27, 30, 35, 40],
                p=[0.10, 0.15, 0.12, 0.18, 0.10, 0.12, 0.10, 0.08, 0.03, 0.02],
            )
        )
        install_days = int(rng.integers(0, 365 * 4))
        install_date = install_start + timedelta(days=install_days)
        installed = rng.random() < 0.97
        locked = rng.random() < 0.03
        temporary = rng.random() < 0.05

        rows.append(
            {
                "id": i,
                "installed": installed,
                "locked": locked,
                "install_date": install_date.strftime("%Y-%m-%d"),
                "name": name,
                "terminal_name": f"0{str(i).zfill(4)}",
                "latitude": round(lat, 6),
                "longitude": round(lon, 6),
                "nbdocks": nb_docks,
                "temporary": temporary,
            }
        )

    return pd.DataFrame(rows)


# ═════════════════════════════════════════════════════════════════
# 2.  RIDES  (cycle_hire)
# Realistic patterns:
#   - Double commuter peaks 07-09 & 17-19
#   - Weekday vs weekend distribution
#   - Short urban trips (median ~12 min, long tail to 120 min)
#   - Station popularity follows power law (few very busy hubs)
# ═════════════════════════════════════════════════════════════════

# Hourly demand weights — realistic London commuter pattern
HOUR_WEIGHTS = [
    0.3,
    0.15,
    0.10,
    0.10,
    0.20,
    0.50,  # 00-05
    1.20,
    3.80,
    4.50,
    2.50,
    1.80,
    1.60,  # 06-11
    2.20,
    1.80,
    1.60,
    1.70,
    2.40,
    4.20,  # 12-17
    3.60,
    2.20,
    1.50,
    1.10,
    0.70,
    0.45,  # 18-23
]

WEEKEND_HOUR_WEIGHTS = [
    0.20,
    0.10,
    0.08,
    0.08,
    0.10,
    0.20,
    0.35,
    0.60,
    1.10,
    1.80,
    2.50,
    2.80,
    3.00,
    2.90,
    2.70,
    2.50,
    2.30,
    2.10,
    1.80,
    1.40,
    1.00,
    0.60,
    0.35,
    0.22,
]


def _hour_from_weights(rng: np.random.Generator, is_weekend: bool) -> int:
    weights = WEEKEND_HOUR_WEIGHTS if is_weekend else HOUR_WEIGHTS
    weights_arr = np.array(weights)
    weights_arr /= weights_arr.sum()
    return int(rng.choice(24, p=weights_arr))


def _duration_seconds(rng: np.random.Generator) -> int:
    """
    Lognormal ride duration.
    median ~720s (12 min), long tail to ~7200s (2 hrs).
    Clamped to BQ source range: 60s – 86400s.
    """
    log_mean = np.log(720)
    log_std = 0.9
    secs = int(rng.lognormal(log_mean, log_std))
    return max(60, min(86400, secs))


def generate_rides(
    stations_df: pd.DataFrame,
    n: int = 10_000,
    seed: int = SEED,
    start_date: str = "2022-01-01",
    end_date: str = "2023-12-31",
) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    station_ids = stations_df["id"].values
    # Power-law popularity: top 10% of stations get ~50% of rides
    popularity = rng.power(0.4, size=len(station_ids))
    popularity /= popularity.sum()

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    date_range_days = (end_dt - start_dt).days

    rows = []
    for rental_id in range(1, n + 1):
        # pick a random day
        day_offset = int(rng.integers(0, date_range_days))
        ride_day = start_dt + timedelta(days=day_offset)
        is_weekend = ride_day.weekday() >= 5

        hour = _hour_from_weights(rng, is_weekend)
        minute = int(rng.integers(0, 60))
        second = int(rng.integers(0, 60))

        start_dt_ride = ride_day.replace(hour=hour, minute=minute, second=second)
        duration_secs = _duration_seconds(rng)
        end_dt_ride = start_dt_ride + timedelta(seconds=duration_secs)

        start_station = int(rng.choice(station_ids, p=popularity))
        # end station: 70% different from start, 30% same area return
        if rng.random() < 0.70:
            end_station = int(rng.choice(station_ids, p=popularity))
        else:
            end_station = start_station

        start_name = stations_df.loc[stations_df["id"] == start_station, "name"].values[
            0
        ]
        end_name = stations_df.loc[stations_df["id"] == end_station, "name"].values[0]

        bike_id = int(rng.integers(1, 15_000))

        rows.append(
            {
                "rental_id": rental_id,
                "duration": duration_secs,
                "bike_id": bike_id,
                "end_date": end_dt_ride.strftime("%Y-%m-%d %H:%M:%S"),
                "end_station_id": end_station,
                "end_station_name": end_name,
                "start_date": start_dt_ride.strftime("%Y-%m-%d %H:%M:%S"),
                "start_station_id": start_station,
                "start_station_name": start_name,
            }
        )

    df = pd.DataFrame(rows)
    # sort by start_date — mirrors BQ source ordering
    df = df.sort_values("start_date").reset_index(drop=True)
    return df


# ═════════════════════════════════════════════════════════════════
# 3.  VALIDATION — quick sanity checks before writing
# ═════════════════════════════════════════════════════════════════


def validate_stations(df: pd.DataFrame) -> None:
    assert df["id"].is_unique, "station id must be unique"
    assert df["id"].notna().all(), "station id must not be null"
    assert (df["nbdocks"] > 0).all(), "docks must be positive"
    assert df["latitude"].between(51.3, 51.7).all(), "lat out of London range"
    assert df["longitude"].between(-0.6, 0.3).all(), "lon out of London range"
    print(f"  [PASS] stations: {len(df)} rows, all checks passed")


def validate_rides(df: pd.DataFrame) -> None:
    assert df["rental_id"].is_unique, "rental_id must be unique"
    assert df["rental_id"].notna().all(), "rental_id must not be null"
    assert df["duration"].between(60, 86400).all(), "duration out of range"
    assert df["start_station_id"].notna().all(), "start_station_id must not be null"
    assert df["end_station_id"].notna().all(), "end_station_id must not be null"
    start_ts = pd.to_datetime(df["start_date"])
    end_ts = pd.to_datetime(df["end_date"])
    assert (end_ts > start_ts).all(), "end_date must be after start_date"
    print(f"  [PASS] rides: {len(df)} rows, all checks passed")


# ═════════════════════════════════════════════════════════════════
# 4.  MAIN
# ═════════════════════════════════════════════════════════════════


def main(n_rides: int = 10_000, seed: int = SEED) -> None:
    print("=" * 60)
    print("CityCycle Mock Data Generator")
    print("=" * 60)

    print(f"\n[1/4] Generating {795} stations ...")
    stations_df = generate_stations(n=795, seed=seed)

    print(f"[2/4] Generating {n_rides:,} rides ...")
    rides_df = generate_rides(stations_df, n=n_rides, seed=seed)

    print("\n[3/4] Validating ...")
    validate_stations(stations_df)
    validate_rides(rides_df)

    print("\n[4/4] Writing CSVs ...")
    stations_path = OUT_DIR / "cycle_stations_mock.csv"
    rides_path = OUT_DIR / "cycle_hire_mock.csv"

    stations_df.to_csv(stations_path, index=False)
    rides_df.to_csv(rides_path, index=False)

    print(
        f"\n  Stations : {stations_path}  ({os.path.getsize(stations_path) / 1024:.1f} KB)"
    )
    print(f"  Rides    : {rides_path}  ({os.path.getsize(rides_path) / 1024:.1f} KB)")

    # ── Quick stats summary ───────────────────────────────────────
    print("\n── Mock Data Summary ──────────────────────────────────────")
    print(f"  Stations  : {len(stations_df)}")
    print(f"  Rides     : {len(rides_df):,}")
    avg_dur = rides_df["duration"].mean() / 60
    med_dur = rides_df["duration"].median() / 60
    print(f"  Avg ride  : {avg_dur:.1f} min  |  Median: {med_dur:.1f} min")

    top_start = rides_df.groupby("start_station_name").size().nlargest(3).index.tolist()
    print(f"  Top 3 start stations: {', '.join(top_start)}")

    peak_hour = pd.to_datetime(rides_df["start_date"]).dt.hour.value_counts().idxmax()
    print(f"  Peak hour : {peak_hour:02d}:00")
    print("\nDone. Safe to use — zero BigQuery API calls made.")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CityCycle mock data generator")
    parser.add_argument(
        "--rides",
        type=int,
        default=10_000,
        help="Number of ride rows to generate (default: 10000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=SEED,
        help="Random seed for reproducibility (default: 42)",
    )
    args = parser.parse_args()
    main(n_rides=args.rides, seed=args.seed)
