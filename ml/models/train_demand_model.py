"""
ml/models/train_demand_model.py
=================================
Trains a RandomForest demand forecasting model on mock (or live BQ) data.

Features:
    hour_of_day, day_of_week, is_weekend, is_holiday,
    station_id (label encoded), rolling_7d_avg

Target:
    ride_count per station per hour

Usage:
    python ml/models/train_demand_model.py              # train on mock data
    python ml/models/train_demand_model.py --source bq  # train on live BQ data
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = ROOT / "ml" / "models" / "demand_model.pkl"
MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)


def build_features(rides_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    Aggregate rides to station-hour level and build feature matrix.
    Returns X (features), y (target: ride count per station-hour).
    """
    df = rides_df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])
    df["hire_date"] = df["start_date"].dt.date
    df["hour"] = df["start_date"].dt.hour
    df["day_of_week"] = df["start_date"].dt.dayofweek  # 0=Mon … 6=Sun
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["is_holiday"] = 0  # simplified — extend with actual UK bank holidays

    # Aggregate to station-hour-day level
    agg = (
        df.groupby(
            [
                "hire_date",
                "start_station_id",
                "hour",
                "day_of_week",
                "is_weekend",
                "is_holiday",
            ]
        )
        .size()
        .reset_index(name="ride_count")
    )

    # Rolling 7-day average per station (lagged — no data leakage)
    agg = agg.sort_values(["start_station_id", "hire_date", "hour"])
    agg["rolling_7d_avg"] = (
        agg.groupby(["start_station_id", "hour"])["ride_count"]
        .transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())
        .fillna(0)
    )

    feature_cols = [
        "hour",
        "day_of_week",
        "is_weekend",
        "is_holiday",
        "start_station_id",
        "rolling_7d_avg",
    ]
    X = agg[feature_cols]
    y = agg["ride_count"]
    return X, y


def train(source: str = "mock") -> None:
    print("=" * 55)
    print("CityCycle — Demand Forecasting Model Training")
    print("=" * 55)

    # ── Load data ─────────────────────────────────────────────────
    if source == "mock":
        print("\n[1/4] Loading mock ride data...")
        rides = pd.read_csv(ROOT / "data" / "mock" / "cycle_hire_mock.csv")
    else:
        print("\n[1/4] Loading data from BigQuery...")
        try:
            from google.cloud import bigquery
            import os

            client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
            query = """
                SELECT rental_id, start_date, end_date, duration,
                       start_station_id, end_station_id
                FROM `{project}.citycycle_marts.fact_rides`
                WHERE hire_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
            """.format(
                project=os.environ["GCP_PROJECT_ID"]
            )
            rides = client.query(query).to_dataframe()
        except Exception as e:
            print(f"[ERROR] BQ query failed: {e}")
            sys.exit(1)

    print(f"  Loaded {len(rides):,} rides")

    # ── Build features ────────────────────────────────────────────
    print("\n[2/4] Building feature matrix...")
    X, y = build_features(rides)
    print(f"  Feature matrix: {X.shape[0]:,} rows × {X.shape[1]} features")
    print(f"  Target range: {y.min()} – {y.max()} rides/station/hour")

    # ── Train / test split ────────────────────────────────────────
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    from sklearn.model_selection import train_test_split

    print("\n[3/4] Training RandomForest model (80/20 split)...")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = RandomForestRegressor(
        n_estimators=100,
        max_depth=10,
        min_samples_leaf=5,
        n_jobs=-1,
        random_state=42,
    )
    model.fit(X_train, y_train)

    # ── Evaluate ──────────────────────────────────────────────────
    y_pred = model.predict(X_test)
    rmse = mean_squared_error(y_test, y_pred) ** 0.5
    mae = mean_absolute_error(y_test, y_pred)

    print(f"\n  RMSE : {rmse:.2f} rides/station/hour")
    print(f"  MAE  : {mae:.2f} rides/station/hour")

    if rmse > 10:
        print("  [WARN] RMSE > 10 — model may need more training data")
    else:
        print("  [PASS] RMSE within acceptable threshold (<10 rides/hr)")

    # ── Feature importance ────────────────────────────────────────
    importance = pd.Series(model.feature_importances_, index=X.columns).sort_values(
        ascending=False
    )
    print("\n  Feature importance:")
    for feat, imp in importance.items():
        print(f"    {feat:<22} {imp:.3f}")

    # ── Save model ────────────────────────────────────────────────
    print(f"\n[4/4] Saving model to {MODEL_PATH}...")
    import joblib

    joblib.dump(model, MODEL_PATH)
    print(f"  Saved. ({MODEL_PATH.stat().st_size / 1024:.1f} KB)")

    print("\nModel training complete.")
    print("Reload Streamlit dashboard to use predictions.")
    print("=" * 55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["mock", "bq"], default="mock")
    args = parser.parse_args()
    train(source=args.source)
