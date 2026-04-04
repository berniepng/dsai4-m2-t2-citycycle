"""
ml/models/train_demand_model.py
=================================
Trains THREE demand forecasting models and compares them:
  1. Linear Regression  (baseline)
  2. Random Forest      (ensemble, good all-rounder)
  3. XGBoost            (gradient boosting, often best on tabular data)

The best model by RMSE is saved to demand_model.pkl.
All three are saved individually for reference.

Usage:
    python ml/models/train_demand_model.py              # mock data
    python ml/models/train_demand_model.py --source bq  # live BigQuery
"""

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = ROOT / "ml" / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
MODEL_PATH = MODEL_DIR / "demand_model.pkl"  # best model saved here

# UK bank holidays 2020-2024
UK_BANK_HOLIDAYS = {
    "2020-01-01",
    "2020-04-10",
    "2020-04-13",
    "2020-05-08",
    "2020-05-25",
    "2020-08-31",
    "2020-12-25",
    "2020-12-28",
    "2021-01-01",
    "2021-04-02",
    "2021-04-05",
    "2021-05-03",
    "2021-05-31",
    "2021-08-30",
    "2021-12-27",
    "2021-12-28",
    "2022-01-03",
    "2022-04-15",
    "2022-04-18",
    "2022-05-02",
    "2022-06-02",
    "2022-06-03",
    "2022-08-29",
    "2022-09-19",
    "2022-12-26",
    "2022-12-27",
    "2023-01-02",
    "2023-04-07",
    "2023-04-10",
    "2023-05-01",
    "2023-05-08",
    "2023-05-29",
    "2023-08-28",
    "2023-12-25",
    "2023-12-26",
    "2024-01-01",
    "2024-03-29",
    "2024-04-01",
    "2024-05-06",
    "2024-05-27",
    "2024-08-26",
    "2024-12-25",
    "2024-12-26",
}


# ════════════════════════════════════════════════════════════════
# FEATURE ENGINEERING
# ════════════════════════════════════════════════════════════════


def build_features(rides_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    df = rides_df.copy()
    df["start_date"] = pd.to_datetime(df["start_date"])

    # Use pre-computed columns from fact_rides if available
    if "start_hour" in df.columns:
        df["hour"] = df["start_hour"]
        df["day_of_week"] = (
            df["day_of_week"]
            if "day_of_week" in df.columns
            else df["start_date"].dt.dayofweek
        )
        df["is_weekend"] = (
            df["is_weekend"].astype(int)
            if "is_weekend" in df.columns
            else (df["day_of_week"] >= 5).astype(int)
        )
    else:
        df["hour"] = df["start_date"].dt.hour
        df["day_of_week"] = df["start_date"].dt.dayofweek
        df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)

    if "hire_date" not in df.columns:
        df["hire_date"] = df["start_date"].dt.date

    df["hire_date"] = df["hire_date"].astype(str)
    df["month"] = df["start_date"].dt.month
    df["is_holiday"] = df["hire_date"].isin(UK_BANK_HOLIDAYS).astype(int)

    # Season: 0=winter 1=spring 2=summer 3=autumn
    def get_season(m):
        if m in (12, 1, 2):
            return 0
        elif m in (3, 4, 5):
            return 1
        elif m in (6, 7, 8):
            return 2
        else:
            return 3

    df["season"] = df["month"].apply(get_season)

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
                "season",
            ]
        )
        .size()
        .reset_index(name="ride_count")
    )

    # Rolling 7-day average — lagged to prevent data leakage
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
        "season",
        "start_station_id",
        "rolling_7d_avg",
    ]
    return agg[feature_cols], agg["ride_count"]


# ════════════════════════════════════════════════════════════════
# MODEL COMPARISON TABLE
# ════════════════════════════════════════════════════════════════


def print_comparison(results: list[dict]) -> None:
    print("\n" + "=" * 65)
    print("  MODEL COMPARISON RESULTS")
    print("=" * 65)
    print(f"  {'Model':<22} {'RMSE':>8} {'MAE':>8} {'R²':>8} {'Time':>8}")
    print(f"  {'-'*22} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
    for r in sorted(results, key=lambda x: x["rmse"]):
        best = " ← best" if r == min(results, key=lambda x: x["rmse"]) else ""
        print(
            f"  {r['name']:<22} {r['rmse']:>8.3f} {r['mae']:>8.3f} "
            f"{r['r2']:>8.3f} {r['time']:>7.1f}s{best}"
        )
    print("=" * 65)


# ════════════════════════════════════════════════════════════════
# MAIN TRAINING FUNCTION
# ════════════════════════════════════════════════════════════════


def train(source: str = "mock") -> None:
    print("=" * 65)
    print("  CityCycle — Demand Forecasting Model Training")
    print("  Models: Linear Regression · Random Forest · XGBoost")
    print("=" * 65)

    # ── 1. Load data ──────────────────────────────────────────────
    if source == "mock":
        print("\n[1/5] Loading mock ride data...")
        rides = pd.read_csv(ROOT / "data" / "mock" / "cycle_hire_mock.csv")
    else:
        print("\n[1/5] Loading data from BigQuery...")
        project = os.environ.get("GCP_PROJECT_ID")
        if not project:
            print("[ERROR] GCP_PROJECT_ID not set.")
            sys.exit(1)
        try:
            from google.cloud import bigquery

            client = bigquery.Client(project=project)
            query = """
                SELECT
                    rental_id,
                    start_datetime   AS start_date,
                    duration_seconds AS duration,
                    start_station_id,
                    hire_date,
                    start_hour,
                    day_of_week,
                    is_weekend,
                    peak_hour_flag
                FROM `""" + project + """.citycycle_dev_marts.fact_rides`
                WHERE hire_date BETWEEN '2020-01-01' AND '2023-01-31'
            """
            dry = client.query(
                query,
                job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False),
            )
            est_gb = dry.total_bytes_processed / 1e9
            print(f"  Estimated scan : {est_gb:.3f} GB")

            if est_gb > 50.0:
                print("  [WARN] Over 50 GB — check query before proceeding")
                sys.exit(1)

            print("  Executing query...")
            rides = client.query(query).to_dataframe()
        except Exception as e:
            print(f"[ERROR] BQ query failed: {e}")
            sys.exit(1)

    print(f"  Loaded {len(rides):,} rides")

    # ── 2. Build features ─────────────────────────────────────────
    print("\n[2/5] Building feature matrix...")
    X, y = build_features(rides)
    print(f"  Rows     : {X.shape[0]:,}")
    print(f"  Features : {list(X.columns)}")
    print(
        f"  Target   : {y.min()}–{y.max()} rides/station/hour  "
        f"(mean {y.mean():.2f})"
    )

    # ── 3. Train/test split ───────────────────────────────────────
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import mean_absolute_error, mean_squared_error
    from sklearn.preprocessing import StandardScaler

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(f"\n[3/5] Split: {len(X_train):,} train / {len(X_test):,} test")

    results = []
    models = {}

    # ── 4. Train all three models ─────────────────────────────────
    print("\n[4/5] Training models...\n")

    # ── Model A: Linear Regression (baseline) ─────────────────────
    print("  [A] Linear Regression (baseline)...")
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import Pipeline

    t0 = time.time()
    lr = Pipeline(
        [
            ("scaler", StandardScaler()),
            ("model", Ridge(alpha=1.0)),
        ]
    )
    lr.fit(X_train, y_train)
    elapsed = time.time() - t0

    y_pred_lr = lr.predict(X_test)
    rmse_lr = mean_squared_error(y_test, y_pred_lr) ** 0.5
    mae_lr = mean_absolute_error(y_test, y_pred_lr)
    r2_lr = lr.score(X_test, y_test)
    print(
        f"      RMSE {rmse_lr:.3f}  MAE {mae_lr:.3f}  R² {r2_lr:.3f}  ({elapsed:.1f}s)"
    )

    results.append(
        {
            "name": "Linear Regression",
            "rmse": rmse_lr,
            "mae": mae_lr,
            "r2": r2_lr,
            "time": elapsed,
            "model": lr,
        }
    )
    models["linear_regression"] = lr

    # ── Model B: Random Forest ─────────────────────────────────────
    print("  [B] Random Forest (n=200, depth=12)...")
    from sklearn.ensemble import RandomForestRegressor

    t0 = time.time()
    rf = RandomForestRegressor(
        n_estimators=200,
        max_depth=12,
        min_samples_leaf=3,
        n_jobs=-1,
        random_state=42,
    )
    rf.fit(X_train, y_train)
    elapsed = time.time() - t0

    y_pred_rf = rf.predict(X_test)
    rmse_rf = mean_squared_error(y_test, y_pred_rf) ** 0.5
    mae_rf = mean_absolute_error(y_test, y_pred_rf)
    r2_rf = rf.score(X_test, y_test)
    print(
        f"      RMSE {rmse_rf:.3f}  MAE {mae_rf:.3f}  R² {r2_rf:.3f}  ({elapsed:.1f}s)"
    )

    # Feature importance
    fi = pd.Series(rf.feature_importances_, index=X.columns).sort_values(
        ascending=False
    )
    print("      Feature importance:")
    for feat, imp in fi.items():
        bar = "█" * int(imp * 30)
        print(f"        {feat:<22} {imp:.3f}  {bar}")

    results.append(
        {
            "name": "Random Forest",
            "rmse": rmse_rf,
            "mae": mae_rf,
            "r2": r2_rf,
            "time": elapsed,
            "model": rf,
        }
    )
    models["random_forest"] = rf

    # ── Model C: XGBoost ──────────────────────────────────────────
    print("  [C] XGBoost...")
    try:
        import xgboost as xgb

        t0 = time.time()
        xgb_model = xgb.XGBRegressor(
            n_estimators=300,
            max_depth=8,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            min_child_weight=5,
            n_jobs=-1,
            random_state=42,
            verbosity=0,
        )
        xgb_model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )
        elapsed = time.time() - t0

        y_pred_xgb = xgb_model.predict(X_test)
        rmse_xgb = mean_squared_error(y_test, y_pred_xgb) ** 0.5
        mae_xgb = mean_absolute_error(y_test, y_pred_xgb)
        r2_xgb = xgb_model.score(X_test, y_test)
        print(
            f"      RMSE {rmse_xgb:.3f}  MAE {mae_xgb:.3f}  R² {r2_xgb:.3f}  ({elapsed:.1f}s)"
        )

        # XGBoost feature importance
        xgb_fi = pd.Series(xgb_model.feature_importances_, index=X.columns).sort_values(
            ascending=False
        )
        print("      Feature importance:")
        for feat, imp in xgb_fi.items():
            bar = "█" * int(imp * 30)
            print(f"        {feat:<22} {imp:.3f}  {bar}")

        results.append(
            {
                "name": "XGBoost",
                "rmse": rmse_xgb,
                "mae": mae_xgb,
                "r2": r2_xgb,
                "time": elapsed,
                "model": xgb_model,
            }
        )
        models["xgboost"] = xgb_model

    except ImportError:
        print("      [SKIP] xgboost not installed — run: pip install xgboost")

    # ── 5. Save models ────────────────────────────────────────────
    print("\n[5/5] Saving models...")
    import joblib

    # Save all individual models
    for name, model in models.items():
        path = MODEL_DIR / f"{name}.pkl"
        joblib.dump(model, path)
        print(f"  Saved {name}.pkl  ({path.stat().st_size/1024:.0f} KB)")

    # Save best model as demand_model.pkl (used by dashboard)
    best = min(results, key=lambda x: x["rmse"])
    joblib.dump(best["model"], MODEL_PATH)
    print(
        f"\n  Best model → demand_model.pkl : {best['name']} (RMSE {best['rmse']:.3f})"
    )

    # ── Comparison table ──────────────────────────────────────────
    print_comparison(results)

    print("\nDone. Reload Streamlit dashboard to use updated predictions.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["mock", "bq"], default="mock")
    args = parser.parse_args()
    train(source=args.source)
