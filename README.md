# 🚲 CityCycle London — Bike Rebalancing Intelligence Pipeline

> **dsai4-m2-t2-citycycle-c**  
> End-to-end ELT pipeline for the London Bicycle Sharing dataset, built for the CityCycle operations team to solve the bike rebalancing problem using data engineering, ML forecasting, and interactive dashboards.

---

## Table of Contents

1. [Business Problem](#business-problem)
2. [Solution Overview](#solution-overview)
3. [Architecture](#architecture)
4. [Tech Stack](#tech-stack)
5. [Repository Structure](#repository-structure)
6. [Getting Started](#getting-started)
7. [Mock Data Strategy (Free Tier Protection)](#mock-data-strategy)
8. [Pipeline Walkthrough](#pipeline-walkthrough)
   - [1. Ingestion (Meltano)](#1-ingestion-meltano)
   - [2. Data Warehouse Design (BigQuery Star Schema)](#2-data-warehouse-design)
   - [3. ELT Transformation (dbt)](#3-elt-transformation-dbt)
   - [4. Data Quality (Great Expectations)](#4-data-quality-great-expectations)
   - [5. Analysis & ML (Python / scikit-learn)](#5-analysis--ml)
   - [6. Orchestration (Dagster)](#6-orchestration-dagster)
   - [7. Dashboards (Streamlit + Looker Studio)](#7-dashboards)
9. [Key Findings (Mock Data)](#key-findings-mock-data)
10. [Risks & Mitigations](#risks--mitigations)
11. [Contributing](#contributing)

---

## Business Problem

London's CityCycle bike-sharing network operates **795 docking stations** across the city, processing millions of rides annually. The core operational challenge is **bike rebalancing**: stations run empty (stranded demand) or overflow (no docks to return), leading to:

- **Lost revenue** from unfulfilled rentals
- **Increased operational costs** for manual rebalancing crews
- **Poor customer experience** and negative NPS
- **Inefficient fleet utilisation** across the network

**Goal:** Build an intelligent, data-driven pipeline that ingests ride history, detects imbalance patterns, forecasts demand per station, and visualises actionable rebalancing recommendations in near real-time.

---

## Solution Overview

```
BigQuery Public Data → Meltano Ingest → BQ Raw → dbt Transform
→ Great Expectations Quality Gate → ML Demand Forecast
→ Streamlit Dashboard + Looker Studio Report
(All orchestrated by Dagster, running daily at 02:00 UTC)
```

---

## Architecture

![CityCycle ELT Pipeline Architecture](docs/diagrams/dataflow_diagram.png)

The pipeline follows a **medallion-style** architecture:
- **Bronze** (`raw.*`): Raw tables ingested from BigQuery public dataset via Meltano
- **Silver** (`staging.*`): Cleaned, typed, validated tables via dbt staging models
- **Gold** (`marts.*`): Star schema fact/dimension tables for analytics and ML

---

## Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | **Meltano** (tap-bigquery → target-bigquery) | Singer-protocol EL from source to raw |
| Warehouse | **Google BigQuery** | Cloud data warehouse, star schema |
| Transform | **dbt Core** | SQL-based ELT, lineage, testing |
| Quality | **Great Expectations** | Expectation suites, checkpoints, data docs |
| Orchestration | **Dagster** | Asset-based pipeline, schedules, alerts |
| Analysis | **Python / pandas / scikit-learn** | EDA, feature engineering, ML |
| Dashboard | **Streamlit** | Interactive ops dashboard + geospatial map |
| BI Reporting | **Looker Studio** | Executive KPI report (BQ connector) |

---

## Repository Structure

```
dsai4-m2-t2-citycycle-c/
├── .github/
│   └── workflows/
│       └── ci.yml                    # GitHub Actions: lint, test, dry-run
├── ingestion/
│   ├── meltano.yml                   # Meltano project config (taps & targets)
│   ├── load_mock.py                  # Python loader: mock CSV → BigQuery
│   └── README.md
├── warehouse/
│   ├── schema/
│   │   ├── raw_schema.sql            # Raw table DDL
│   │   └── star_schema.sql           # Dimension + fact table DDL
│   └── README.md
├── transform/
│   ├── dbt_project.yml               # dbt project config
│   ├── profiles_template.yml         # profiles.yml template (DO NOT commit real profiles.yml)
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_cycle_hire.sql
│   │   │   ├── stg_cycle_stations.sql
│   │   │   └── _staging.yml          # schema tests
│   │   ├── intermediate/
│   │   │   ├── int_rides_enriched.sql
│   │   │   └── int_station_daily_stats.sql
│   │   └── marts/
│   │       ├── dim_stations.sql
│   │       ├── dim_date.sql
│   │       ├── fact_rides.sql
│   │       └── _marts.yml
│   ├── macros/
│   │   └── generate_surrogate_key.sql
│   └── tests/
│       └── assert_ride_duration_positive.sql
├── quality/
│   ├── great_expectations.yml
│   ├── expectations/
│   │   └── suites/
│   │       ├── raw_cycle_hire.json
│   │       └── fact_rides.json
│   ├── checkpoints/
│   │   ├── post_ingest.yml
│   │   └── post_transform.yml
│   └── run_ge_checks.py
├── orchestration/
│   ├── workspace.yaml                # Dagster workspace
│   ├── assets/
│   │   ├── ingestion_assets.py
│   │   ├── transform_assets.py
│   │   └── quality_assets.py
│   ├── jobs/
│   │   └── citycycle_pipeline_job.py
│   └── sensors/
│       └── bq_sensor.py
├── analysis/
│   ├── notebooks/
│   │   ├── 01_eda_mock_data.ipynb
│   │   ├── 02_station_imbalance_analysis.ipynb
│   │   └── 03_demand_forecasting_model.ipynb
│   └── scripts/
│       ├── connect_bq.py             # SQLAlchemy → BigQuery
│       └── run_analysis.py
├── ml/
│   ├── features/
│   │   └── feature_engineering.py
│   └── models/
│       ├── train_demand_model.py
│       └── predict_rebalancing.py
├── dashboard/
│   ├── app.py                        # Streamlit entry point
│   ├── pages/
│   │   ├── 01_overview.py
│   │   ├── 02_station_map.py         # Geospatial map (pydeck / folium)
│   │   ├── 03_rebalancing.py
│   │   └── 04_forecast.py
│   └── utils/
│       ├── bq_client.py
│       └── mock_data_generator.py
├── data/
│   └── mock/
│       ├── cycle_hire_mock.csv       # ~10,000 synthetic rides
│       └── cycle_stations_mock.csv  # 795 station records
├── docs/
│   ├── diagrams/
│   │   ├── dataflow_diagram.png      # Architecture diagram (this README)
│   │   └── star_schema_erd.png
│   └── reports/
│       └── technical_report.md
├── .env.example                      # Template for env vars (no secrets)
├── .gitignore
├── requirements.txt
├── pyproject.toml
└── README.md                         # This file
```

---

## Getting Started

### Prerequisites

- Python 3.10+
- Google Cloud account with BigQuery access
- `gcloud` CLI authenticated
- Node.js 18+ (for pptxgenjs, optional)

### 1. Clone & Install

```bash
git clone https://github.com/YOUR_ORG/dsai4-m2-t2-citycycle-c.git
cd dsai4-m2-t2-citycycle-c

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env — add your GCP project ID, BQ dataset names, etc.
# NEVER commit .env to Git
```

### 3. Run with Mock Data First (Recommended)

Before touching BigQuery's live data, validate the full pipeline with local mock data:

```bash
# Generate mock data
python dashboard/utils/mock_data_generator.py

# Load mock CSV into BigQuery (raw schema)
python ingestion/load_mock.py --mode=mock

# Run dbt transformations
cd transform && dbt run --target dev

# Run quality checks
python quality/run_ge_checks.py

# Launch dashboard
streamlit run dashboard/app.py
```

### 4. Run Full Pipeline (Real Data)

Once validated on mock data, switch to live ingestion:

```bash
# Meltano ingest from BQ public dataset
cd ingestion && meltano run tap-bigquery target-bigquery

# Then continue with dbt + GE as above

# Or run full Dagster pipeline
dagster dev -f orchestration/workspace.yaml
```

---

## Mock Data Strategy

### Why Mock Data First?

BigQuery's free tier provides **1 TB of query processing per month**. The `cycle_hire` table has **83 million rows**. A single unguarded `SELECT *` could consume the entire monthly quota instantly.

### Our Approach

| Risk | Mitigation |
|------|-----------|
| Full-table scan on `cycle_hire` | `LIMIT` clauses on all dev queries; partitioned by `hire_date` |
| Accidental `SELECT *` | dbt `+limit` macro in dev profile; BQ slot quota set |
| Exceeding 1 TB free tier | Dry-run cost estimates before every query; budget alert at 80% |
| Development iteration cost | All development runs against `data/mock/` CSV files |
| CI/CD test cost | GitHub Actions uses mock data only; no live BQ calls in CI |

### Mock Data Schema

The mock data mirrors the exact schema of the public BigQuery tables:

```
cycle_hire_mock.csv    → bike_id, rental_id, duration, start_date,
                         start_station_id, start_station_name,
                         end_date, end_station_id, end_station_name
cycle_stations_mock.csv → id, install_date, installed, latitude,
                          locked, longitude, name, nbdocks,
                          temporary, terminal_name
```

---

## Pipeline Walkthrough

### 1. Ingestion (Meltano)

Meltano uses the **Singer protocol** (tap → target) to extract data from BigQuery and load it into the raw dataset.

- **tap-bigquery**: Reads from `bigquery-public-data.london_bicycles`
- **target-bigquery**: Writes to your project's `raw` dataset
- Supports full refresh and incremental loads (state-based on `start_date`)

```bash
meltano run tap-bigquery target-bigquery
```

### 2. Data Warehouse Design

Star schema optimised for ride analytics and rebalancing queries:

**Fact Table:**
- `fact_rides` — one row per ride: duration, start/end station FK, date FK, hour, day-of-week

**Dimension Tables:**
- `dim_stations` — station metadata: name, location (lat/lon), dock capacity, zone
- `dim_date` — date spine: year, month, week, is_weekend, is_holiday (UK bank holidays)
- `dim_duration` — banded ride durations (short/medium/long/extended)

### 3. ELT Transformation (dbt)

```
raw.cycle_hire
    └── stg_cycle_hire        (cast types, rename columns, parse timestamps)
        └── int_rides_enriched (join stations, add peak_hour_flag, duration_band)
            └── fact_rides     (final fact table, add is_station_imbalanced flag)

raw.cycle_stations
    └── stg_cycle_stations    (clean nulls, add zone via lat/lon lookup)
        └── dim_stations       (final dimension, add capacity_tier)
```

Derived columns generated in dbt:
- `ride_duration_minutes` — `TIMESTAMP_DIFF(end_date, start_date, MINUTE)`
- `peak_hour_flag` — 1 if 07:00–09:00 or 17:00–19:00, else 0
- `is_station_imbalanced` — 1 if net outflow > 20% over rolling 7-day window
- `weekly_demand_index` — normalised ride count relative to station capacity

### 4. Data Quality (Great Expectations)

Two checkpoint stages:

**Post-ingest checkpoint** (`raw.*`):
- `rental_id` not null, unique
- `start_date` > '2010-01-01'
- `duration` between 60 and 86400 seconds
- `start_station_id` in valid station list

**Post-transform checkpoint** (`fact_rides`, `dim_stations`):
- No orphan station FK references
- `ride_duration_minutes` between 1 and 1440
- `is_station_imbalanced` only 0 or 1
- Null rate < 5% on all key columns

Results are published as HTML data docs.

### 5. Analysis & ML

#### EDA (notebooks)
- Monthly and hourly ride trends
- Top 20 most-used start/end stations
- Station-level imbalance detection (net flow heatmap)
- Customer segmentation: commuter vs casual (duration + time patterns)

#### Demand Forecasting Model
- **Features**: hour_of_day, day_of_week, is_weekend, is_holiday, station_id (encoded), rolling_7d_avg, season
- **Target**: `ride_count` per station per hour (next 24h)
- **Models tested**: RandomForest, XGBoost, LinearRegression (baseline)
- **Metric**: RMSE on 20% holdout; MAE for operational thresholds

### 6. Orchestration (Dagster)

```
Daily cron: 02:00 UTC
│
├── meltano_ingest_asset       (Meltano run, retry x3)
│   └── ge_post_ingest_asset   (Great Expectations checkpoint)
│       └── dbt_run_asset      (dbt run + dbt test)
│           └── ge_post_transform_asset
│               ├── ml_train_asset     (retrain model if new data)
│               └── dashboard_refresh  (update Streamlit cache)
```

On failure at any stage: pipeline halts, Slack alert sent to #citycycle-data-ops.

### 7. Dashboards

#### Streamlit (Operational)
- **Overview**: Daily ride KPIs, imbalance score, fleet utilisation
- **Station Map**: Pydeck geospatial map of all 795 stations, colour-coded by imbalance severity
- **Rebalancing**: Ranked list of stations needing intervention, with predicted demand delta
- **Forecast**: 24h demand forecast per station with confidence intervals

#### Looker Studio (Executive)
- Connected directly to BigQuery `marts.*` dataset
- KPI scorecard: total rides, avg duration, peak utilisation, rebalancing interventions
- Scheduled weekly PDF email to operations leadership

---

## Key Findings (Mock Data)

> These findings are based on **synthetic mock data** that mirrors the shape and distribution of the real London Bicycles dataset. They will be updated with actual findings once the live pipeline runs.

| Metric | Mock Value | Insight |
|--------|-----------|---------|
| Avg ride duration | 18.4 min | Primarily short-hop commuter trips |
| Peak demand hours | 08:00 & 17:30 | Classic commuter double peak |
| Top imbalanced stations | Waterloo, King's Cross, Liverpool St | Major transit hubs net-export bikes AM |
| Imbalanced station rate | ~23% of stations | 1 in 4 stations needs daily rebalancing |
| Forecast model RMSE | 4.2 rides/hr | Within operational planning threshold |

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|-----------|
| BigQuery free tier exceeded | Medium | High | Mock data dev; LIMIT guards; dry-run estimates; budget alerts |
| Meltano tap-bigquery schema drift | Low | Medium | dbt schema tests; GE not-null/type checks catch regressions |
| Long BQ query runtime in CI | Medium | Medium | CI uses mock CSV only; no live BQ in GitHub Actions |
| ML model staleness | Medium | Medium | Dagster daily retrain asset; model version tracking |
| Dashboard downtime | Low | Low | Streamlit caches last-good result; graceful error states |
| Credentials leaked to Git | Low | Critical | .gitignore covers all credential patterns; .env.example only |

---

## Contributing

1. Fork and create a feature branch: `git checkout -b feat/your-feature`
2. Develop against mock data only (`--target dev` in dbt)
3. Run `dbt test` before committing
4. Open a PR against `main` — CI will run linting and mock-data tests
5. Never commit `.env`, `profiles.yml`, or any `*keyfile*.json`

---

*Built for DSAI Module 2 Project — CityCycle Team C*
