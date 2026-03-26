"""
dashboard/pages/05_scenario.py
================================
Scenario planning page — combines station map, rebalancing table,
and demand forecast into a single guided operational flow.

Guides the user through:
1. Date selection
2. Folium map showing critical/high stations
3. Click-driven station detail table
4. Crew run calculator
5. Action statement with response time
"""

import os
from pathlib import Path
from datetime import date

import numpy as np
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
MOCK_DIR = ROOT / "data" / "mock"
MODEL_PATH = ROOT / "ml" / "models" / "demand_model.pkl"
PROJECT = os.environ.get("GCP_PROJECT_ID", "citycycle-dsai4")
DATASET = "citycycle_dev_marts"
DATE_FROM = "2020-01-01"
DATE_TO = "2023-01-15"
STATIONS_PER_RUN = 15

try:
    import folium
    from streamlit_folium import st_folium

    HAS_FOLIUM = True
except ImportError:
    HAS_FOLIUM = False


# ── Data loaders ──────────────────────────────────────────────────
@st.cache_data(ttl=1800)
def load_scenario_data(use_mock, date_from, date_to):
    """Load station imbalance data for the selected date range."""
    if use_mock:
        stations = pd.read_csv(MOCK_DIR / "cycle_stations_mock.csv")
        rides = pd.read_csv(
            MOCK_DIR / "cycle_hire_mock.csv", parse_dates=["start_date"]
        )
        dep = rides.groupby("start_station_id").size().reset_index(name="departures")
        arr = (
            rides.groupby("end_station_id")
            .size()
            .reset_index(name="arrivals")
            .rename(columns={"end_station_id": "start_station_id"})
        )
        flow = dep.merge(arr, on="start_station_id", how="outer").fillna(0)
        flow["net_flow"] = flow["departures"] - flow["arrivals"]
        flow["total"] = flow["departures"] + flow["arrivals"]
        flow["imb_score"] = flow["net_flow"].abs() / flow["total"].clip(lower=1)
        df = stations.merge(
            flow.rename(columns={"start_station_id": "id"}), on="id", how="left"
        ).fillna(0)
        df = df.rename(columns={"name": "station", "nbdocks": "nb_docks"})
        df["lat"] = df["latitude"]
        df["lon"] = df["longitude"]
    else:
        from sqlalchemy import create_engine, text

        engine = create_engine(f"bigquery://{PROJECT}/{DATASET}")
        sql = f"""
            SELECT
                start_station_name                              AS station,
                start_zone                                      AS zone,
                start_lat                                       AS lat,
                start_lon                                       AS lon,
                start_nb_docks                                  AS nb_docks,
                ROUND(AVG(start_station_imbalance_score), 3)   AS imb_score,
                ROUND(AVG(start_station_net_flow), 1)          AS net_flow,
                start_station_imbalance_direction               AS imb_direction,
                COUNT(*)                                        AS total_rides
            FROM `{PROJECT}.{DATASET}.fact_rides`
            WHERE hire_date BETWEEN '{date_from}' AND '{date_to}'
            GROUP BY 1, 2, 3, 4, 5, 8
            HAVING COUNT(*) > 50
            ORDER BY imb_score DESC
        """
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)

    # Derived fields
    df["action"] = np.where(
        df["net_flow"] > 2,
        "DELIVER BIKES",
        np.where(df["net_flow"] < -2, "COLLECT BIKES", "MONITOR"),
    )
    df["priority"] = pd.cut(
        df["imb_score"],
        bins=[-0.01, 0.10, 0.18, 0.25, 1.01],
        labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
    ).astype(str)

    # Colour helpers
    def to_hex(score):
        if score >= 0.25:
            return "#9a031e"  # CRITICAL
        if score >= 0.18:
            return "#e36414"  # HIGH
        if score >= 0.10:
            return "#f6bd60"  # MEDIUM
        return "#a7c957"  # LOW

    df["hex_colour"] = df["imb_score"].apply(to_hex)
    return df.sort_values("imb_score", ascending=False).reset_index(drop=True)


@st.cache_data(ttl=1800)
def get_forecast(station_id: int) -> pd.DataFrame:
    hours = list(range(24))
    if MODEL_PATH.exists():
        import joblib

        model = joblib.load(MODEL_PATH)
        today = pd.Timestamp.now()
        features = pd.DataFrame(
            {
                "hour": hours,
                "day_of_week": [today.dayofweek] * 24,
                "is_weekend": [int(today.dayofweek >= 5)] * 24,
                "is_holiday": [0] * 24,
                "season": [1] * 24,
                "start_station_id": [station_id] * 24,
                "rolling_7d_avg": [50.0] * 24,
            }
        )[
            [
                "hour",
                "day_of_week",
                "is_weekend",
                "is_holiday",
                "season",
                "start_station_id",
                "rolling_7d_avg",
            ]
        ]
        predicted = np.maximum(model.predict(features), 0)
    else:
        HOUR_WEIGHTS = np.array(
            [
                0.3,
                0.15,
                0.10,
                0.10,
                0.20,
                0.50,
                1.20,
                3.80,
                4.50,
                2.50,
                1.80,
                1.60,
                2.20,
                1.80,
                1.60,
                1.70,
                2.40,
                4.20,
                3.60,
                2.20,
                1.50,
                1.10,
                0.70,
                0.45,
            ]
        )
        rng = np.random.default_rng(station_id % 1000)
        scale = rng.uniform(0.5, 2.0)
        predicted = np.maximum(HOUR_WEIGHTS * scale * 4.5 + rng.normal(0, 0.5, 24), 0)
    ci = predicted * 0.25
    return pd.DataFrame(
        {
            "hour": hours,
            "forecast": np.round(predicted, 1),
            "lower_ci": np.round(np.maximum(predicted - ci, 0), 1),
            "upper_ci": np.round(predicted + ci, 1),
            "is_peak": [h in [7, 8, 17, 18] for h in hours],
        }
    )


# ════════════════════════════════════════════════════════════════
# PAGE LAYOUT
# ════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Scenario · CityCycle", page_icon="🎯", layout="wide")

st.title("🎯 Operational Scenario Planner")
st.markdown(
    "A guided view for operations teams — identify which stations need attention, "
    "plan crew runs, and check demand forecasts before dispatching."
)
st.markdown("---")

use_mock = st.sidebar.toggle("Use mock data", value=True)

# ── STEP 1: Date selection ────────────────────────────────────────
st.subheader("Step 1 — Select Date Range")
st.markdown(
    "Choose the period you want to analyse. The map and table below will reflect "
    "imbalance patterns for the selected dates."
)

col_d1, col_d2, col_d3 = st.columns([2, 2, 3])
with col_d1:
    if use_mock:
        sel_from = st.date_input(
            "From date",
            value=date(2022, 1, 1),
            min_value=date(2020, 1, 1),
            max_value=date(2023, 1, 15),
        )
    else:
        sel_from = st.date_input(
            "From date",
            value=date(2022, 1, 1),
            min_value=date(2020, 1, 1),
            max_value=date(2023, 1, 15),
        )
with col_d2:
    if use_mock:
        sel_to = st.date_input(
            "To date",
            value=date(2022, 12, 31),
            min_value=date(2020, 1, 1),
            max_value=date(2023, 1, 15),
        )
    else:
        sel_to = st.date_input(
            "To date",
            value=date(2022, 12, 31),
            min_value=date(2020, 1, 1),
            max_value=date(2023, 1, 15),
        )
with col_d3:
    st.info(
        f"📅 Analysing: **{sel_from.strftime('%d %b %Y')}** → "
        f"**{sel_to.strftime('%d %b %Y')}**  "
        f"({(sel_to - sel_from).days + 1} days)"
    )

date_from_str = str(sel_from)
date_to_str = str(sel_to)

df = load_scenario_data(use_mock, date_from_str, date_to_str)

# Filter to stations needing action only (CRITICAL + HIGH by default)
critical_high = df[df["priority"].isin(["CRITICAL", "HIGH"])].copy()
needs_action = df[df["action"].isin(["DELIVER BIKES", "COLLECT BIKES"])].copy()

st.markdown("---")

# ── STEP 2: MAP ───────────────────────────────────────────────────
st.subheader("Step 2 — Identify Stations in the Danger Zone")
st.markdown(
    "The map below shows stations that are **CRITICAL** or **HIGH** priority — "
    "these are in the danger zone and require crew attention. "
    "**●** = CRITICAL · HIGH · MEDIUM · LOW — colour-coded by priority. "
    "Click any marker to see station details. "
    "Scores shown are **averages across the selected date range** — stations appearing here are chronically imbalanced, not just occasional anomalies."
)

if not HAS_FOLIUM:
    st.warning("Install folium: `pip install folium streamlit-folium`")
else:
    map_data = critical_high if len(critical_high) > 0 else df.head(20)
    map_data = map_data.dropna(subset=["lat", "lon"]).reset_index(drop=True)
    centre_lat = map_data["lat"].mean() if len(map_data) > 0 else 51.508
    centre_lon = map_data["lon"].mean() if len(map_data) > 0 else -0.128

    m = folium.Map(
        location=[centre_lat, centre_lon],
        zoom_start=12,
        tiles="CartoDB positron",
    )

    for _, row in map_data.iterrows():
        action_icon = {
            "DELIVER BIKES": "arrow-down",
            "COLLECT BIKES": "arrow-up",
            "MONITOR": "pause",
        }.get(row["action"], "info-sign")

        dock_col = next((c for c in ["nb_docks", "nbdocks", "docks_count"] if c in row.index), None)
        nb_docks = int(row[dock_col]) if dock_col and not pd.isna(row[dock_col]) else 0

        popup_html = f"""
        <div style="font-family: Arial, sans-serif; min-width: 220px;">
            <h4 style="margin:0 0 8px 0; color:#0F172A; font-size:14px;">
                {row['station']}
            </h4>
            <table style="width:100%; font-size:12px; border-collapse:collapse;">
                <tr style="background:#F1F5F9;">
                    <td style="padding:4px 6px;"><b>Priority</b></td>
                    <td style="padding:4px 6px; color:{row['hex_colour']};
                        font-weight:bold;">{row['priority']}</td>
                </tr>
                <tr>
                    <td style="padding:4px 6px;"><b>Action Required</b></td>
                    <td style="padding:4px 6px; font-weight:bold;
                        color:{'#EF4444' if row['action']=='DELIVER BIKES' else '#3B82F6'};">
                        {row['action']}
                    </td>
                </tr>
                <tr style="background:#F1F5F9;">
                    <td style="padding:4px 6px;"><b>Imbalance Score</b></td>
                    <td style="padding:4px 6px;">{row['imb_score']:.3f}</td>
                </tr>
                <tr>
                    <td style="padding:4px 6px;"><b>Net Flow</b></td>
                    <td style="padding:4px 6px;">{int(row['net_flow']):+d} bikes/day</td>
                </tr>
                <tr style="background:#F1F5F9;">
                    <td style="padding:4px 6px;"><b>Docks Available</b></td>
                    <td style="padding:4px 6px;">{nb_docks} docks</td>
                </tr>
            </table>
        </div>
        """

        folium.CircleMarker(
            location=[row["lat"], row["lon"]],
            radius=12,
            color=row["hex_colour"],
            fill=True,
            fill_color=row["hex_colour"],
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=f"{row['station']} — {row['priority']} — {row['action']}",
        ).add_to(m)

    map_result = st_folium(
        m,
        use_container_width=True,
        height=520,
        returned_objects=["last_object_clicked"],
    )

    st.markdown(
        '<span style="color:#9a031e; font-size:14px;">●</span> CRITICAL (score ≥ 0.25) &nbsp;·&nbsp; '
        '<span style="color:#e36414; font-size:14px;">●</span> HIGH (score ≥ 0.18) &nbsp;·&nbsp; '
        'Click any marker for full details',
        unsafe_allow_html=True,
    )

st.markdown("---")

# ── STEP 3: STATION TABLE ─────────────────────────────────────────
st.subheader("Step 3 — Station Detail Table")
st.markdown(
    "All stations in the danger zone are listed below, ranked by imbalance score. "
    "Use this table to plan the order of crew visits."
)

# Priority filter
col_p1, col_p2 = st.columns([2, 3])
with col_p1:
    show_priority = st.multiselect(
        "Show priority levels",
        ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        default=["CRITICAL", "HIGH"],
    )
with col_p2:
    show_action = st.multiselect(
        "Show action type",
        ["DELIVER BIKES", "COLLECT BIKES", "MONITOR"],
        default=["DELIVER BIKES", "COLLECT BIKES"],
    )

table_df = df[
    df["priority"].isin(show_priority) & df["action"].isin(show_action)
].copy()

# Highlight selected station from map click
selected_station = None
if HAS_FOLIUM and map_result and map_result.get("last_object_clicked"):
    clicked = map_result["last_object_clicked"]
    if clicked:
        clicked_lat = clicked.get("lat")
        clicked_lng = clicked.get("lng")
        if clicked_lat and clicked_lng:
            match = df[
                (df["lat"].round(4) == round(clicked_lat, 4))
                & (df["lon"].round(4) == round(clicked_lng, 4))
            ]
            if len(match) > 0:
                selected_station = match.iloc[0]["station"]
                st.success(f"📍 Selected from map: **{selected_station}**")

display_cols = ["station", "priority", "action", "imb_score", "net_flow", "nb_docks"]
# Normalise dock column name in case rename did not apply
for col in ["nbdocks", "docks_count"]:
    if col in table_df.columns and "nb_docks" not in table_df.columns:
        table_df = table_df.rename(columns={col: "nb_docks"})
for col in ["nbdocks", "docks_count"]:
    if col in map_data.columns and "nb_docks" not in map_data.columns:
        map_data = map_data.rename(columns={col: "nb_docks"})
avail_cols = [c for c in display_cols if c in table_df.columns]

st.dataframe(
    table_df[avail_cols].rename(
        columns={
            "station": "Station",
            "priority": "Priority",
            "action": "Action Required",
            "imb_score": "Imbalance Score",
            "net_flow": "Net Flow (bikes/day)",
            "nb_docks": "Docks Available",
        }
    ),
    use_container_width=True,
    hide_index=True,
    column_config={
        "Imbalance Score": st.column_config.ProgressColumn(
            "Imbalance Score", min_value=0, max_value=1, format="%.3f"
        ),
    },
)

st.markdown("---")

# ── STEP 4: CREW RUN CALCULATOR ───────────────────────────────────
st.subheader("Step 4 — Crew Run Plan")

n_critical = len(df[df["priority"] == "CRITICAL"])
n_high = len(df[df["priority"] == "HIGH"])
n_action = len(table_df)
n_deliver = len(table_df[table_df["action"] == "DELIVER BIKES"])
n_collect = len(table_df[table_df["action"] == "COLLECT BIKES"])
crew_runs = max(1, round(n_action / STATIONS_PER_RUN))

col_c1, col_c2, col_c3, col_c4, col_c5 = st.columns(5)
col_c1.metric("CRITICAL stations", n_critical, delta="immediate action")
col_c2.metric("HIGH stations", n_high)
col_c3.metric("Need delivery", n_deliver, delta="DELIVER BIKES")
col_c4.metric("Need collection", n_collect, delta="COLLECT BIKES")
col_c5.metric(
    "Crew runs needed",
    crew_runs,
    delta=f"{STATIONS_PER_RUN} stations per run",
)

st.info(
    f"ℹ️ Each crew run covers **{STATIONS_PER_RUN} stations**. "
    f"With **{n_action} stations** requiring action, you need a minimum of "
    f"**{crew_runs} crew run{'s' if crew_runs > 1 else ''}** today. "
    f"Prioritise CRITICAL stations first, then HIGH."
)

st.markdown("---")

# ── STEP 5: ACTION STATEMENT ──────────────────────────────────────
st.subheader("Step 5 — Today's Action Statement")

# Reflects Step 3 filters — updates dynamically when priority/action filter changes
action_df = table_df.copy()
deliver_df = action_df[action_df["action"] == "DELIVER BIKES"].sort_values(
    "imb_score", ascending=False
)
collect_df = action_df[action_df["action"] == "COLLECT BIKES"].sort_values(
    "imb_score", ascending=False
)

if len(action_df) > 0:
    worst_score = action_df.iloc[0]["imb_score"]
    if worst_score >= 0.40:
        hours = 2
        urgency = "immediately"
    elif worst_score >= 0.25:
        hours = 4
        urgency = "urgently"
    else:
        hours = 6
        urgency = "today"

    st.error(
        f"🚨 **Action required {urgency} — {len(action_df)} stations need attention "
        f"({len(deliver_df)} deliveries · {len(collect_df)} collections). "
        f"Dispatch {crew_runs} crew run{'s' if crew_runs > 1 else ''} "
        f"and clear all flagged stations within {hours} hours.**"
    )

    if len(deliver_df) > 0:
        st.markdown(f"**⬇️ DELIVER BIKES to {len(deliver_df)} station(s):**")
        for _, row in deliver_df.iterrows():
            icon = "🔴" if row["priority"] == "CRITICAL" else "🟠"
            dock_col = next(
                (c for c in ["nb_docks", "nbdocks", "docks_count"] if c in row.index),
                None,
            )
            docks = int(row[dock_col]) if dock_col and not pd.isna(row[dock_col]) else 0
            st.markdown(
                f"{icon} **{row['station']}** — score {row['imb_score']:.2f} · "
                f"net flow +{int(row['net_flow'])} bikes/day · {docks} docks"
            )

    if len(collect_df) > 0:
        st.markdown(f"**⬆️ COLLECT BIKES from {len(collect_df)} station(s):**")
        for _, row in collect_df.iterrows():
            icon = "🔴" if row["priority"] == "CRITICAL" else "🟠"
            dock_col = next(
                (c for c in ["nb_docks", "nbdocks", "docks_count"] if c in row.index),
                None,
            )
            docks = int(row[dock_col]) if dock_col and not pd.isna(row[dock_col]) else 0
            st.markdown(
                f"{icon} **{row['station']}** — score {row['imb_score']:.2f} · "
                f"net flow {int(row['net_flow'])} bikes/day · {docks} docks"
            )
else:
    st.success(
        "✅ **No urgent action required today.** "
        "All stations are within acceptable imbalance thresholds. "
        "Continue monitoring — re-check before the AM peak (07:00–09:00)."
    )

st.markdown("---")

# ── STEP 6: DEMAND FORECAST FOR SELECTED STATION ─────────────────
st.subheader("Step 6 — Demand Forecast")
st.markdown(
    "Select any station to see the 24-hour demand forecast. "
    "Use this to time crew dispatch — arrive before the peak, not during it."
)

forecast_options = df["station"].tolist()
default_idx = 0
if selected_station and selected_station in forecast_options:
    default_idx = forecast_options.index(selected_station)

col_fs1, col_fs2 = st.columns([3, 2])
with col_fs1:
    forecast_station = st.selectbox(
        "Station to forecast",
        forecast_options,
        index=default_idx,
    )
with col_fs2:
    st.caption(
        f"🗓 Forecast date: **{pd.Timestamp.now().strftime('%A, %d %B %Y')}**  \n"
        "Based on historical patterns from 2020–2023 training data."
    )

# Get station ID
station_row = df[df["station"] == forecast_station]
if len(station_row) > 0:
    sid = int(station_row.iloc[0].get("id", station_row.index[0]))
else:
    sid = 1

forecast = get_forecast(sid)

# Chart
st.area_chart(
    forecast.set_index("hour")[["forecast", "lower_ci", "upper_ci"]],
    height=240,
    color=["#14B8A6", "#0D948840", "#0D948820"],
)
st.caption(
    "Teal = forecast  ·  Shaded = ±25% planning buffer  ·  "
    "Peak hours 07–09 and 17–19 typically show highest demand"
)

# Peak hours summary
peak_total = forecast[forecast["is_peak"]]["forecast"].sum()
offpeak_total = forecast[~forecast["is_peak"]]["forecast"].sum()
next_peak_hour = forecast[forecast["is_peak"]]["hour"].min()
now_hour = pd.Timestamp.now().hour
hours_to_peak = (next_peak_hour - now_hour) % 24

col_f1, col_f2, col_f3 = st.columns(3)
col_f1.metric("Predicted peak demand", f"{peak_total:.0f} rides")
col_f2.metric("Off-peak demand", f"{offpeak_total:.0f} rides")
col_f3.metric("Next peak in", f"{hours_to_peak}h (at {next_peak_hour:02d}:00)")

# Dispatch recommendation
station_action = "MONITOR"
station_net_flow = 0
if len(station_row) > 0:
    station_action = station_row.iloc[0].get("action", "MONITOR")
    station_net_flow = float(station_row.iloc[0].get("net_flow", 0))

if station_action != "MONITOR":
    dispatch_window = max(1, hours_to_peak - 1)
    bikes_needed = max(1, round(abs(station_net_flow)))
    action_lower = (
        "deliver bikes to"
        if station_action == "DELIVER BIKES"
        else "collect bikes from"
    )
    bike_verb = "deliver" if station_action == "DELIVER BIKES" else "collect"
    st.info(
        f"⏱ **Dispatch recommendation:** "
        f"To **{action_lower} {forecast_station}** before peak demand at "
        f"**{next_peak_hour:02d}:00**, dispatch crew within the next "
        f"**{dispatch_window} hour{'s' if dispatch_window > 1 else ''}** "
        f"and {bike_verb} approximately **{bikes_needed} bike{'s' if bikes_needed > 1 else ''}**."
    )

if use_mock:
    st.caption("Data source: mock CSV — toggle off in sidebar for live BigQuery")
else:
    st.caption(
        f"Data source: {PROJECT}.{DATASET}.fact_rides  ·  "
        f"Period: {date_from_str} → {date_to_str}"
    )
