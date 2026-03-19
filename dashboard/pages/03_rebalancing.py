"""
dashboard/pages/03_rebalancing.py
===================================
Ranked rebalancing intervention list.
Shows which stations need bikes delivered or collected today,
sorted by urgency, with export to CSV for ops crew routing.
"""

from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
MOCK_DIR = ROOT / "data" / "mock"

st.set_page_config(page_title="Rebalancing · CityCycle", page_icon="⚖️", layout="wide")
st.title("⚖️ Rebalancing Priority")
st.markdown(
    "Stations ranked by urgency. Green = balanced. Red = needs immediate action."
)


@st.cache_data(ttl=1800)
def build_rebalancing_list() -> pd.DataFrame:
    stations = pd.read_csv(MOCK_DIR / "cycle_stations_mock.csv")
    rides = pd.read_csv(MOCK_DIR / "cycle_hire_mock.csv", parse_dates=["start_date"])

    departures = rides.groupby("start_station_id").size().reset_index(name="departures")
    arrivals = rides.groupby("end_station_id").size().reset_index(name="arrivals")
    arrivals = arrivals.rename(columns={"end_station_id": "start_station_id"})

    flow = departures.merge(arrivals, on="start_station_id", how="outer").fillna(0)
    flow["net_flow"] = flow["departures"] - flow["arrivals"]
    flow["total_moves"] = flow["departures"] + flow["arrivals"]
    flow["imb_score"] = flow["net_flow"].abs() / flow["total_moves"].clip(lower=1)

    df = stations.merge(
        flow.rename(columns={"start_station_id": "id"}), on="id", how="left"
    ).fillna(0)

    df["action_needed"] = np.where(
        df["net_flow"] > 2,
        "DELIVER BIKES",
        np.where(df["net_flow"] < -2, "COLLECT BIKES", "MONITOR"),
    )
    df["urgency_score"] = df["imb_score"]
    df["priority"] = pd.cut(
        df["imb_score"],
        bins=[-0.01, 0.1, 0.3, 0.5, 1.01],
        labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
    ).astype(str)

    return df.sort_values("urgency_score", ascending=False)


df = build_rebalancing_list()

# ── Filters ───────────────────────────────────────────────────────
col_f1, col_f2 = st.columns(2)
with col_f1:
    action_filter = st.multiselect(
        "Action needed",
        options=["DELIVER BIKES", "COLLECT BIKES", "MONITOR"],
        default=["DELIVER BIKES", "COLLECT BIKES"],
    )
with col_f2:
    priority_filter = st.multiselect(
        "Priority",
        options=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        default=["CRITICAL", "HIGH"],
    )

filtered = df[
    df["action_needed"].isin(action_filter) & df["priority"].isin(priority_filter)
]

# ── KPI row ───────────────────────────────────────────────────────
c1, c2, c3 = st.columns(3)
c1.metric("Stations needing action", len(filtered))
c2.metric(
    "Need bike delivery", len(filtered[filtered["action_needed"] == "DELIVER BIKES"])
)
c3.metric(
    "Need bike collection", len(filtered[filtered["action_needed"] == "COLLECT BIKES"])
)

st.markdown("---")

# ── Table ─────────────────────────────────────────────────────────
display = (
    filtered[
        [
            "name",
            "priority",
            "action_needed",
            "net_flow",
            "imb_score",
            "departures",
            "arrivals",
            "nbdocks",
            "latitude",
            "longitude",
        ]
    ]
    .rename(
        columns={
            "name": "Station",
            "priority": "Priority",
            "action_needed": "Action",
            "net_flow": "Net Flow",
            "imb_score": "Urgency Score",
            "departures": "Departures",
            "arrivals": "Arrivals",
            "nbdocks": "Docks",
            "latitude": "Lat",
            "longitude": "Lon",
        }
    )
    .head(50)
)

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "Urgency Score": st.column_config.ProgressColumn(
            "Urgency Score", min_value=0, max_value=1, format="%.2f"
        ),
        "Priority": st.column_config.TextColumn("Priority"),
    },
)

# ── Export ────────────────────────────────────────────────────────
csv = display.to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Export to CSV (ops crew routing)",
    data=csv,
    file_name="citycycle_rebalancing_plan.csv",
    mime="text/csv",
)
