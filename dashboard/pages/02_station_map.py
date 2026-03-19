"""
dashboard/pages/02_station_map.py
===================================
Geospatial map of all 795 CityCycle docking stations.
Colour-coded by rebalancing urgency (green → amber → red).
Uses pydeck for the map layer.

Install: pip install pydeck
"""

from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
MOCK_DIR = ROOT / "data" / "mock"

st.set_page_config(page_title="Station Map · CityCycle", page_icon="🗺", layout="wide")
st.title("🗺 Station Map")
st.markdown("All 795 docking stations, colour-coded by rebalancing priority.")

# ── Try to import pydeck ──────────────────────────────────────────
try:
    import pydeck as pdk

    HAS_PYDECK = True
except ImportError:
    HAS_PYDECK = False
    st.warning(
        "pydeck not installed. Install with: `pip install pydeck`\n\n"
        "Showing fallback table view instead."
    )

# ── Data loader ───────────────────────────────────────────────────


@st.cache_data(ttl=3600)
def load_station_map_data() -> pd.DataFrame:
    stations = pd.read_csv(MOCK_DIR / "cycle_stations_mock.csv")
    rides = pd.read_csv(MOCK_DIR / "cycle_hire_mock.csv", parse_dates=["start_date"])

    # Compute imbalance per station
    departures = rides.groupby("start_station_id").size().reset_index(name="departures")
    arrivals = rides.groupby("end_station_id").size().reset_index(name="arrivals")
    arrivals = arrivals.rename(columns={"end_station_id": "start_station_id"})

    flow = departures.merge(arrivals, on="start_station_id", how="outer").fillna(0)
    flow["net_flow"] = flow["departures"] - flow["arrivals"]
    flow["total_moves"] = flow["departures"] + flow["arrivals"]
    flow["imb_score"] = flow["net_flow"].abs() / flow["total_moves"].clip(lower=1)
    flow["imb_direction"] = np.where(
        flow["net_flow"] > 0,
        "draining",
        np.where(flow["net_flow"] < 0, "filling", "balanced"),
    )

    df = stations.merge(
        flow.rename(columns={"start_station_id": "id"}), on="id", how="left"
    ).fillna(
        {
            "imb_score": 0,
            "departures": 0,
            "arrivals": 0,
            "net_flow": 0,
            "imb_direction": "balanced",
        }
    )

    # Rebalancing priority tier
    df["priority"] = pd.cut(
        df["imb_score"],
        bins=[-0.01, 0.1, 0.3, 0.5, 1.01],
        labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
    ).astype(str)

    # RGB colour for pydeck (green → amber → red)
    def score_to_rgb(score: float) -> list:
        if score < 0.1:
            return [34, 197, 94]  # green
        elif score < 0.3:
            return [234, 179, 8]  # amber
        elif score < 0.5:
            return [249, 115, 22]  # orange
        else:
            return [239, 68, 68]  # red

    df["colour"] = df["imb_score"].apply(score_to_rgb)
    df["radius"] = (df["imb_score"] * 200 + 60).clip(upper=300)

    return df


df = load_station_map_data()

# ── Sidebar filters ───────────────────────────────────────────────
with st.sidebar:
    st.subheader("Filters")
    priority_filter = st.multiselect(
        "Priority",
        options=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
        default=["HIGH", "CRITICAL"],
    )
    direction_filter = st.multiselect(
        "Imbalance direction",
        options=["draining", "filling", "balanced"],
        default=["draining", "filling"],
    )

filtered = df[
    df["priority"].isin(priority_filter) & df["imb_direction"].isin(direction_filter)
]

# ── Summary stats ─────────────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Stations", len(df))
c2.metric("Filtered Stations", len(filtered))
c3.metric(
    "Critical",
    len(df[df["priority"] == "CRITICAL"]),
    delta="need urgent attention",
    delta_color="inverse",
)
c4.metric("Draining Now", len(df[df["imb_direction"] == "draining"]))

st.markdown("---")

# ── Map ───────────────────────────────────────────────────────────
if HAS_PYDECK and len(filtered) > 0:
    view = pdk.ViewState(
        latitude=filtered["latitude"].mean(),
        longitude=filtered["longitude"].mean(),
        zoom=11,
        pitch=30,
    )

    scatter_layer = pdk.Layer(
        "ScatterplotLayer",
        data=filtered,
        get_position=["longitude", "latitude"],
        get_color="colour",
        get_radius="radius",
        pickable=True,
        opacity=0.85,
        stroked=True,
        line_width_min_pixels=1,
    )

    tooltip = {
        "html": """
            <b>{name}</b><br/>
            Priority: <b>{priority}</b><br/>
            Direction: {imb_direction}<br/>
            Net flow: {net_flow:.0f}<br/>
            Imbalance score: {imb_score:.2f}<br/>
            Docks: {nbdocks}
        """,
        "style": {
            "backgroundColor": "#1E293B",
            "color": "#F8FAFC",
            "fontSize": "13px",
            "padding": "8px",
        },
    }

    deck = pdk.Deck(
        layers=[scatter_layer],
        initial_view_state=view,
        tooltip=tooltip,
        map_style="mapbox://styles/mapbox/light-v11",
    )
    st.pydeck_chart(deck, use_container_width=True)
    st.caption(
        "🟢 LOW  🟡 MEDIUM  🟠 HIGH  🔴 CRITICAL  "
        "| Radius = imbalance severity  "
        "| Click a station for details"
    )

elif not HAS_PYDECK:
    # Fallback: show lat/lon scatter via st.map
    st.map(
        filtered[["latitude", "longitude"]].rename(
            columns={"latitude": "lat", "longitude": "lon"}
        ),
        use_container_width=True,
    )

else:
    st.info("No stations match the current filters.")

# ── Detailed table ────────────────────────────────────────────────
st.subheader(f"Station Details — {len(filtered)} stations")
display_cols = [
    "name",
    "priority",
    "imb_direction",
    "imb_score",
    "net_flow",
    "departures",
    "arrivals",
    "nbdocks",
]
st.dataframe(
    filtered[display_cols]
    .sort_values("imb_score", ascending=False)
    .rename(
        columns={
            "name": "Station",
            "priority": "Priority",
            "imb_direction": "Direction",
            "imb_score": "Imbalance Score",
            "net_flow": "Net Flow",
            "departures": "Departures",
            "arrivals": "Arrivals",
            "nbdocks": "Docks",
        }
    ),
    use_container_width=True,
    hide_index=True,
)
