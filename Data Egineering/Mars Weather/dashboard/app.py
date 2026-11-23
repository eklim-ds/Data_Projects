"""Mars Surface Weather Desk: a simple dashboard over the dbt marts.

Run with:  make dashboard      (or: PYTHONPATH=src streamlit run dashboard/app.py)
"""
from __future__ import annotations

import plotly.graph_objects as go
import streamlit as st

from mars_weather import queries
from mars_weather.config import Settings

STATUS_COLORS = {"GO": "#2e9e5b", "CAUTION": "#e0a800", "NO_GO": "#c0392b", "NO_DATA": "#7f8c8d"}

st.set_page_config(page_title="Mars Surface Weather Desk", layout="wide")


@st.cache_data(ttl=60, show_spinner=False)
def load(warehouse_path: str) -> dict:
    """Read everything the page needs in one go, then release the DuckDB file."""
    con = queries.connect(warehouse_path)
    try:
        briefing = queries.ops_briefing(con)
        return {
            "briefing": briefing,
            "audit": queries.ingestion_audit(con),
            "rose_all": queries.wind_rose(con),
            "rose_by_sol": {int(sol): queries.wind_rose(con, int(sol)) for sol in briefing["sol"]},
        }
    finally:
        con.close()


BADGE_COLORS = {"GO": "green", "CAUTION": "orange", "NO_GO": "red", "NO_DATA": "gray"}


def status_badge(status: str) -> str:
    """Streamlit colored-text markdown, e.g. ':green[**GO**]'."""
    return f":{BADGE_COLORS[status]}[**{status.replace('_', '-')}**]"


def wind_chart(df):
    fig = go.Figure()
    fig.add_bar(
        x=df["sol"], y=df["wind_max_ms"], name="Peak wind",
        marker_color=[STATUS_COLORS[s] for s in df["ops_status"]],
        customdata=df[["earth_date", "ops_status"]], hovertemplate="Sol %{x} (%{customdata[0]})<br>Peak %{y:.1f} m/s<br>%{customdata[1]}<extra></extra>",
    )
    fig.add_scatter(x=df["sol"], y=df["wind_avg_ms"], name="Average wind", mode="lines+markers", line={"color": "#34495e"})
    fig.add_hline(y=df["limit_caution_gust_ms"].iloc[0], line_dash="dot", line_color=STATUS_COLORS["CAUTION"], annotation_text="caution gust")
    fig.add_hline(y=df["limit_nogo_gust_ms"].iloc[0], line_dash="dot", line_color=STATUS_COLORS["NO_GO"], annotation_text="no-go gust")
    fig.update_layout(xaxis_title="Sol", yaxis_title="Wind speed (m/s)", xaxis={"type": "category"}, height=380, margin={"t": 20})
    return fig


def temperature_chart(df):
    fig = go.Figure()
    fig.add_scatter(x=df["sol"], y=df["temp_max_c"], name="Max", mode="lines+markers", line={"color": "#e67e22"})
    fig.add_scatter(x=df["sol"], y=df["temp_min_c"], name="Min", mode="lines+markers", line={"color": "#2980b9"}, fill="tonexty")
    fig.add_scatter(x=df["sol"], y=df["temp_avg_c"], name="Average", mode="lines", line={"color": "#34495e", "dash": "dash"})
    fig.update_layout(xaxis_title="Sol", yaxis_title="Air temperature (°C)", xaxis={"type": "category"}, height=380, margin={"t": 20})
    return fig


def pressure_chart(df):
    fig = go.Figure()
    fig.add_scatter(x=df["sol"], y=df["pressure_max_pa"], name="Max", mode="lines", line={"color": "#95a5a6"})
    fig.add_scatter(x=df["sol"], y=df["pressure_min_pa"], name="Min", mode="lines", line={"color": "#95a5a6"}, fill="tonexty")
    fig.add_scatter(x=df["sol"], y=df["pressure_avg_pa"], name="Average", mode="lines+markers", line={"color": "#8e44ad"})
    fig.update_layout(xaxis_title="Sol", yaxis_title="Pressure (Pa)", xaxis={"type": "category"}, height=380, margin={"t": 20})
    return fig


def wind_rose_chart(df):
    fig = go.Figure(
        go.Barpolar(
            theta=df["compass_degrees"], r=df["share_of_samples"] * 100, width=22.5 * 0.9,
            marker_color="#2980b9", customdata=df["compass_point"],
            hovertemplate="%{customdata}: %{r:.1f}% of samples<extra></extra>",
        )
    )
    fig.update_layout(
        polar={
            "angularaxis": {"direction": "clockwise", "rotation": 90, "tickmode": "array",
                            "tickvals": [0, 90, 180, 270], "ticktext": ["N", "E", "S", "W"]},
            "radialaxis": {"ticksuffix": "%"},
        },
        height=420, margin={"t": 30},
    )
    return fig


st.title("Mars Surface Weather Desk")
st.caption("Daily weather from NASA's InSight lander (Elysium Planitia) and a GO / CAUTION / NO-GO call for wind-sensitive surface work.")

settings = Settings.from_env()
try:
    data = load(str(settings.warehouse_path))
except queries.WarehouseNotReady as exc:
    st.warning(str(exc))
    st.stop()

briefing, audit = data["briefing"], data["audit"]
if briefing.empty:
    st.warning("The warehouse has no sols yet. Run `make pipeline`.")
    st.stop()

latest = briefing.iloc[-1]
last_run = audit.iloc[-1]
if last_run["new_sols"] == 0:
    st.info(
        f"The latest batch run returned {int(last_run['sols_returned'])} sols but none were new: NASA's feed has not advanced "
        f"(it currently ends at sol {int(last_run['max_sol'])}). The InSight mission has ended, so this is expected."
    )

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Latest sol", int(latest["sol"]), f"Earth date {latest['earth_date']:%Y-%m-%d}", delta_color="off")
c2.markdown(f"**Ops status**  \n{status_badge(latest['ops_status'])}")
c2.caption(latest["ops_status_reason"])
c3.metric("Avg temperature", f"{latest['temp_avg_c']:.1f} °C", f"{latest['temp_avg_c'] - latest['temp_avg_7sol_c']:+.1f} vs 7-sol avg", delta_color="off")
c4.metric("Avg wind", f"{latest['wind_avg_ms']:.1f} m/s", f"{latest['wind_avg_ms'] - latest['wind_avg_7sol_ms']:+.1f} vs 7-sol avg", delta_color="off")
c5.metric("Avg pressure", f"{latest['pressure_avg_pa']:.0f} Pa", None if latest["pressure_change_pa"] != latest["pressure_change_pa"] else f"{latest['pressure_change_pa']:+.1f} vs prev sol", delta_color="off")

tab_ops, tab_climate, tab_rose, tab_health = st.tabs(["Ops briefing", "Temperature & pressure", "Wind rose", "Pipeline health"])

with tab_ops:
    st.plotly_chart(wind_chart(briefing), width="stretch")
    st.dataframe(
        briefing[["sol", "earth_date", "ops_status", "ops_status_reason", "wind_avg_ms", "wind_max_ms", "temp_range_c"]]
        .sort_values("sol", ascending=False),
        hide_index=True, width="stretch",
    )

with tab_climate:
    left, right = st.columns(2)
    left.subheader("Air temperature")
    left.plotly_chart(temperature_chart(briefing), width="stretch")
    right.subheader("Surface pressure")
    right.plotly_chart(pressure_chart(briefing), width="stretch")

with tab_rose:
    choice = st.selectbox("Sol", ["All sols"] + [int(s) for s in briefing["sol"]][::-1])
    rose = data["rose_all"] if choice == "All sols" else data["rose_by_sol"][choice]
    st.plotly_chart(wind_rose_chart(rose), width="stretch")
    st.caption("Share of wind samples per compass direction. Bins with no samples show as empty, not as missing.")

with tab_health:
    st.write(
        "One row per batch run. NASA returns an overlapping rolling window of sols, so most sols arrive more than once; "
        "the warehouse keeps the latest copy of each."
    )
    st.dataframe(audit, hide_index=True, width="stretch")
