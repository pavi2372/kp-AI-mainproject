import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from pos_ingest.db import engine

st.set_page_config(page_title="POS Intelligence Agent", layout="wide")

@st.cache_data
def load_data():
    agg = pd.read_sql_table("pos_agg_daily", engine, parse_dates=["dt"])
    alerts = pd.read_sql_table("pos_alerts", engine, parse_dates=["dt"])
    decisions = pd.read_sql_table("pos_sku_decisions", engine, parse_dates=["dt"])
    return agg, alerts, decisions

agg, alerts, decisions = load_data()

st.title("Trends Intelligent Agent – POS Data Intelligence")

col1, col2 = st.columns(2)
with col1:
    store = st.selectbox("Store", sorted(agg["store_id"].unique()))
with col2:
    sku = st.selectbox("SKU", sorted(agg["sku"].unique()))

filtered = agg[(agg["store_id"] == store) & (agg["sku"] == sku)]

st.subheader("Trending SKU – Daily Net Sales")
st.line_chart(filtered.set_index("dt")[["net_sales"]])

st.subheader("Alerts & Recommendations")
alerts_view = alerts[(alerts["store_id"] == store) & (alerts["sku"] == sku)]
decisions_view = decisions[(decisions["store_id"] == store) & (decisions["sku"] == sku)]

st.write("Alerts")
st.dataframe(alerts_view[["dt", "alert_type", "net_sales", "z_score"]])

st.write("SKU Decisions")
for _, row in decisions_view.iterrows():
    st.markdown(f"**{row.dt.date()} – {row.alert_type}**")
    st.write(row.get("insight", ""))
    st.write(f"Recommended action: {row['recommended_action']}")
    st.markdown("---")
