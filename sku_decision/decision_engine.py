import pandas as pd
from sqlalchemy import create_engine
from pos_ingest.db import engine

def load_sources():
    agg = pd.read_sql_table("pos_agg_daily", engine, parse_dates=["dt"])
    alerts = pd.read_sql_table("pos_alerts", engine, parse_dates=["dt"])
    insights = pd.read_sql_table("pos_insights_llm", engine, parse_dates=["dt"])
    return agg, alerts, insights

def simple_replenishment_logic(df: pd.DataFrame, stock_df: pd.DataFrame):
    merged = df.merge(stock_df, on=["store_id", "sku"], how="left")
    avg_daily_sales = (
        merged.groupby(["store_id", "sku"])["qty"]
        .rolling(window=7, min_periods=3)
        .mean()
        .reset_index(level=[0,1], drop=True)
    )
    merged["avg_daily_sales_7d"] = avg_daily_sales

    # Target: cover next 7 days demand
    merged["target_stock"] = merged["avg_daily_sales_7d"] * 7
    merged["replenishment_qty"] = (merged["target_stock"] - merged["stock_qty"]).clip(lower=0)
    return merged[["store_id", "sku", "dt", "replenishment_qty"]]

def build_decisions():
    agg, alerts, insights = load_sources()

    # Example: dummy stock DF
    stock_df = agg.groupby(["store_id", "sku"], as_index=False)["qty"].last()
    stock_df.rename(columns={"qty": "stock_qty"}, inplace=True)

    repl = simple_replenishment_logic(agg, stock_df)

    decisions = alerts.merge(insights, on=["store_id", "sku", "dt", "alert_type"], how="left")
    decisions = decisions.merge(repl, on=["store_id", "sku", "dt"], how="left")

    decisions["recommended_action"] = decisions.apply(
        lambda r: (
            f"Replenish {int(r.replenishment_qty)} units and monitor promotion impact."
            if r.get("replenishment_qty", 0) > 0 else
            "No replenishment needed; monitor trend."
        ),
        axis=1,
    )

    decisions.to_sql("pos_sku_decisions", engine, if_exists="replace", index=False)
    return decisions

if __name__ == "__main__":
    build_decisions()
