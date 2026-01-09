import pandas as pd
from sqlalchemy import create_engine
from pos_ingest.db import engine

def load_agg():
    return pd.read_sql_table("pos_agg_daily", engine, parse_dates=["dt"])

def detect_sales_spikes(df: pd.DataFrame, window: int = 7, threshold: float = 2.0):
    df = df.sort_values(["store_id", "sku", "dt"])
    df["rolling_mean"] = (
        df.groupby(["store_id", "sku"])["net_sales"].transform(
            lambda s: s.rolling(window, min_periods=3).mean()
        )
    )
    df["rolling_std"] = (
        df.groupby(["store_id", "sku"])["net_sales"].transform(
            lambda s: s.rolling(window, min_periods=3).std()
        )
    )
    df["z_score"] = (df["net_sales"] - df["rolling_mean"]) / df["rolling_std"]
    spikes = df[(df["z_score"] >= threshold) & df["rolling_std"].notna()]
    spikes["alert_type"] = "sales_spike"
    return spikes[["store_id", "sku", "dt", "net_sales", "z_score", "alert_type"]]

def detect_low_stock_high_demand(stock_df: pd.DataFrame, demand_df: pd.DataFrame,
                                 stock_threshold: float = 10, demand_threshold: float = 50):
    # stock_df columns: store_id, sku, stock_qty
    # demand_df: aggregated sales
    merged = demand_df.merge(stock_df, on=["store_id", "sku"], how="left")
    cond = (merged["stock_qty"] <= stock_threshold) & (merged["qty"] >= demand_threshold)
    alerts = merged[cond].copy()
    alerts["alert_type"] = "low_stock_high_demand"
    return alerts[["store_id", "sku", "dt", "qty", "stock_qty", "alert_type"]]

def run_all_rules():
    df = load_agg()
    spike_alerts = detect_sales_spikes(df)
    # Example: using a dummy stock view or separate table
    # stock_df = pd.read_sql_table("stock_levels", engine)
    # low_stock_alerts = detect_low_stock_high_demand(stock_df, df)

    all_alerts = spike_alerts  # append other rule outputs as you add them
    all_alerts.to_sql("pos_alerts", engine, if_exists="replace", index=False)
    return all_alerts

if __name__ == "__main__":
    run_all_rules()
