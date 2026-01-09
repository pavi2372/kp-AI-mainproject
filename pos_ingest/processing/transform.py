import pandas as pd
from sqlalchemy import text
from pos_ingest.db import engine

def load_raw():
    query = """
    SELECT transaction_id, store_id, sku, qty, price, discount, timestamp
    FROM transactions
    """
    return pd.read_sql_query(query, engine, parse_dates=["timestamp"])

def clean_and_standardize(df: pd.DataFrame) -> pd.DataFrame:
    # Drop duplicates
    df = df.drop_duplicates(subset=["transaction_id", "sku", "timestamp"])

    # Handle missing values
    df["discount"] = df["discount"].fillna(0.0)
    df["qty"] = df["qty"].fillna(0).astype(float)

    # Normalize numeric types
    df["price"] = df["price"].astype(float)

    # Compute net_sales
    df["net_sales"] = (df["price"] - df["discount"]) * df["qty"]
    return df

def aggregate_time(df: pd.DataFrame, freq="D") -> pd.DataFrame:
    df = df.set_index("timestamp")
    agg = (
        df.groupby(["store_id", "sku"])
          .resample(freq)["qty", "net_sales"]
          .sum()
          .reset_index()
    )
    agg.rename(columns={"timestamp": "dt"}, inplace=True)
    return agg

def write_processed(agg_df: pd.DataFrame):
    agg_df.to_sql("pos_agg_daily", engine, if_exists="replace", index=False)

if __name__ == "__main__":
    raw = load_raw()
    cleaned = clean_and_standardize(raw)
    daily = aggregate_time(cleaned, freq="D")

    # write to DB
    write_processed(daily)

    # write Parquet file
    daily.to_parquet("data/processed_pos.parquet", index=False)

