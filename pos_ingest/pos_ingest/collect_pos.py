import pandas as pd
from sqlalchemy import Table, Column, Integer, String, Float, DateTime, MetaData
from sqlalchemy.dialects.sqlite import DATETIME
from .db import engine

metadata = MetaData()

transactions = Table(
    "transactions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("transaction_id", String),
    Column("store_id", String),
    Column("sku", String),
    Column("qty", Float),
    Column("price", Float),
    Column("discount", Float),
    Column("timestamp", DATETIME),
)

def init_db():
    metadata.create_all(engine)

def ingest_batch(df: pd.DataFrame):
    df.to_sql("transactions", con=engine, if_exists="append", index=False)

if __name__ == "__main__":
    init_db()

    # Example: load from CSV exported from POS
    df = pd.read_csv("data/raw_transactions.csv", parse_dates=["timestamp"])
    ingest_batch(df)
