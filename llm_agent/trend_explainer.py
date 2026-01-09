import pandas as pd
from sqlalchemy import create_engine
from pos_ingest.db import engine
from .llama_client import generate_response

def load_alerts():
    return pd.read_sql_table("pos_alerts", engine, parse_dates=["dt"])

def make_prompt(row: pd.Series, context_df: pd.DataFrame) -> str:
    sku = row["sku"]
    store = row["store_id"]
    dt = row["dt"]

    history = (
        context_df[(context_df["store_id"] == store) & (context_df["sku"] == sku)]
        .sort_values("dt")
        .tail(14)
    )

    history_text = "\n".join(
        f"{r.dt.date()} - qty: {r.qty}, net_sales: {r.net_sales:.2f}"
        for _, r in history.iterrows()
    )

    prompt = f"""
You are a POS trends analyst for a retail chain.
Analyze the following sales history for SKU {sku} at store {store}.

History:
{history_text}

An alert was triggered on {dt.date()} for type '{row['alert_type']}' with net_sales={row['net_sales']:.2f}.

1. Explain why this trend might have occurred (consider promotions, seasonality, price changes, or external factors).
2. Identify any cross-SKU relationships or cannibalization candidates.
3. Provide 2-3 concise recommendations for the retail manager.
Respond in clear, business-friendly language.
"""
    return prompt.strip()

def generate_insights():
    alerts = load_alerts()
    context_df = pd.read_sql_table("pos_agg_daily", engine, parse_dates=["dt"])
    insights_rows = []

    for _, row in alerts.iterrows():
        prompt = make_prompt(row, context_df)
        explanation = generate_response(prompt)
        insights_rows.append({
            "store_id": row["store_id"],
            "sku": row["sku"],
            "dt": row["dt"],
            "alert_type": row["alert_type"],
            "insight": explanation,
        })

    insights_df = pd.DataFrame(insights_rows)
    insights_df.to_sql("pos_insights_llm", engine, if_exists="replace", index=False)
    return insights_df

if __name__ == "__main__":
    generate_insights()
