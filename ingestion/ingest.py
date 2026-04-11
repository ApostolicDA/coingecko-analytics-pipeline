import os
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timezone

client = bigquery.Client()

PROJECT_ID = "coingecko-pipeline-492920"
DATASET = "raw"
INGESTED_AT = datetime.now(timezone.utc).isoformat()

def load_to_bigquery(df, table_name):
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(df)} rows → {table_id}")

def ingest_coingecko_markets():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": 1, "sparkline": False}
    df = pd.DataFrame(requests.get(url, params=params).json())
    df["ingested_at"] = INGESTED_AT
    load_to_bigquery(df, "raw_coingecko_markets")

def ingest_fear_greed():
    df = pd.DataFrame(requests.get("https://api.alternative.me/fng/?limit=30").json()["data"])
    df["ingested_at"] = INGESTED_AT
    load_to_bigquery(df, "raw_fear_greed_index")

def ingest_exchange_rates():
    data = requests.get("https://api.exchangerate-api.com/v4/latest/USD").json()
    df = pd.DataFrame([{"base_currency": "USD", "target_currency": c, "rate": data["rates"].get(c), "date": data["date"], "ingested_at": INGESTED_AT} for c in ["ZAR", "GBP", "EUR", "AUD"]])
    load_to_bigquery(df, "raw_exchange_rates")

def ingest_global_market():
    data = requests.get("https://api.coingecko.com/api/v3/global").json()["data"]
    df = pd.DataFrame([{"active_cryptocurrencies": data["active_cryptocurrencies"], "total_market_cap_usd": data["total_market_cap"]["usd"], "total_volume_usd": data["total_volume"]["usd"], "market_cap_percentage_btc": data["market_cap_percentage"]["btc"], "market_cap_percentage_eth": data["market_cap_percentage"]["eth"], "market_cap_change_percentage_24h": data["market_cap_change_percentage_24h_usd"], "ingested_at": INGESTED_AT}])
    load_to_bigquery(df, "raw_global_market")

if __name__ == "__main__":
    print("🚀 Starting ingestion pipeline...")
    ingest_coingecko_markets()
    ingest_fear_greed()
    ingest_exchange_rates()
    ingest_global_market()
    print("✅ All sources ingested successfully!")
