### Test Suite
Every mart and staging model has automated tests. The test layer caught a real data quality issue during development — BTC was returning NULL from the exchange rate API (unsupported on the free tier). The fix was applied at the ingestion layer, clean data reloaded, and all tests passed. That is the system working as designed.

---

## Key Business Insights

### 1. Market is in Extreme Fear
Over the 30-day analysis period, the crypto market spent the majority of days in Extreme Fear territory on the Fear and Greed Index. This level of sustained fear typically signals either a buying opportunity for long-term holders or continued downside risk if macro conditions worsen.

### 2. Bitcoin Dominance is Elevated
BTC dominance is holding above average, indicating capital consolidation into the market leader during uncertain conditions. This is a classic risk-off pattern — investors rotate from altcoins into Bitcoin as a relative safe haven within crypto.

### 3. Total Market Cap at $2.5 Trillion
With 100 tracked coins and a combined market cap of $2.5T and daily volume of $143B, the market remains liquid despite the fear sentiment. High volume during fear periods can indicate capitulation — forced selling — which historically precedes recoveries.

### 4. ZAR Exposure is Significant
At a USD/ZAR rate of approximately 18.4, South African investors face compounded volatility — crypto price swings on top of currency risk. Bitcoin priced at approximately R1.2M highlights the dual exposure. A 10% crypto drop combined with a 5% ZAR weakening represents a 15% loss in rand terms.

### 5. Altcoin Dispersion is Wide
The performance category distribution across the top 100 coins shows significant dispersion — while Bitcoin and Ethereum show relative stability, smaller cap coins are experiencing stronger losses, consistent with risk-off market behaviour.

---

## Recommendations

**For crypto investors:** Elevated fear with high volume suggests the market is in a distribution or capitulation phase. Dollar-cost averaging into BTC during Extreme Fear periods has historically produced strong long-term returns, but position sizing should account for ZAR/USD currency risk.

**For data teams:** This pipeline architecture — multi-source ingestion, governed staging layer, business logic in marts, automated tests — is directly applicable to any business with fragmented data sources. Replace the crypto APIs with Shopify, Stripe, Salesforce, and Segment and the architecture is identical.

**For product teams:** Sentiment data combined with price action and FX rates creates a richer signal than any single source alone. The mart_market_sentiment model's risk signal field — classifying market conditions as Normal, Overheating, or High Capitulation Risk — is the kind of derived insight that powers automated alerts and decision support tools.

---

## How to Run

### Ingestion
```bash
cd ingestion
pip install google-cloud-bigquery pandas requests pyarrow
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/key.json
python ingest.py
```

### dbt
```bash
dbt build          # run all models and tests
dbt test           # run tests only
dbt run            # run models only
```

### Automation
The pipeline is scheduled to run daily at 6am UTC via GitHub Actions / Google Cloud Scheduler. Each run overwrites the raw layer with fresh data, dbt rebuilds all models, and the Looker Studio dashboard updates automatically.

---


## Project Structure
coingecko-analytics-pipeline/
├── .github/workflows/
│   └── daily_ingestion.yml     # GitHub Actions daily schedule
├── ingestion/
│   └── ingest.py               # 4-source Python ingestion script
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── schema.yml
│   │   ├── stg_coingecko_markets.sql
│   │   ├── stg_fear_greed_index.sql
│   │   ├── stg_exchange_rates.sql
│   │   └── stg_global_market.sql
│   └── marts/
│       ├── schema.yml
│       ├── mart_coin_performance.sql
│       ├── mart_market_sentiment.sql
│       └── mart_fx_crypto_correlation.sql
├── tests/
├── dbt_project.yml
└── README.md


---

## Author

**Proud Kudzai Ndlovu**
Analytics Engineer | dbt · BigQuery · SQL · Python
Johannesburg, South Africa | Open to Remote
[LinkedIn](https://linkedin.com/in/proud-ndlovu-89070854) · [GitHub](https://github.com/ApostolicDA)





