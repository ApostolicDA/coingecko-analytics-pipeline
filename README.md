# Crypto Market Analytics Pipeline
### A production-style analytics engineering project — built to demonstrate the architecture dot.cards needs

**Live Dashboard:** https://lookerstudio.google.com/reporting/5b2423b8-a2c7-44b6-8aa1-62b36d16311d

**Author:** Proud Kudzai Ndlovu — Analytics Engineer | dbt · BigQuery · SQL · Python

---

## Why this project exists

From the role description, the core challenge at dot.cards seems to be replacing fragile Zapier workflows, building a governed dbt layer, and integrating sources like Shopify, Stripe, Segment, and Salesforce, and deliver trusted data to tools like Mixpanel and Customer.io.

I built this pipeline to explore that architecture end-to-end and simulate how it would work in practice.

The data sources are crypto APIs. The architecture closely mirrors what dot.cards needs..

---

## The direct mapping

| This project | dot.cards production equivalent |
|---|---|
| CoinGecko markets API | Shopify — orders, products, customers |
| ExchangeRate API | Stripe — payments, subscriptions, refunds |
| Fear & Greed Index API | Segment — user events, NFC taps, profile views |
| CoinGecko global market API | Salesforce — CRM, leads, accounts |
| Python ingestion (`ingest.py`) | Replaces Zapier-based manual workflows |
| dbt staging layer | Cleans and types raw source data |
| dbt mart layer | Business logic — revenue, retention, funnel |
| dbt test suite | Data quality and governance |
| Looker Studio dashboard | Reporting layer (Mixpanel equivalent) |
| GitHub Actions (daily at 6am UTC) | Automated orchestration — zero manual intervention |

Swap the APIs. The ingestion pattern, staging logic, mart structure, and test suite are identical.The domain changes. The data engineering patterns don’t.

---

## Architecture

```
4 External APIs (CoinGecko · Alternative.me · ExchangeRate-API)
      │
      ▼
Python Ingestion Script — ingestion/ingest.py
      │   pulls from all sources, writes to BigQuery raw layer
      ▼
BigQuery Raw Layer — 4 tables, full fidelity, timestamped
      │   no transformation, audit trail preserved
      ▼
dbt Staging Layer — clean, type, rename, document
      │   one model per source, no business logic
      ▼
dbt Mart Layer — business logic, joins, enrichment
      │   cross-source, decision-ready
      ▼
dbt Test Suite — not_null, unique, accepted_values
      │   automated quality gate on every run
      ▼
Looker Studio Dashboard — 3 pages, live, interactive
      │
      ▼
GitHub Actions — daily schedule, zero manual intervention
```

---

## Pipeline structure

### Staging layer — make data trustworthy
```
models/staging/
├── stg_coingecko_markets.sql      → maps to: stg_shopify__orders.sql
├── stg_fear_greed_index.sql       → maps to: stg_segment__events.sql
├── stg_exchange_rates.sql         → maps to: stg_stripe__charges.sql
└── stg_global_market.sql          → maps to: stg_salesforce__accounts.sql
```

Staging has one job: make raw data trustworthy. Explicit type casting, unambiguous column naming, NULL handling, and audit timestamps. No business logic. Fix it once upstream, every downstream model inherits clean data automatically.

### Mart layer — business logic lives here
```
models/marts/
├── mart_coin_performance.sql      → maps to: fct_orders.sql
├── mart_market_sentiment.sql      → maps to: fct_events.sql
└── mart_fx_crypto_correlation.sql → maps to: fct_revenue_by_channel.sql
```

Cross-source joins, enrichment, and decision-ready metrics. This is what Mixpanel and Customer.io consume.

---

## Automation — replacing Zapier

Pipeline runs daily at 6am UTC via GitHub Actions with zero manual intervention:

1. Python ingestion pulls from all 4 APIs → overwrites BigQuery raw tables
2. dbt build rebuilds all staging and mart models
3. Full dbt test suite validates every model
4. Looker Studio dashboard refreshes live

This is the same pattern used to replace fragile Zapier workflows — event-driven or scheduled ingestion, governed transformation, automated quality gates.

See: `.github/workflows/daily_ingestion.yml`

---

## Data quality — two layers, both required

During this build, the test layer caught two real issues:

**Issue 1 — caught by dbt tests:** BTC was returning NULL from the exchange rate API (unsupported on the free tier). The `not_null` test flagged it immediately. Fixed at ingestion, clean data reloaded, all tests passed.

**Issue 2 — caught by dashboard validation:** FX scorecards were summing rates across 10 mart rows instead of averaging, producing USD/ZAR of 163.9 instead of 16.41. Caught through visual validation of the dashboard output.

**The lesson:** dbt tests catch structural issues, NULLs, type errors, uniqueness violations. Business logic validation requires human eyes on the output. Both layers matter. Neither replaces the other.

This is the QA philosophy I'd bring to dot.cards' pipeline from day one.

---

## Tech stack

| Layer | Tool |
|---|---|
| Ingestion | Python, pandas, google-cloud-bigquery |
| Warehouse | Google BigQuery |
| Transformation | dbt Cloud |
| Testing | dbt generic + custom tests |
| Dashboard | Looker Studio |
| Version control | GitHub |
| Automation | GitHub Actions |

---

## Dashboard

**Coin Performance** — maps to: order and revenue performance by product

![Coin Performance](images/Coin_Performance_Dashboard.png)

**Market Sentiment** — maps to: user engagement and NFC tap trends

![Market Sentiment](images/Market_Sentiment_Dashboard.png)

**FX Correlation** — maps to: multi-source revenue attribution

![FX Correlation](images/FX_Crypto_Correlation.png)

---

## Project structure

```
coingecko-analytics-pipeline/
├── .github/workflows/
│   └── daily_ingestion.yml        ← GitHub Actions automation
├── ingestion/
│   └── ingest.py                  ← Python multi-source ingestion
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
```

---

## How to run

**Ingestion**
```bash
pip install google-cloud-bigquery pandas requests pyarrow
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
python ingestion/ingest.py
```

**dbt**
```bash
dbt build        # run all models + tests
dbt test         # tests only
dbt run          # models only
```

---

*Built by Proud Kudzai Ndlovu — April 2026*
*[LinkedIn](https://www.linkedin.com/in/proud-ndlovu-89070854/) · [GitHub](https://github.com/ApostolicDA)*
