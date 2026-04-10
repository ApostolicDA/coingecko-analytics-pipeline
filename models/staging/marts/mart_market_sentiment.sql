with fear_greed as (
    select * from {{ ref('stg_fear_greed_index') }}
),

global_market as (
    select * from {{ ref('stg_global_market') }}
),

latest_sentiment as (
    select
        fear_greed_value,
        sentiment_label,
        sentiment_date,
        case
            when fear_greed_value >= 75 then 'Extreme Greed'
            when fear_greed_value >= 55 then 'Greed'
            when fear_greed_value >= 45 then 'Neutral'
            when fear_greed_value >= 25 then 'Fear'
            else 'Extreme Fear'
        end as sentiment_zone,
        ingested_at
    from fear_greed
),

final as (
    select
        -- sentiment
        ls.fear_greed_value,
        ls.sentiment_label,
        ls.sentiment_zone,
        ls.sentiment_date,

        -- global market context
        gm.total_market_cap_usd,
        gm.total_volume_usd,
        gm.btc_dominance_pct,
        gm.eth_dominance_pct,
        gm.market_cap_change_pct_24h,

        -- derived insight
        case
            when ls.fear_greed_value < 25
            and gm.market_cap_change_pct_24h < 0
            then 'High Capitulation Risk'
            when ls.fear_greed_value > 75
            and gm.market_cap_change_pct_24h > 0
            then 'Overheating Market'
            else 'Normal Conditions'
        end as market_risk_signal,

        -- metadata
        gm.ingestion_date

    from latest_sentiment ls
    cross join global_market gm
)

select * from final