with coins as (
    select * from {{ ref('stg_coingecko_markets') }}
),

exchange_rates as (
    select * from {{ ref('stg_exchange_rates') }}
),

zar_rate as (
    select exchange_rate
    from exchange_rates
    where target_currency = 'ZAR'
),

final as (
    select
        -- identifiers
        c.coin_id,
        c.coin_symbol,
        c.coin_name,
        c.market_cap_rank,

        -- usd prices
        c.current_price_usd,
        c.high_24h_usd,
        c.low_24h_usd,
        c.market_cap_usd,
        c.total_volume_usd,

        -- zar prices (cross currency)
        round(c.current_price_usd * z.exchange_rate, 2)   as current_price_zar,
        round(c.market_cap_usd * z.exchange_rate, 2)       as market_cap_zar,

        -- performance
        c.price_change_pct_24h,
        c.market_cap_change_pct_24h,

        case
            when c.price_change_pct_24h >= 10  then 'Strong Gainer'
            when c.price_change_pct_24h >= 2   then 'Gainer'
            when c.price_change_pct_24h >= -2  then 'Stable'
            when c.price_change_pct_24h >= -10 then 'Loser'
            else 'Strong Loser'
        end as performance_category,

        -- supply
        c.circulating_supply,
        c.total_supply,
        c.max_supply,
        c.all_time_high_usd,
        c.all_time_low_usd,

        -- metadata
        c.ingestion_date

    from coins c
    cross join zar_rate z
)

select * from final