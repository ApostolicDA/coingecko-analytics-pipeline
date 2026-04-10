with exchange_rates as (
    select * from {{ ref('stg_exchange_rates') }}
),

global_market as (
    select * from {{ ref('stg_global_market') }}
),

top_coins as (
    select
        coin_symbol,
        coin_name,
        current_price_usd,
        price_change_pct_24h,
        market_cap_rank
    from {{ ref('mart_coin_performance') }}
    where market_cap_rank <= 10
),

fx_pivot as (
    select
        max(case when target_currency = 'ZAR' then exchange_rate end) as usd_to_zar,
        max(case when target_currency = 'GBP' then exchange_rate end) as usd_to_gbp,
        max(case when target_currency = 'EUR' then exchange_rate end) as usd_to_eur,
        max(case when target_currency = 'AUD' then exchange_rate end) as usd_to_aud,
        max(rate_date)                                                  as rate_date
    from exchange_rates
),

final as (
    select
        -- top coin
        tc.market_cap_rank,
        tc.coin_symbol,
        tc.coin_name,
        tc.current_price_usd,
        tc.price_change_pct_24h,

        -- price in multiple currencies
        round(tc.current_price_usd * fx.usd_to_zar, 2) as price_zar,
        round(tc.current_price_usd * fx.usd_to_gbp, 2) as price_gbp,
        round(tc.current_price_usd * fx.usd_to_eur, 2) as price_eur,
        round(tc.current_price_usd * fx.usd_to_aud, 2) as price_aud,

        -- market context
        gm.btc_dominance_pct,
        gm.total_market_cap_usd,
        gm.market_cap_change_pct_24h,

        -- fx rates reference
        fx.usd_to_zar,
        fx.usd_to_gbp,
        fx.usd_to_eur,
        fx.usd_to_aud,
        fx.rate_date,

        -- metadata
        gm.ingestion_date

    from top_coins tc
    cross join fx_pivot fx
    cross join global_market gm
)

select * from final