with source as (
    select * from {{ source('raw', 'raw_coingecko_markets') }}
),

renamed as (
    select
        -- identifiers
        id                          as coin_id,
        symbol                      as coin_symbol,
        name                        as coin_name,

        -- market data
        current_price               as current_price_usd,
        market_cap                  as market_cap_usd,
        market_cap_rank,
        total_volume                as total_volume_usd,
        high_24h                    as high_24h_usd,
        low_24h                     as low_24h_usd,

        -- price changes
        price_change_24h            as price_change_24h_usd,
        price_change_percentage_24h as price_change_pct_24h,
        market_cap_change_24h       as market_cap_change_24h_usd,
        market_cap_change_percentage_24h as market_cap_change_pct_24h,

        -- supply
        circulating_supply,
        total_supply,
        max_supply,
        ath                         as all_time_high_usd,
        atl                         as all_time_low_usd,

        -- metadata
        last_updated,
        ingested_at,
        current_date()              as ingestion_date

    from source
)

select * from renamed