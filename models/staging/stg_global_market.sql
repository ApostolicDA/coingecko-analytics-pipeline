with source as (
    select * from {{ source('raw', 'raw_global_market') }}
),

renamed as (
    select
        -- market size
        active_cryptocurrencies,
        cast(total_market_cap_usd as float64)      as total_market_cap_usd,
        cast(total_volume_usd as float64)           as total_volume_usd,
        
        -- dominance
        cast(market_cap_percentage_btc as float64)  as btc_dominance_pct,
        cast(market_cap_percentage_eth as float64)  as eth_dominance_pct,
        
        -- change
        cast(market_cap_change_percentage_24h as float64) as market_cap_change_pct_24h,
        
        -- metadata
        ingested_at,
        current_date()                              as ingestion_date

    from source
)

select * from renamed