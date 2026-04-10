with source as (
    select * from {{ source('raw', 'raw_exchange_rates') }}
),

renamed as (
    select
        -- identifiers
        base_currency,
        target_currency,
        
        -- rates
        cast(rate as float64)         as exchange_rate,
        
        -- time
        cast(date as date)            as rate_date,
        
        -- metadata
        ingested_at

    from source
)

select * from renamed