with source as (
    select * from {{ source('raw', 'raw_fear_greed_index') }}
),

renamed as (
    select
        -- identifiers
        cast(value as int64)          as fear_greed_value,
        value_classification          as sentiment_label,
        
        -- time
        cast(timestamp as int64)      as unix_timestamp,
        timestamp_add(
            timestamp('1970-01-01'),
            interval cast(timestamp as int64) second
        )                             as sentiment_date,
        
        -- metadata
        ingested_at

    from source
)

select * from renamed