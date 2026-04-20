with base as (
    select *
    from {{ source('raw', 'shopify_orders') }}
),

clean as (
    select
        -- Order identity
        order_id,
        order_number,

        -- Customer identity
        customer_id,
        coalesce(customer_email, 'unknown')         as customer_email,

        -- Order status standardization
        lower(financial_status)                     as financial_status,
        lower(fulfillment_status)                   as fulfillment_status,

        -- Revenue fields: cast and handle nulls
        cast(coalesce(total_price, '0') as float64)         as total_price,
        cast(coalesce(subtotal_price, '0') as float64)      as subtotal_price,
        cast(coalesce(total_discounts, '0') as float64)     as total_discounts,
        cast(coalesce(total_tax, '0') as float64)           as total_tax,
        currency,

        -- Product flags
        cast(coalesce(line_items_count, 0) as int64)        as line_items_count,

        -- Channel attribution
        coalesce(source_name, 'unknown')            as source_name,
        referring_site,

        -- Delivery
        lower(shipping_address_country)             as shipping_country,
        lower(shipping_address_city)                as shipping_city,

        -- Timestamps
        cast(created_at as timestamp)               as order_created_at,
        cast(updated_at as timestamp)               as order_updated_at,
        cast(processed_at as timestamp)             as order_processed_at,

        -- Audit trail
        _ingested_at

    from base
),

-- Only include paid or partially paid orders downstream
filtered as (
    select *
    from clean
    where financial_status in ('paid', 'partially_paid', 'refunded', 'partially_refunded')
)

select * from filtered
