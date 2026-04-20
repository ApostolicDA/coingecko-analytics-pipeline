with base as (
    select *
    from {{ source('raw', 'segment_events_v2') }}
),

clean as (
    select
        event_id,

        -- Identity resolution: coalesce logged-in and anonymous users
        user_id,
        anonymous_id,
        coalesce(user_id, anonymous_id) as unified_user_id,

        -- Event taxonomy: standardize inconsistent Segment event names
        case
            when lower(event_name) in ('page viewed', 'page_viewed')   then 'page_viewed'
            when lower(event_name) in ('signup', 'sign_up')            then 'user_signed_up'
            when lower(event_name) in ('add to cart', 'add_to_cart')   then 'add_to_cart'
            when lower(event_name) in ('purchase', 'order_completed')  then 'purchase'
            when lower(event_name) = 'login'                           then 'login'
            when lower(event_name) in ('nfc_tap', 'card_tap')          then 'nfc_tap'
            when lower(event_name) in ('profile_view', 'profile viewed') then 'profile_view'
            else 'unknown_event'
        end as event_name_clean,

        timestamp,

        -- JSON property extraction: pull typed fields from Segment properties blob
        json_value(properties, '$.page')        as page,
        json_value(properties, '$.method')      as signup_method,
        json_value(properties, '$.product_id')  as product_id,
        json_value(properties, '$.order_id')    as order_id,
        json_value(properties, '$.card_id')     as card_id,
        json_value(properties, '$.profile_id')  as profile_id,

        cast(json_value(properties, '$.revenue') as float64) as revenue,

        -- Preserve raw properties for auditability
        properties,

        -- Audit trail
        _ingested_at

    from base
),

dedup as (
    -- Remove duplicate events: keep earliest event_id per user + event + timestamp
    select *
    from (
        select *,
            row_number() over (
                partition by unified_user_id, event_name_clean, timestamp
                order by event_id
            ) as rn
        from clean
    )
    where rn = 1
)

select
    event_id,
    user_id,
    anonymous_id,
    unified_user_id,
    event_name_clean,
    timestamp,
    page,
    signup_method,
    product_id,
    order_id,
    card_id,
    profile_id,
    revenue,
    properties,
    _ingested_at

from dedup
