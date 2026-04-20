with base as (
    select *
    from {{ source('raw', 'stripe_charges') }}
),

clean as (
    select
        -- Charge identity
        charge_id,
        payment_intent_id,
        customer_id,

        -- Amount: Stripe stores in cents — convert to dollars
        cast(amount as float64) / 100.0             as amount_usd,
        cast(amount_refunded as float64) / 100.0    as amount_refunded_usd,
        (cast(amount as float64) - cast(amount_refunded as float64)) / 100.0
                                                    as net_amount_usd,
        currency,

        -- Status standardization
        lower(status)                               as charge_status,

        -- Payment method
        lower(payment_method_type)                  as payment_method_type,
        card_brand,
        card_last4,

        -- Subscription flag: is this a recurring charge?
        case
            when subscription_id is not null then true
            else false
        end as is_subscription,

        subscription_id,
        invoice_id,

        -- Failure handling
        failure_code,
        failure_message,

        -- Timestamps
        cast(created as timestamp)                  as charge_created_at,

        -- Audit trail
        _ingested_at

    from base
),

-- Only surface successful or refunded charges
filtered as (
    select *
    from clean
    where charge_status in ('succeeded', 'refunded', 'partially_refunded')
)

select * from filtered
