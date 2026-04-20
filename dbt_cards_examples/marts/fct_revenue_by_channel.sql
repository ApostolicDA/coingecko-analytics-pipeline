-- fct_revenue_by_channel.sql
-- Unified revenue across dot.cards three business lines:
-- (1) E-commerce — physical card sales via Shopify
-- (2) SaaS — dot.teams subscriptions via Stripe
-- (3) One-time — individual digital purchases via Stripe
--
-- Leadership gets ONE total revenue number.
-- Analysts can filter by business_line for granular view.
-- No double counting — each charge appears in exactly one line.

with shopify_revenue as (
    select
        date_trunc(order_created_at, month)     as revenue_month,
        order_id                                as transaction_id,
        customer_id,
        'ecommerce'                             as business_line,
        'shopify'                               as revenue_source,
        total_price                             as gross_revenue,
        total_discounts                         as discounts,
        total_price - total_discounts           as net_revenue,
        shipping_country                        as country,
        source_name                             as acquisition_channel,
        order_created_at                        as transaction_at

    from {{ ref('stg_shopify__orders') }}
    where financial_status = 'paid'
),

stripe_saas_revenue as (
    select
        date_trunc(charge_created_at, month)    as revenue_month,
        charge_id                               as transaction_id,
        customer_id,
        'saas'                                  as business_line,
        'stripe_subscription'                   as revenue_source,
        net_amount_usd                          as gross_revenue,
        amount_refunded_usd                     as discounts,
        net_amount_usd                          as net_revenue,
        null                                    as country,
        'subscription'                          as acquisition_channel,
        charge_created_at                       as transaction_at

    from {{ ref('stg_stripe__charges') }}
    where is_subscription = true
      and charge_status = 'succeeded'
),

stripe_onetime_revenue as (
    select
        date_trunc(charge_created_at, month)    as revenue_month,
        charge_id                               as transaction_id,
        customer_id,
        'digital'                               as business_line,
        'stripe_onetime'                        as revenue_source,
        net_amount_usd                          as gross_revenue,
        amount_refunded_usd                     as discounts,
        net_amount_usd                          as net_revenue,
        null                                    as country,
        'direct'                                as acquisition_channel,
        charge_created_at                       as transaction_at

    from {{ ref('stg_stripe__charges') }}
    where is_subscription = false
      and charge_status = 'succeeded'
),

-- Union all three revenue streams
all_revenue as (
    select * from shopify_revenue
    union all
    select * from stripe_saas_revenue
    union all
    select * from stripe_onetime_revenue
)

select
    revenue_month,
    transaction_id,
    customer_id,
    business_line,
    revenue_source,
    gross_revenue,
    discounts,
    net_revenue,
    country,
    acquisition_channel,
    transaction_at,

    -- Running totals for dashboard scorecards
    sum(net_revenue) over (
        partition by business_line
        order by transaction_at
        rows between unbounded preceding and current row
    ) as cumulative_revenue_by_line,

    sum(net_revenue) over (
        order by transaction_at
        rows between unbounded preceding and current row
    ) as cumulative_total_revenue

from all_revenue
order by transaction_at desc
