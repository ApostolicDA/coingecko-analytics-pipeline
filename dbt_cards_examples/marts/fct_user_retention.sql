-- fct_user_retention.sql
-- Cohort retention analysis for dot.cards users.
-- Tracks what % of users acquired in a given month return in subsequent months.
-- Segments by acquisition channel and card type.
--
-- Business questions this answers:
-- - Are NFC tap users more retained than signup users?
-- - Which acquisition month had the strongest 30/60/90 day retention?
-- - Where is the biggest drop-off in the user lifecycle?

with first_touch as (
    -- Identify each user's first event and acquisition channel
    select
        unified_user_id,
        min(timestamp)                              as first_seen_at,
        date_trunc(min(timestamp), month)           as cohort_month,

        -- Acquisition channel: what was their first meaningful action?
        first_value(event_name_clean) over (
            partition by unified_user_id
            order by timestamp asc
            rows between unbounded preceding and current row
        )                                           as acquisition_event,

        -- Card type: did they tap an NFC card or sign up digitally?
        max(case when event_name_clean = 'nfc_tap' then 1 else 0 end)
                                                    as acquired_via_nfc

    from {{ ref('stg_segment__events') }}
    group by unified_user_id
),

activity as (
    -- All user activity with month reference
    select
        unified_user_id,
        date_trunc(timestamp, month)                as activity_month,
        count(*)                                    as event_count

    from {{ ref('stg_segment__events') }}
    group by unified_user_id, date_trunc(timestamp, month)
),

cohort_activity as (
    -- Join user activity to their cohort
    select
        ft.unified_user_id,
        ft.cohort_month,
        ft.acquisition_event,
        ft.acquired_via_nfc,
        a.activity_month,

        -- Months since acquisition (0 = acquisition month)
        date_diff(a.activity_month, ft.cohort_month, month) as months_since_acquisition,
        a.event_count

    from first_touch ft
    inner join activity a
        on ft.unified_user_id = a.unified_user_id
        and a.activity_month >= ft.cohort_month
),

cohort_size as (
    -- Count users per cohort month
    select
        cohort_month,
        acquisition_event,
        acquired_via_nfc,
        count(distinct unified_user_id) as cohort_users

    from first_touch
    group by cohort_month, acquisition_event, acquired_via_nfc
),

retention as (
    -- Count retained users per cohort per month
    select
        ca.cohort_month,
        ca.acquisition_event,
        ca.acquired_via_nfc,
        ca.months_since_acquisition,
        count(distinct ca.unified_user_id) as retained_users

    from cohort_activity ca
    group by
        ca.cohort_month,
        ca.acquisition_event,
        ca.acquired_via_nfc,
        ca.months_since_acquisition
)

select
    r.cohort_month,
    r.acquisition_event,
    r.acquired_via_nfc,
    r.months_since_acquisition,
    cs.cohort_users,
    r.retained_users,

    -- Retention rate: what % of the original cohort came back this month?
    round(
        safe_divide(r.retained_users, cs.cohort_users) * 100,
        2
    )                                               as retention_rate_pct,

    -- Convenience flags for dashboard filtering
    case when r.months_since_acquisition = 1  then true else false end as is_month_1,
    case when r.months_since_acquisition = 3  then true else false end as is_month_3,
    case when r.months_since_acquisition = 6  then true else false end as is_month_6

from retention r
inner join cohort_size cs
    on r.cohort_month = cs.cohort_month
    and r.acquisition_event = cs.acquisition_event
    and r.acquired_via_nfc = cs.acquired_via_nfc

order by
    r.cohort_month,
    r.acquisition_event,
    r.months_since_acquisition
