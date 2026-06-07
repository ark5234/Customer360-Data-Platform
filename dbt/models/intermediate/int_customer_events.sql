-- Intermediate: Event Behaviour Aggregates
-- Aggregates raw events into per-customer behavioural signals.
-- Feeds the feature_engineering pipeline and product analytics mart.

{{ config(materialized='table', schema='intermediate') }}

WITH base_events AS (
    SELECT
        customer_id,
        event_type,
        event_timestamp::DATE       AS event_date,
        event_timestamp::TIMESTAMP  AS event_ts
    FROM {{ source('warehouse', 'raw_events') }}
    WHERE customer_id IS NOT NULL
),

event_counts AS (
    SELECT
        customer_id,

        -- Overall engagement
        COUNT(*)                                                AS total_events,
        COUNT(DISTINCT event_date)                              AS active_days,
        MIN(event_date)                                         AS first_event_date,
        MAX(event_date)                                         AS last_event_date,
        CURRENT_DATE - MAX(event_date)                          AS days_since_last_event,

        -- Event-type breakdowns
        COUNT(*) FILTER (WHERE event_type = 'Login')            AS login_count,
        COUNT(*) FILTER (WHERE event_type = 'ProductView')      AS product_view_count,
        COUNT(*) FILTER (WHERE event_type = 'Search')           AS search_count,
        COUNT(*) FILTER (WHERE event_type = 'AddToCart')        AS cart_add_count,
        COUNT(*) FILTER (WHERE event_type = 'Purchase')         AS purchase_count,
        COUNT(*) FILTER (WHERE event_type = 'Refund')           AS refund_count,
        COUNT(*) FILTER (WHERE event_type = 'PaymentFailed')    AS payment_fail_count,
        COUNT(*) FILTER (WHERE event_type = 'WishlistAdd')      AS wishlist_count,
        COUNT(*) FILTER (WHERE event_type = 'ReviewSubmit')     AS review_count,

        -- Time-windowed counts (last 30 days)
        COUNT(*) FILTER (
            WHERE event_date >= CURRENT_DATE - INTERVAL '30 days'
        )                                                       AS events_last_30d,
        COUNT(*) FILTER (
            WHERE event_type = 'Login'
            AND event_date >= CURRENT_DATE - INTERVAL '30 days'
        )                                                       AS logins_last_30d,
        COUNT(*) FILTER (
            WHERE event_type = 'Purchase'
            AND event_date >= CURRENT_DATE - INTERVAL '30 days'
        )                                                       AS purchases_last_30d

    FROM base_events
    GROUP BY customer_id
),

derived_signals AS (
    SELECT
        *,

        -- Cart abandonment rate = cart adds without purchase
        CASE
            WHEN cart_add_count > 0
            THEN ROUND(
                1.0 - (purchase_count::NUMERIC / NULLIF(cart_add_count, 0)),
                4
            )
            ELSE NULL
        END                                                     AS cart_abandonment_rate,

        -- Browse-to-buy ratio
        CASE
            WHEN product_view_count > 0
            THEN ROUND(
                purchase_count::NUMERIC / NULLIF(product_view_count, 0),
                4
            )
            ELSE 0
        END                                                     AS browse_to_buy_ratio,

        -- Engagement score (weighted signal)
        ROUND((
            login_count * 1 +
            product_view_count * 2 +
            search_count * 1 +
            cart_add_count * 5 +
            purchase_count * 10 +
            review_count * 8
        )::NUMERIC / NULLIF(active_days, 0), 2)                 AS daily_engagement_score

    FROM event_counts
)

SELECT * FROM derived_signals
