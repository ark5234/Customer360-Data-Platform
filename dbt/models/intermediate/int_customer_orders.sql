-- Intermediate: Customer Order Summary
-- Aggregates raw order + customer data into a per-customer summary
-- Used as a shared building block for LTV, retention, and product mart models.

{{ config(materialized='table', schema='intermediate') }}

WITH base_orders AS (
    SELECT
        o.customer_id,
        o.order_id,
        o.total_amount,
        o.discount_amount,
        o.payment_method,
        o.items_count,
        o.region,
        o.device,
        o.event_timestamp::DATE AS order_date
    FROM {{ source('warehouse', 'fact_orders') }} o
    WHERE o.total_amount > 0           -- exclude zero-value orders
),

customer_summary AS (
    SELECT
        customer_id,
        region,

        -- Volume metrics
        COUNT(order_id)                                         AS total_orders,
        SUM(total_amount)                                       AS total_spend,
        AVG(total_amount)                                       AS avg_order_value,
        MAX(total_amount)                                       AS max_order_value,
        MIN(total_amount)                                       AS min_order_value,
        SUM(items_count)                                        AS total_items,
        AVG(items_count)                                        AS avg_items_per_order,

        -- Recency metrics
        MIN(order_date)                                         AS first_order_date,
        MAX(order_date)                                         AS last_order_date,
        CURRENT_DATE - MAX(order_date)                          AS days_since_last_order,
        (MAX(order_date) - MIN(order_date))                     AS customer_lifespan_days,

        -- Time-windowed metrics (last 12 months)
        COUNT(order_id) FILTER (
            WHERE order_date >= CURRENT_DATE - INTERVAL '12 months'
        )                                                       AS orders_last_12m,
        SUM(total_amount) FILTER (
            WHERE order_date >= CURRENT_DATE - INTERVAL '12 months'
        )                                                       AS spend_last_12m,

        -- Time-windowed metrics (last 30 days)
        COUNT(order_id) FILTER (
            WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
        )                                                       AS orders_last_30d,
        SUM(total_amount) FILTER (
            WHERE order_date >= CURRENT_DATE - INTERVAL '30 days'
        )                                                       AS spend_last_30d,

        -- Discount behaviour
        AVG(discount_amount)                                    AS avg_discount,
        SUM(discount_amount)                                    AS total_discount,

        -- Device & payment preferences
        MODE() WITHIN GROUP (ORDER BY payment_method)           AS preferred_payment,
        MODE() WITHIN GROUP (ORDER BY device)                   AS preferred_device,

        -- Order frequency (orders per month over active lifespan)
        ROUND(
            COUNT(order_id)::NUMERIC /
            NULLIF(GREATEST((MAX(order_date) - MIN(order_date)) / 30, 1), 0),
            2
        )                                                       AS monthly_order_frequency

    FROM base_orders
    GROUP BY customer_id, region
)

SELECT
    cs.*,
    -- Churn risk proxy based on recency
    CASE
        WHEN cs.days_since_last_order <= 30  THEN 'active'
        WHEN cs.days_since_last_order <= 90  THEN 'at_risk'
        WHEN cs.days_since_last_order <= 180 THEN 'lapsed'
        ELSE 'churned'
    END                                                         AS recency_segment,

    -- Value tier
    CASE
        WHEN cs.spend_last_12m >= 500000 THEN 'platinum'
        WHEN cs.spend_last_12m >= 100000 THEN 'gold'
        WHEN cs.spend_last_12m >= 25000  THEN 'silver'
        ELSE 'bronze'
    END                                                         AS value_tier

FROM customer_summary cs
