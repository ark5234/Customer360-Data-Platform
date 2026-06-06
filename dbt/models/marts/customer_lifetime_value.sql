-- Customer Lifetime Value (LTV) Model
-- Segments customers by predicted future value

{{ config(
    materialized='table',
    indexes=[{'columns': ['customer_id'], 'unique': True}]
) }}

WITH order_history AS (
    SELECT
        customer_id,
        COUNT(order_id)                             AS total_orders,
        SUM(total_amount)                           AS total_spend,
        AVG(total_amount)                           AS avg_order_value,
        MIN(order_date)                             AS first_order_date,
        MAX(order_date)                             AS last_order_date,
        MAX(order_date) - MIN(order_date)           AS customer_lifespan_days,
        COUNT(order_id) FILTER (
            WHERE order_date >= CURRENT_DATE - INTERVAL '12 months'
        )                                           AS orders_last_12m,
        SUM(total_amount) FILTER (
            WHERE order_date >= CURRENT_DATE - INTERVAL '12 months'
        )                                           AS spend_last_12m
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
),

customer_info AS (
    SELECT
        customer_id,
        region,
        preferred_device,
        activity_status
    FROM {{ ref('stg_customers') }}
),

ltv_scored AS (
    SELECT
        o.customer_id,
        o.total_orders,
        o.total_spend,
        ROUND(o.avg_order_value::NUMERIC, 2)        AS avg_order_value,
        o.first_order_date,
        o.last_order_date,
        o.customer_lifespan_days,
        COALESCE(o.orders_last_12m, 0)              AS orders_last_12m,
        COALESCE(o.spend_last_12m, 0)               AS spend_last_12m,
        c.region,
        c.preferred_device,
        c.activity_status,

        -- Predicted LTV (12 months forward using historical trend)
        ROUND(
            CASE
                WHEN o.customer_lifespan_days > 0 THEN
                    (o.total_spend / GREATEST(o.customer_lifespan_days, 1)) * 365
                ELSE o.avg_order_value
            END::NUMERIC,
            2
        )                                           AS predicted_ltv_12m,

        -- LTV Segment
        CASE
            WHEN o.spend_last_12m >= 500000    THEN 'platinum'
            WHEN o.spend_last_12m >= 100000    THEN 'gold'
            WHEN o.spend_last_12m >= 25000     THEN 'silver'
            ELSE 'bronze'
        END                                         AS ltv_segment,

        -- LTV Score (0-100)
        LEAST(
            100,
            ROUND(
                (LOG(GREATEST(o.total_spend, 1)) * 5 +
                 o.total_orders * 2 +
                 GREATEST(0, 30 - (CURRENT_DATE - o.last_order_date)) * 0.5
                )::NUMERIC,
                1
            )
        )                                           AS ltv_score,

        CURRENT_TIMESTAMP                           AS calculated_at

    FROM order_history o
    LEFT JOIN customer_info c USING (customer_id)
)

SELECT * FROM ltv_scored
