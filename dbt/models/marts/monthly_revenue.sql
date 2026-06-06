-- Monthly Revenue Model
-- Business-ready revenue metrics by month and region

{{ config(materialized='table') }}

WITH monthly_orders AS (
    SELECT
        order_year,
        order_month,
        DATE_TRUNC('month', order_timestamp)        AS month_start,
        region,
        device,
        payment_method,
        COUNT(order_id)                             AS total_orders,
        SUM(total_amount)                           AS gross_revenue,
        SUM(discount_amount)                        AS total_discounts,
        SUM(total_amount - COALESCE(discount_amount, 0)) AS net_revenue,
        AVG(total_amount)                           AS avg_order_value,
        COUNT(DISTINCT customer_id)                 AS unique_customers,
        SUM(items_count)                            AS total_items_sold
    FROM {{ ref('stg_orders') }}
    GROUP BY order_year, order_month, month_start, region, device, payment_method
),

with_growth AS (
    SELECT
        *,
        LAG(gross_revenue) OVER (
            PARTITION BY region
            ORDER BY month_start
        )                                           AS prev_month_revenue,

        ROUND(
            (gross_revenue - LAG(gross_revenue) OVER (
                PARTITION BY region ORDER BY month_start
            )) / NULLIF(LAG(gross_revenue) OVER (
                PARTITION BY region ORDER BY month_start
            ), 0) * 100,
            2
        )                                           AS revenue_growth_pct,

        SUM(gross_revenue) OVER (
            PARTITION BY region, order_year
            ORDER BY month_start
            ROWS UNBOUNDED PRECEDING
        )                                           AS ytd_revenue

    FROM monthly_orders
)

SELECT
    order_year,
    order_month,
    month_start,
    region,
    device,
    payment_method,
    total_orders,
    ROUND(gross_revenue::NUMERIC, 2)                AS gross_revenue,
    ROUND(total_discounts::NUMERIC, 2)              AS total_discounts,
    ROUND(net_revenue::NUMERIC, 2)                  AS net_revenue,
    ROUND(avg_order_value::NUMERIC, 2)              AS avg_order_value,
    unique_customers,
    total_items_sold,
    ROUND(prev_month_revenue::NUMERIC, 2)           AS prev_month_revenue,
    revenue_growth_pct,
    ROUND(ytd_revenue::NUMERIC, 2)                  AS ytd_revenue,
    CURRENT_TIMESTAMP                               AS calculated_at
FROM with_growth
ORDER BY month_start DESC, gross_revenue DESC
