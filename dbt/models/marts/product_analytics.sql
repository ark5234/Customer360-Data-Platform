-- Product Analytics mart
-- Combines view, cart, purchase funnel metrics per product

{{ config(materialized='table') }}

WITH event_summary AS (
    SELECT
        -- Use JSONB fields from raw_events for product metrics
        (payload->>'product_id')::VARCHAR           AS product_id,
        (payload->>'product_name')::VARCHAR         AS product_name,
        (payload->>'category')::VARCHAR             AS category,
        event_type,
        COUNT(*)                                    AS event_count,
        COUNT(DISTINCT customer_id)                 AS unique_customers
    FROM {{ source('raw', 'raw_events') }}
    WHERE payload->>'product_id' IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

pivoted AS (
    SELECT
        product_id,
        MAX(product_name)                           AS product_name,
        MAX(category)                               AS category,
        SUM(CASE WHEN event_type = 'PRODUCT_VIEW'  THEN event_count ELSE 0 END) AS views,
        SUM(CASE WHEN event_type = 'ADD_TO_CART'   THEN event_count ELSE 0 END) AS cart_adds,
        SUM(CASE WHEN event_type = 'PURCHASE'      THEN event_count ELSE 0 END) AS purchases,
        SUM(CASE WHEN event_type = 'REFUND'        THEN event_count ELSE 0 END) AS refunds,
        MAX(CASE WHEN event_type = 'PRODUCT_VIEW'  THEN unique_customers END) AS unique_viewers
    FROM event_summary
    GROUP BY product_id
)

SELECT
    product_id,
    product_name,
    category,
    views,
    cart_adds,
    purchases,
    refunds,
    unique_viewers,
    ROUND(
        cart_adds::NUMERIC / NULLIF(views, 0) * 100, 2
    )                                               AS view_to_cart_rate,
    ROUND(
        purchases::NUMERIC / NULLIF(cart_adds, 0) * 100, 2
    )                                               AS cart_to_purchase_rate,
    ROUND(
        purchases::NUMERIC / NULLIF(views, 0) * 100, 2
    )                                               AS overall_conversion_rate,
    ROUND(
        refunds::NUMERIC / NULLIF(purchases, 0) * 100, 2
    )                                               AS refund_rate,
    CURRENT_TIMESTAMP                               AS calculated_at
FROM pivoted
ORDER BY purchases DESC
