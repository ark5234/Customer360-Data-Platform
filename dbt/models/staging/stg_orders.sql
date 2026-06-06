-- Staging: Orders fact table

{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    event_timestamp                             AS order_timestamp,
    DATE(event_timestamp)                       AS order_date,
    EXTRACT(YEAR FROM event_timestamp)::INT     AS order_year,
    EXTRACT(MONTH FROM event_timestamp)::INT    AS order_month,
    total_amount,
    discount_amount,
    tax_amount,
    total_amount - COALESCE(discount_amount, 0) AS net_amount,
    payment_method,
    items_count,
    INITCAP(TRIM(region))                       AS region,
    INITCAP(TRIM(device))                       AS device,
    created_at

FROM {{ source('warehouse', 'fact_orders') }}

WHERE
    order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND total_amount > 0
