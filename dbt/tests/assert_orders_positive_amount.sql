-- dbt Test: assert_orders_positive_amount
-- Ensures no negative or zero-value orders exist in the orders staging model.

SELECT
    order_id,
    customer_id,
    total_amount
FROM {{ ref('stg_orders') }}
WHERE total_amount <= 0
