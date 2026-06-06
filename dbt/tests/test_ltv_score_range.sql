-- Test: LTV score must be between 0 and 100
SELECT *
FROM {{ ref('customer_lifetime_value') }}
WHERE ltv_score < 0 OR ltv_score > 100
