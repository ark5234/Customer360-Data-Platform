-- dbt Test: assert_churn_probability_bounds
-- Ensures all churn probability scores are between 0 and 1 (valid probability).

SELECT
    customer_id,
    churn_probability
FROM {{ ref('customer_lifetime_value') }}
WHERE churn_probability IS NOT NULL
  AND (churn_probability < 0 OR churn_probability > 1)
