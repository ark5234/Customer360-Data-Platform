-- dbt Test: assert_revenue_metrics_no_nulls
-- Ensures every revenue_metrics record has non-null region, year, and revenue.

SELECT
    id,
    region,
    year,
    month,
    total_revenue
FROM {{ ref('monthly_revenue') }}
WHERE region IS NULL
   OR year IS NULL
   OR total_revenue IS NULL
