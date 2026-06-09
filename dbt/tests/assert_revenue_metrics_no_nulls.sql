-- dbt Test: assert_revenue_metrics_no_nulls
-- Ensures every revenue_metrics record has non-null region, year, and revenue.

SELECT
    order_year,
    order_month,
    region,
    gross_revenue
FROM {{ ref('monthly_revenue') }}
WHERE region IS NULL
   OR order_year IS NULL
   OR order_month IS NULL
   OR gross_revenue IS NULL
