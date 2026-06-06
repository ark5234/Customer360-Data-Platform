-- Test: No negative revenue in monthly_revenue mart
SELECT *
FROM {{ ref('monthly_revenue') }}
WHERE gross_revenue < 0
