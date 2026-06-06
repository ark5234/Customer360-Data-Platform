-- Customer Retention Cohort Analysis
-- Cohort: month of first purchase
-- Retention: % who came back in subsequent months

{{ config(materialized='table') }}

WITH first_purchases AS (
    SELECT
        customer_id,
        MIN(order_date)                             AS first_purchase_date,
        DATE_TRUNC('month', MIN(order_date))        AS cohort_month
    FROM {{ ref('stg_orders') }}
    GROUP BY customer_id
),

all_purchases AS (
    SELECT
        o.customer_id,
        o.order_date,
        DATE_TRUNC('month', o.order_date)           AS purchase_month,
        fp.cohort_month
    FROM {{ ref('stg_orders') }} o
    JOIN first_purchases fp USING (customer_id)
),

cohort_data AS (
    SELECT
        cohort_month,
        purchase_month,
        COUNT(DISTINCT customer_id)                 AS customers,
        EXTRACT(MONTH FROM AGE(purchase_month, cohort_month))::INT AS period_number
    FROM all_purchases
    GROUP BY cohort_month, purchase_month
),

cohort_sizes AS (
    SELECT
        cohort_month,
        customers                                   AS cohort_size
    FROM cohort_data
    WHERE period_number = 0
)

SELECT
    cd.cohort_month,
    cd.purchase_month,
    cd.period_number,
    cs.cohort_size,
    cd.customers                                    AS retained_customers,
    ROUND(
        cd.customers::NUMERIC / NULLIF(cs.cohort_size, 0) * 100,
        2
    )                                               AS retention_rate_pct,
    cs.cohort_size - cd.customers                  AS churned_customers,
    ROUND(
        (1 - cd.customers::NUMERIC / NULLIF(cs.cohort_size, 0)) * 100,
        2
    )                                               AS churn_rate_pct,
    CURRENT_TIMESTAMP                               AS calculated_at

FROM cohort_data cd
JOIN cohort_sizes cs USING (cohort_month)
ORDER BY cd.cohort_month, cd.period_number
