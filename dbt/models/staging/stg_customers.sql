-- Staging: Customer dimension

{{ config(materialized='view') }}

SELECT
    customer_id,
    region,
    country,
    preferred_device,
    customer_segment,
    total_events,
    total_purchases,
    total_spend,
    avg_spend,
    days_active,
    first_seen_date,
    last_seen_date,
    CASE
        WHEN last_seen_date >= NOW() - INTERVAL '30 days'  THEN 'active'
        WHEN last_seen_date >= NOW() - INTERVAL '90 days'  THEN 'at_risk'
        WHEN last_seen_date >= NOW() - INTERVAL '180 days' THEN 'lapsed'
        ELSE 'churned'
    END AS activity_status,
    updated_at

FROM {{ source('warehouse', 'dim_customer') }}
