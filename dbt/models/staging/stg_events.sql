-- Staging: Raw events from warehouse raw_events table
-- Applies basic type casting and field renaming

{{ config(materialized='view') }}

SELECT
    event_id,
    customer_id,
    UPPER(TRIM(event_type))                     AS event_type,
    session_id,
    kafka_topic,
    INITCAP(TRIM(device))                       AS device,
    INITCAP(TRIM(region))                       AS region,
    event_timestamp::TIMESTAMP                  AS event_timestamp,
    DATE(event_timestamp)                       AS event_date,
    EXTRACT(YEAR FROM event_timestamp)::INT     AS event_year,
    EXTRACT(MONTH FROM event_timestamp)::INT    AS event_month,
    EXTRACT(HOUR FROM event_timestamp)::INT     AS event_hour,
    EXTRACT(DOW FROM event_timestamp)::INT      AS day_of_week,
    ingested_at

FROM {{ source('raw', 'raw_events') }}

WHERE
    customer_id IS NOT NULL
    AND event_type IS NOT NULL
    AND event_timestamp IS NOT NULL
    AND event_timestamp >= '{{ var("start_date") }}'
    AND event_timestamp <= NOW()
