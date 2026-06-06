-- ============================================================
-- Customer360 Data Platform
-- PostgreSQL Warehouse Schema
-- ============================================================

-- Create dedicated schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS ml;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ────────────────────────────────────────────────────────────
-- AIRFLOW DATABASE
-- ────────────────────────────────────────────────────────────
-- Airflow needs its own database (created at container startup)
-- If running outside Docker, create manually:
-- CREATE DATABASE airflow_db;

-- ────────────────────────────────────────────────────────────
-- DIMENSION TABLES
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id         VARCHAR(20) PRIMARY KEY,
    region              VARCHAR(100),
    country             VARCHAR(100) DEFAULT 'India',
    city                VARCHAR(100),
    preferred_device    VARCHAR(50),
    customer_segment    VARCHAR(50),  -- high_value, medium_value, low_value, churned
    total_events        BIGINT DEFAULT 0,
    total_purchases     BIGINT DEFAULT 0,
    total_spend         NUMERIC(15,2) DEFAULT 0,
    avg_spend           NUMERIC(12,2) DEFAULT 0,
    days_active         INTEGER DEFAULT 0,
    first_seen_date     TIMESTAMP,
    last_seen_date      TIMESTAMP,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id          VARCHAR(20) PRIMARY KEY,
    product_name        VARCHAR(500),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    brand               VARCHAR(200),
    base_price          NUMERIC(12,2),
    rating              NUMERIC(3,1),
    review_count        INTEGER DEFAULT 0,
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMP DEFAULT NOW(),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_region (
    region_id           SERIAL PRIMARY KEY,
    region_name         VARCHAR(100) UNIQUE NOT NULL,
    country             VARCHAR(100) DEFAULT 'India',
    region_tier         VARCHAR(50),  -- metro, tier1, tier2, tier3
    population_bucket   VARCHAR(50),
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id             SERIAL PRIMARY KEY,
    full_date           DATE UNIQUE NOT NULL,
    year                INTEGER,
    quarter             INTEGER,
    month               INTEGER,
    month_name          VARCHAR(20),
    week                INTEGER,
    day_of_month        INTEGER,
    day_of_week         INTEGER,
    day_name            VARCHAR(20),
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_payment_method (
    method_id           SERIAL PRIMARY KEY,
    method_name         VARCHAR(50) UNIQUE NOT NULL,
    method_category     VARCHAR(50),  -- card, upi, banking, cash
    is_digital          BOOLEAN DEFAULT TRUE
);

-- ────────────────────────────────────────────────────────────
-- FACT TABLES
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id            VARCHAR(50) PRIMARY KEY,
    customer_id         VARCHAR(20) REFERENCES dim_customer(customer_id),
    event_timestamp     TIMESTAMP,
    event_date          DATE,
    total_amount        NUMERIC(15,2),
    discount_amount     NUMERIC(12,2) DEFAULT 0,
    tax_amount          NUMERIC(12,2) DEFAULT 0,
    net_amount          NUMERIC(15,2) GENERATED ALWAYS AS (total_amount - discount_amount) STORED,
    payment_method      VARCHAR(50),
    items_count         INTEGER DEFAULT 1,
    region              VARCHAR(100),
    device              VARCHAR(50),
    delivery_type       VARCHAR(50),
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      VARCHAR(50) DEFAULT gen_random_uuid()::TEXT PRIMARY KEY,
    order_id            VARCHAR(50),
    customer_id         VARCHAR(20),
    transaction_type    VARCHAR(50),  -- purchase, refund, subscription, payment_failure
    amount              NUMERIC(15,2),
    payment_method      VARCHAR(50),
    status              VARCHAR(50),  -- success, failed, pending
    failure_reason      VARCHAR(200),
    event_timestamp     TIMESTAMP,
    event_date          DATE,
    region              VARCHAR(100),
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_sessions (
    session_id          VARCHAR(50) PRIMARY KEY,
    customer_id         VARCHAR(20),
    session_start       TIMESTAMP,
    session_end         TIMESTAMP,
    session_duration_s  INTEGER,
    event_count         INTEGER DEFAULT 0,
    page_views          INTEGER DEFAULT 0,
    product_views       INTEGER DEFAULT 0,
    cart_adds           INTEGER DEFAULT 0,
    purchases           INTEGER DEFAULT 0,
    device              VARCHAR(50),
    region              VARCHAR(100),
    converted           BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- RAW TABLES (Staging / Landing)
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_events (
    id                  BIGSERIAL PRIMARY KEY,
    event_id            VARCHAR(50),
    customer_id         VARCHAR(20),
    event_type          VARCHAR(50),
    payload             JSONB,
    kafka_topic         VARCHAR(100),
    event_timestamp     TIMESTAMP,
    ingested_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_events_customer ON raw_events(customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_type ON raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_ts ON raw_events(event_timestamp);

-- ────────────────────────────────────────────────────────────
-- ANALYTICS / GOLD TABLES
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS revenue_metrics (
    id                  SERIAL PRIMARY KEY,
    region              VARCHAR(100),
    year                INTEGER,
    month               INTEGER,
    total_revenue       NUMERIC(20,2),
    order_count         BIGINT,
    avg_order_value     NUMERIC(12,2),
    unique_customers    BIGINT,
    loaded_at           TIMESTAMP DEFAULT NOW(),
    UNIQUE (region, year, month)
);

CREATE TABLE IF NOT EXISTS customer_ltv (
    customer_id         VARCHAR(20) PRIMARY KEY,
    total_spend_12m     NUMERIC(15,2) DEFAULT 0,
    total_orders_12m    INTEGER DEFAULT 0,
    avg_order_value     NUMERIC(12,2) DEFAULT 0,
    ltv_segment         VARCHAR(50),  -- platinum, gold, silver, bronze
    ltv_score           NUMERIC(8,4),
    predicted_ltv_12m   NUMERIC(15,2),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS product_performance (
    product_id          VARCHAR(20) PRIMARY KEY,
    product_name        VARCHAR(500),
    category            VARCHAR(100),
    view_count          BIGINT DEFAULT 0,
    cart_count          BIGINT DEFAULT 0,
    purchase_count      BIGINT DEFAULT 0,
    total_revenue       NUMERIC(15,2) DEFAULT 0,
    avg_price           NUMERIC(12,2),
    cart_rate           NUMERIC(6,4),
    conversion_rate     NUMERIC(6,4),
    updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customer_retention_monthly (
    cohort_month        DATE,
    period_month        DATE,
    cohort_size         INTEGER,
    retained_customers  INTEGER,
    retention_rate      NUMERIC(6,4),
    PRIMARY KEY (cohort_month, period_month)
);

-- ────────────────────────────────────────────────────────────
-- ML TABLES
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS feature_store (
    id                      BIGSERIAL PRIMARY KEY,
    customer_id             VARCHAR(20),
    snapshot_date           DATE,
    feature_set             VARCHAR(50),  -- rfm, behavioral, product, time
    -- RFM features
    recency_days            INTEGER,
    frequency               INTEGER,
    monetary                NUMERIC(15,2),
    avg_purchase_value      NUMERIC(12,2),
    max_purchase_value      NUMERIC(12,2),
    min_purchase_value      NUMERIC(12,2),
    -- Behavioral features
    product_view_count      INTEGER,
    search_count            INTEGER,
    cart_add_count          INTEGER,
    purchase_count          INTEGER,
    login_count             INTEGER,
    cart_abandonment_rate   NUMERIC(6,4),
    -- Derived features
    days_since_last_login   INTEGER,
    monthly_orders          NUMERIC(8,2),
    avg_session_duration    NUMERIC(10,2),
    created_at              TIMESTAMP DEFAULT NOW(),
    updated_at              TIMESTAMP,
    UNIQUE (customer_id, snapshot_date, feature_set)
);

CREATE INDEX IF NOT EXISTS idx_feature_store_customer ON feature_store(customer_id);
CREATE INDEX IF NOT EXISTS idx_feature_store_date ON feature_store(snapshot_date);

CREATE TABLE IF NOT EXISTS customer_churn_scores (
    customer_id         VARCHAR(20) PRIMARY KEY,
    churn_probability   NUMERIC(8,6),
    churn_segment       VARCHAR(20),  -- low_risk, medium_risk, high_risk
    model_version       VARCHAR(50) DEFAULT 'v1.0',
    scored_at           TIMESTAMP DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- MONITORING TABLES
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id                  SERIAL PRIMARY KEY,
    dag_id              VARCHAR(200),
    run_id              VARCHAR(200),
    task_id             VARCHAR(200),
    status              VARCHAR(50),
    records_processed   BIGINT,
    duration_seconds    INTEGER,
    error_message       TEXT,
    started_at          TIMESTAMP,
    completed_at        TIMESTAMP,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    id                  SERIAL PRIMARY KEY,
    rule_name           VARCHAR(200),
    table_name          VARCHAR(200),
    total_records       BIGINT,
    failed_records      BIGINT,
    failure_rate        NUMERIC(8,6),
    passed              BOOLEAN,
    severity            VARCHAR(20),
    checked_at          TIMESTAMP DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────
-- SEED DATA
-- ────────────────────────────────────────────────────────────

INSERT INTO dim_payment_method (method_name, method_category, is_digital) VALUES
    ('credit_card', 'card', TRUE),
    ('debit_card', 'card', TRUE),
    ('upi', 'upi', TRUE),
    ('net_banking', 'banking', TRUE),
    ('wallet', 'wallet', TRUE),
    ('cod', 'cash', FALSE)
ON CONFLICT (method_name) DO NOTHING;

INSERT INTO dim_region (region_name, country, region_tier) VALUES
    ('Maharashtra', 'India', 'metro'),
    ('Karnataka', 'India', 'metro'),
    ('Delhi', 'India', 'metro'),
    ('Tamil Nadu', 'India', 'metro'),
    ('West Bengal', 'India', 'tier1'),
    ('Telangana', 'India', 'metro'),
    ('Gujarat', 'India', 'tier1'),
    ('Rajasthan', 'India', 'tier1'),
    ('Uttar Pradesh', 'India', 'tier1'),
    ('Kerala', 'India', 'tier1'),
    ('Punjab', 'India', 'tier2'),
    ('Madhya Pradesh', 'India', 'tier2'),
    ('Andhra Pradesh', 'India', 'tier2'),
    ('Haryana', 'India', 'tier2'),
    ('Bihar', 'India', 'tier2')
ON CONFLICT (region_name) DO NOTHING;

-- Populate dim_time for 2025-2027
INSERT INTO dim_time (full_date, year, quarter, month, month_name, week, day_of_month, day_of_week, day_name, is_weekend)
SELECT
    d::DATE AS full_date,
    EXTRACT(YEAR FROM d)::INTEGER AS year,
    EXTRACT(QUARTER FROM d)::INTEGER AS quarter,
    EXTRACT(MONTH FROM d)::INTEGER AS month,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(WEEK FROM d)::INTEGER AS week,
    EXTRACT(DAY FROM d)::INTEGER AS day_of_month,
    EXTRACT(DOW FROM d)::INTEGER AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM GENERATE_SERIES('2025-01-01'::DATE, '2027-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (full_date) DO NOTHING;

COMMENT ON TABLE dim_customer IS 'Customer master dimension - updated by Airflow gold_to_warehouse DAG';
COMMENT ON TABLE fact_orders IS 'Purchase event fact table - grain: one row per order';
COMMENT ON TABLE feature_store IS 'ML feature store - daily snapshots per customer';
COMMENT ON TABLE customer_churn_scores IS 'XGBoost churn probability scores per customer';
