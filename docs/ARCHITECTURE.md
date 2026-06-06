# Customer360 Data Platform — Architecture Documentation

## System Overview

The Customer360 Data Platform is a production-grade, real-time customer intelligence system processing 10M+ streaming events using modern data engineering tools.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    Customer360 Data Platform                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────────┐      ┌──────────────┐      ┌────────────────────┐    │
│  │   Synthetic  │      │    Olist     │      │   IBM Telco Churn  │    │
│  │ Event Stream │      │  E-Commerce  │      │    (ML Dataset)    │    │
│  │  (10M+ rows) │      │  (100k rows) │      │      (7k rows)     │    │
│  └──────┬───────┘      └──────┬───────┘      └──────────┬─────────┘    │
│         │                     │                          │               │
│         ▼                     ▼                          ▼               │
│  ┌──────────────┐      ┌──────────────┐      ┌────────────────────┐    │
│  │    Kafka     │      │  Batch ETL   │      │   Feature Store    │    │
│  │   Cluster    │      │   (Airflow)  │      │   + ML Pipeline    │    │
│  └──────┬───────┘      └──────┬───────┘      └──────────┬─────────┘    │
│         │                     │                          │               │
│         ▼                     │                          │               │
│  ┌──────────────┐             │                          │               │
│  │    Spark     │             │                          │               │
│  │  Streaming   │             │                          │               │
│  └──────┬───────┘             │                          │               │
│         │                     │                          │               │
│         ▼                     ▼                          │               │
│  ┌──────────────────────────────────────────────────┐   │               │
│  │              MinIO Data Lake                      │   │               │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │   │               │
│  │  │  Bronze  │→ │  Silver  │→ │     Gold     │   │   │               │
│  │  │  (Raw)   │  │(Cleaned) │  │  (Business)  │   │   │               │
│  │  └──────────┘  └──────────┘  └──────────────┘   │   │               │
│  └───────────────────────────┬──────────────────────┘   │               │
│                              │                           │               │
│                              ▼                           ▼               │
│                   ┌────────────────────────────────────────────────┐    │
│                   │          PostgreSQL Warehouse                   │    │
│                   │  fact_orders │ fact_sessions │ fact_txns       │    │
│                   │  dim_customer │ dim_product │ dim_region       │    │
│                   └───────────────────┬─────────────────────────────┘   │
│                                       │                                  │
│                         ┌─────────────┼─────────────┐                   │
│                         ▼             ▼             ▼                    │
│                   ┌──────────┐ ┌──────────┐ ┌──────────────┐           │
│                   │   dbt    │ │ Superset │ │  Prometheus  │           │
│                   │ Models   │ │Dashboard │ │  + Grafana   │           │
│                   └──────────┘ └──────────┘ └──────────────┘           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Component Details

### 1. Data Ingestion Layer

#### Synthetic Event Generator
- **Tool**: Python + Faker
- **Output**: 10M+ events
- **Event Types**: Login, Logout, ProductView, Search, AddToCart, Purchase, Refund, Subscription, PaymentFailure
- **Format**: Partitioned Parquet files
- **Realism**: 95% login success, UPI payment preference (35%), mobile-first (55%), India-centric geography

#### Kafka Producer
- **Library**: confluent-kafka
- **Topics**: 6 (customer-login, product-events, cart-events, purchase-events, payment-events, refund-events)
- **Throughput**: Configurable (default 5K events/sec)
- **Monitoring**: Prometheus metrics (published count, failed count, latency)

---

### 2. Stream Processing Layer

#### Spark Structured Streaming
- **Version**: Apache Spark 3.5
- **Mode**: Micro-batch (30-second triggers)
- **Pipeline**:
  1. Kafka source (all 6 topics)
  2. JSON parsing + schema validation
  3. Data quality checks (null handling, deduplication, timestamp validation)
  4. Triple sink:
     - **Bronze**: Raw events (all)
     - **Silver**: Clean events (DQ passed)
     - **DLQ**: Failed events
- **Partitioning**: Hive-style (`topic/year/month/day/hour`)

#### Real-Time Aggregations
- Revenue per hour/region (tumbling windows)
- Most viewed products (sliding windows: 1hr duration, 15min slide)
- Conversion funnel (views → cart → purchase)
- Active users (5-minute windows)

---

### 3. Data Lake Layer (MinIO)

#### Medallion Architecture

| Layer | Path | Content | Format | Retention |
|-------|------|---------|--------|-----------|
| **Bronze** | `customer360-bronze/events/` | Raw Kafka JSON | Parquet | 90 days |
| **Silver** | `customer360-silver/events/` | Cleaned, validated | Parquet | 1 year |
| **Gold** | `customer360-gold/` | Business aggregates | Parquet | Indefinite |

#### Buckets
- `customer360-bronze`
- `customer360-silver`
- `customer360-gold`
- `customer360-features` (ML feature snapshots)

---

### 4. Orchestration Layer (Apache Airflow)

#### DAG Schedule

| DAG | Schedule | SLA | Purpose |
|-----|----------|-----|---------|
| `dag_kafka_to_bronze` | */5 * * * * | 10 min | Flush Kafka → Bronze |
| `dag_bronze_to_silver` | */30 * * * * | 45 min | Clean Bronze → Silver (pandas DQ) |
| `dag_silver_to_gold` | @hourly | 1 hr | Aggregate Silver → Gold (revenue, customer_360, product_perf) |
| `dag_gold_to_warehouse` | 0 */2 * * * | 2 hrs | Load Gold → PostgreSQL (dim_customer, fact_orders, revenue_metrics) |
| `dag_feature_engineering` | @daily | 3 hrs | Compute RFM + behavioral features |
| `dag_model_retraining` | 0 2 * * 0 | 4 hrs | Weekly XGBoost retraining |

#### DAG Dependencies
```
dag_kafka_to_bronze → dag_bronze_to_silver → dag_silver_to_gold → dag_gold_to_warehouse
                                                                  ↓
                                                          dag_feature_engineering
                                                                  ↓
                                                          dag_model_retraining
```

---

### 5. Data Warehouse Layer (PostgreSQL)

#### Star Schema

**Fact Tables**:
- `fact_orders` — purchase events (grain: 1 row/order)
- `fact_transactions` — all financial events (purchases, refunds, subscription, failures)
- `fact_sessions` — user session aggregates

**Dimension Tables**:
- `dim_customer` — customer master (updated by Airflow)
- `dim_product` — product catalog
- `dim_region` — geography (India states + international)
- `dim_time` — calendar dimension (pre-populated 2025-2027)
- `dim_payment_method` — payment type lookup

**Gold Tables**:
- `revenue_metrics` — monthly revenue by region
- `customer_ltv` — LTV scoring
- `product_performance` — funnel metrics
- `customer_retention_monthly` — cohort analysis

**ML Tables**:
- `feature_store` — daily customer feature snapshots
- `customer_churn_scores` — XGBoost predictions

**Monitoring Tables**:
- `pipeline_run_log` — Airflow DAG execution history
- `data_quality_results` — DQ check results

---

### 6. Transformation Layer (dbt)

#### Model Layers

**Staging** (`stg_*`):
- `stg_events` — type casting, timestamp parsing
- `stg_orders` — order enrichment
- `stg_customers` — activity status tagging

**Marts** (`marts/*`):
- `customer_lifetime_value` — LTV scoring (platinum/gold/silver/bronze segments)
- `monthly_revenue` — MoM growth, YTD revenue
- `customer_retention` — cohort-based retention curves
- `product_analytics` — view→cart→purchase funnel

#### Tests
- Schema tests (not_null, unique, accepted_values)
- Custom tests:
  - `test_no_negative_revenue`
  - `test_ltv_score_range` (0-100)

---

### 7. ML Pipeline

#### Feature Engineering
- **RFM Features**: recency_days, frequency, monetary, avg_purchase_value
- **Behavioral Features**: cart_abandonment_rate, search_to_view_ratio, refund_rate
- **Engagement Features**: days_since_last_login, avg_session_duration
- **Temporal Features**: monthly_orders, purchase_frequency_per_month

#### Churn Prediction Model
- **Algorithm**: XGBoost Classifier
- **Target**: Customers with recency > 90 days = churned
- **Features**: 11 engineered features
- **Performance**: AUC-ROC ~0.85-0.90 (depending on data quality)
- **Training**: Weekly retraining via Airflow DAG
- **Inference**: Batch scoring → `customer_churn_scores` table
- **Segments**: low_risk (<30%), medium_risk (30-60%), high_risk (>60%)

---

### 8. Analytics Layer

#### Apache Superset
- **Connection**: PostgreSQL warehouse
- **Dashboards**:
  1. Executive Dashboard (revenue, customers, orders, growth)
  2. Customer Dashboard (retention, churn, LTV)
  3. Product Dashboard (top products, funnel, conversions)

#### Grafana
- **Data Sources**: Prometheus + PostgreSQL
- **Dashboards**:
  - Pipeline Health (Kafka lag, Spark throughput, Airflow DAG status)
  - Data Quality (DQ rule pass/fail rates)

---

### 9. Monitoring Layer

#### Prometheus
- **Scrape Targets**:
  - Kafka JMX metrics (lag, throughput)
  - Spark master/worker metrics
  - Custom producer metrics (`kafka_events_published_total`, `kafka_publish_latency_seconds`)
  - PostgreSQL exporter (connections, query performance)

#### Grafana
- **Alerts**:
  - Kafka lag > 10,000 messages
  - Airflow DAG failure
  - Data quality failure rate > 1%

---

## Data Flow

```
1. Event Generator → data/synthetic/*.parquet (10M events)
2. Kafka Producer → Kafka Topics (6 topics, 3-6 partitions each)
3. Spark Streaming → MinIO Bronze (raw Parquet, partitioned by topic/date/hour)
4. Airflow Bronze→Silver DAG → MinIO Silver (cleaned Parquet, partitioned by event_type/date)
5. Airflow Silver→Gold DAG → MinIO Gold (aggregated: revenue_by_region, customer_360, product_performance)
6. Airflow Gold→Warehouse DAG → PostgreSQL (upserts to dim_customer, fact_orders, revenue_metrics)
7. dbt → PostgreSQL marts (LTV, retention, monthly_revenue, product_analytics)
8. Airflow Feature Engineering DAG → PostgreSQL feature_store
9. Airflow Model Retraining DAG → XGBoost → PostgreSQL customer_churn_scores
10. Superset/Grafana → Dashboards (consume from PostgreSQL + Prometheus)
```

---

## Performance Characteristics

| Component | Throughput | Latency | Resource Usage |
|-----------|-----------|---------|----------------|
| Event Generator | 50K events/sec | — | 2 CPU cores, 4GB RAM |
| Kafka Producer | 5K events/sec | <10ms | 1 CPU core, 2GB RAM |
| Spark Streaming | 20K events/sec | 30s micro-batch | 4 CPU cores, 8GB RAM |
| Bronze→Silver DAG | 100K records/run | ~5 min | 2 CPU cores, 4GB RAM |
| Silver→Gold DAG | 500K records/run | ~15 min | 2 CPU cores, 4GB RAM |
| Warehouse Load | 10K rows/min | ~