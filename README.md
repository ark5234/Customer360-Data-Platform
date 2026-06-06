# Customer360 Data Platform

> A production-grade, real-time customer intelligence platform processing **10M+ streaming events** using Kafka, Spark Streaming, Airflow, PostgreSQL, dbt, and Docker.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Customer360 Data Platform                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐ │
│  │   Synthetic  │     │    Olist     │     │   IBM Telco Churn    │ │
│  │ Event Stream │     │  E-Commerce  │     │     (ML Dataset)     │ │
│  │  (10M+ rows) │     │  (100k rows) │     │     (7k rows)        │ │
│  └──────┬───────┘     └──────┬───────┘     └──────────┬───────────┘ │
│         │                    │                          │             │
│         ▼                    ▼                          ▼             │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────────┐ │
│  │    Kafka     │     │  Batch ETL   │     │   Feature Store      │ │
│  │   Cluster    │     │   (Airflow)  │     │   + ML Pipeline      │ │
│  └──────┬───────┘     └──────┬───────┘     └──────────┬───────────┘ │
│         │                    │                          │             │
│         ▼                    │                          │             │
│  ┌──────────────┐            │                          │             │
│  │    Spark     │            │                          │             │
│  │  Streaming   │            │                          │             │
│  └──────┬───────┘            │                          │             │
│         │                    │                          │             │
│         ▼                    ▼                          │             │
│  ┌─────────────────────────────────────────────────┐   │             │
│  │              MinIO Data Lake                     │   │             │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────┐  │   │             │
│  │  │  Bronze  │→ │  Silver  │→ │     Gold     │  │   │             │
│  │  │  (Raw)   │  │(Cleaned) │  │  (Business)  │  │   │             │
│  │  └──────────┘  └──────────┘  └──────────────┘  │   │             │
│  └───────────────────────────┬─────────────────────┘   │             │
│                               │                          │             │
│                               ▼                          ▼             │
│                    ┌──────────────────────────────────────────────┐   │
│                    │          PostgreSQL Warehouse                  │   │
│                    │  fact_orders │ fact_sessions │ fact_txns      │   │
│                    │  dim_customer │ dim_product │ dim_region      │   │
│                    └───────────────────┬──────────────────────────┘   │
│                                        │                               │
│                          ┌─────────────┼─────────────┐                │
│                          ▼             ▼              ▼                │
│                    ┌──────────┐ ┌──────────┐ ┌──────────────┐        │
│                    │   dbt    │ │ Superset │ │  Prometheus  │        │
│                    │ Models   │ │Dashboard │ │  + Grafana   │        │
│                    └──────────┘ └──────────┘ └──────────────┘        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Event Streaming | Apache Kafka 7.5 |
| Stream Processing | Apache Spark 3.5 (Structured Streaming) |
| Orchestration | Apache Airflow 2.8 |
| Data Lake | MinIO (S3-compatible) |
| Data Warehouse | PostgreSQL 15 |
| Transformations | dbt-core |
| Analytics | Apache Superset / Power BI |
| ML | XGBoost, scikit-learn, MLflow |
| Monitoring | Prometheus + Grafana |
| Containerization | Docker + Docker Compose |
| Language | Python 3.11 |

---

## Project Structure

```
customer360-data-platform/
├── producer/               # Kafka event producers
│   ├── event_generator.py  # 10M+ synthetic event generation
│   ├── kafka_producer.py   # Kafka publisher
│   └── schemas.py          # Pydantic event schemas
├── consumer/               # Kafka consumers
├── spark_jobs/             # Spark Streaming jobs
│   ├── streaming_processor.py
│   ├── aggregations.py
│   └── data_quality.py
├── airflow/
│   └── dags/              # 6 orchestration DAGs
├── dbt/                   # Transformation models
│   ├── models/
│   │   ├── staging/       # Raw → Staging
│   │   ├── intermediate/  # Business logic
│   │   └── marts/         # Analytics-ready
├── warehouse/
│   └── migrations/        # PostgreSQL DDL scripts
├── ml/
│   ├── features/          # Feature engineering
│   └── models/            # Churn prediction
├── monitoring/
│   ├── prometheus/
│   └── grafana/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Quick Start

### Prerequisites

- Docker Desktop (16GB RAM recommended)
- Python 3.11+
- Git

### 1. Clone & Configure

```bash
git clone https://github.com/yourusername/customer360-data-platform
cd customer360-data-platform
cp .env.example .env
```

### 2. Start Infrastructure

```bash
docker compose up -d
```

Services start on:

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| Airflow | http://localhost:8081 | admin / admin |
| Spark UI | http://localhost:8082 | — |
| MinIO | http://localhost:9001 | customer360 / customer360secret |
| Grafana | http://localhost:3000 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| **Admin Control Panel** | **http://localhost:5000** | **—** |

### 3. Generate Synthetic Data

```bash
pip install -r requirements.txt
python producer/event_generator.py --events 10000000 --output data/synthetic/
```

### 4. Start Kafka Producer

```bash
python producer/kafka_producer.py
```

### 5. Submit Spark Streaming Job

```bash
docker exec spark-master spark-submit \
  --master spark://localhost:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-apps/streaming_processor.py
```

### 6. Enable Airflow DAGs

Navigate to http://localhost:8081 and enable:
- `dag_kafka_to_bronze`
- `dag_bronze_to_silver`
- `dag_silver_to_gold`
- `dag_gold_to_warehouse`
- `dag_feature_engineering`
- `dag_model_retraining`

### 7. Run dbt Transformations

```bash
cd dbt
dbt deps
dbt run
dbt test
```

---

## Data Architecture

### Medallion Architecture (Data Lake)

```
Bronze  →  Raw Kafka events (JSON, partitioned by date/hour)
Silver  →  Cleaned, deduplicated, schema-validated Parquet
Gold    →  Business aggregates, customer 360 views, KPIs
```

### Warehouse Star Schema

```
fact_orders ──── dim_customer
     │       ──── dim_product
     │       ──── dim_region
     │       ──── dim_time
fact_sessions
fact_transactions
```

---

## Datasets Used

| Dataset | Source | Size | Purpose |
|---------|--------|------|---------|
| Synthetic Events | Python + Faker | 10M+ rows | Kafka streaming, Spark processing |
| Olist E-Commerce | Kaggle | ~100k orders | Warehouse modeling, analytics |
| IBM Telco Churn | Kaggle | 7,043 rows | ML pipeline, churn prediction |

---

## Resume Bullets

```
• Architected a real-time customer intelligence platform processing 10M+ streaming events
  using Kafka, Spark Streaming, Airflow, PostgreSQL, and Docker

• Built distributed ETL pipelines with automated orchestration, schema validation, and
  medallion architecture (Bronze/Silver/Gold) data lake design

• Designed dimensional warehouse models and dbt transformation workflows powering
  customer retention, revenue, and product analytics dashboards

• Implemented observability using Prometheus and Grafana while generating ML-ready
  feature stores for downstream churn prediction models
```

---

## License

MIT
