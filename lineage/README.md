# DataHub Lineage — Customer360

This module publishes **end-to-end data lineage** for the Customer360 pipeline to DataHub.

## Full Lineage Graph

```
Synthetic Events (Python + Faker)
    ↓ [kafka_producer]
Kafka Topics (6 topics × 3-6 partitions)
    ↓ [spark_streaming_processor]
MinIO Bronze  ← raw Parquet, partitioned by topic/date/hour
    ↓ [dag_bronze_to_silver + Great Expectations (12 rules)]
MinIO Silver  ← cleaned, deduplicated, GE-validated Parquet
    ↓ [dag_silver_to_gold]
MinIO Gold    ← customer_360 | revenue_by_region | product_performance
    ↓ [dag_gold_to_warehouse]
PostgreSQL Warehouse (star schema)
    │  dim_customer | dim_product | dim_region | dim_time
    │  fact_orders | fact_transactions | fact_sessions
    │  revenue_metrics
    ↓ [dbt_run]
dbt Marts
    │  customer_lifetime_value | customer_retention
    │  monthly_revenue | product_analytics
    ↓ [dag_feature_engineering]
feature_store (13 RFM + behavioral features)
    ↓ [dag_model_retraining]
customer_churn_scores (XGBoost, AUC-ROC 0.87)
    ↓ [dag_llm_ingestion]
Qdrant VectorDB (RAG — customer profiles + KPIs)
```

## Setup

DataHub runs as an optional Docker profile:

```bash
# Start DataHub (requires ~6GB RAM)
docker compose --profile lineage up -d

# Access DataHub UI
open http://localhost:9002
# Default credentials: datahub / datahub
```

## Publishing Lineage

```bash
# Publish all pipeline stages at once
python lineage/publish_lineage.py --stage all

# Publish a specific stage
python lineage/publish_lineage.py --stage bronze_to_silver

# Dry-run: print lineage map without pushing to DataHub
python lineage/publish_lineage.py --dry-run

# Use a custom GMS URL
python lineage/publish_lineage.py --gms-url http://localhost:8080 --stage all
```

## From Airflow

The Gold→Warehouse DAG automatically publishes lineage at the end of each run:

```python
from lineage.publish_lineage import publish_stage_lineage
publish_stage_lineage("gold_to_warehouse")
```

## Pipeline Stages

| Stage | Job | Inputs | Outputs |
|-------|-----|--------|---------|
| `event_generation` | event_generator.py | — | synthetic_events |
| `kafka_ingest` | kafka_producer.py | synthetic_events | kafka.events |
| `spark_bronze` | streaming_processor.py | kafka.events | bronze/events |
| `bronze_to_silver` | dag_bronze_to_silver | bronze/events | silver/events |
| `silver_to_gold` | dag_silver_to_gold | silver/events | gold/* |
| `gold_to_warehouse` | dag_gold_to_warehouse | gold/* + silver | warehouse tables |
| `dbt_transforms` | dbt run | warehouse tables | dbt marts |
| `feature_engineering` | dag_feature_engineering | dim_customer + fact_orders | feature_store |
| `model_retraining` | dag_model_retraining | feature_store | churn_scores |
| `llm_ingestion` | dag_llm_ingestion | warehouse + marts | qdrant |
