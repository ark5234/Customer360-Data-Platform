# Customer360 Data Platform — Command Reference

Complete list of commands to run the entire platform.

---

## Initial Setup

```bash
# Clone repository
git clone https://github.com/ark5234/Customer360-Data-Platform.git
cd Customer360-Data-Platform

# Copy and configure environment variables
cp .env.example .env
# Edit .env: set GOOGLE_API_KEY and any other values

# Start all Docker services
docker-compose up -d

# Wait for services to be healthy (2-3 minutes)
docker-compose ps

# Initialize PostgreSQL schema
docker exec -i postgres psql -U customer360 -d customer360_warehouse < warehouse/migrations/01_create_schema.sql

# Install Python dependencies
pip install -r requirements.txt

# Create Kafka topics
python scripts/setup_kafka_topics.py
```

---

## Data Generation & Streaming

```bash
# Generate 100K events (test)
python producer/event_generator.py --events 100000 --output data/synthetic/

# Generate 10M events (full dataset)
python producer/event_generator.py --events 10000000 --output data/synthetic/

# Preview 5 sample events
python producer/event_generator.py --events 100 --preview

# Start Kafka producer (terminal 1)
python producer/kafka_producer.py --source data/synthetic/ --rate 5000

# Start Kafka producer with continuous loop
python producer/kafka_producer.py --source data/synthetic/ --rate 5000 --loop
```

---

## Spark Jobs

```bash
# Start Spark Streaming processor
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/streaming_processor.py

# Start Spark aggregations job
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/aggregations.py

# Run data quality checks (batch)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/data_quality.py
```

---

## Kafka Consumer

```bash
# Start consumer (writes to MinIO Bronze)
python consumer/kafka_consumer.py --topics customer-login,product-events,cart-events,purchase-events,payment-events,refund-events
```

---

## Airflow DAGs

Access Airflow UI: http://localhost:8081 (admin / admin)

**Enable these DAGs via UI:**
- `dag_kafka_to_bronze`
- `dag_bronze_to_silver`
- `dag_silver_to_gold`
- `dag_gold_to_warehouse`
- `dag_feature_engineering`
- `dag_model_retraining`

**Trigger DAGs manually (CLI):**
```bash
# Trigger specific DAG
docker exec airflow-webserver airflow dags trigger dag_kafka_to_bronze

# List all DAGs
docker exec airflow-webserver airflow dags list

# Check DAG run status
docker exec airflow-webserver airflow dags list-runs
```

---

## dbt Transformations

```bash
cd dbt

# Install dbt dependencies
dbt deps

# Run all models (staging → marts)
dbt run

# Run specific model
dbt run --select customer_lifetime_value

# Run tests
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

---

## ML Pipeline

```bash
# Feature engineering
cd ml/features
python feature_engineering.py

# Train churn model
cd ../models
python churn_predictor.py --train

# Score all customers
python churn_predictor.py --predict

# Evaluate model
python churn_predictor.py --evaluate

# Train with specific version
python churn_predictor.py --train --version v1.0
```

---

## LLM / RAG Pipeline

```bash
# Ingest warehouse data into Qdrant vector store
python llm/ingest_to_vectordb.py

# Check Qdrant collection status
curl http://localhost:6333/collections/customer360

# Launch AI Admin Control Panel
python admin_panel/app.py
# Visit http://localhost:5000
```

The Admin Panel provides a chat interface powered by a LangGraph ReAct agent.
You can ask natural-language questions like:
- "Show me the top 10 customers by lifetime value"
- "What is the current pipeline health?"
- "How many high-risk churn customers are there this week?"

---

## Database Operations

```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U customer360 -d customer360_warehouse

# Run SQL query
docker exec -it postgres psql -U customer360 -d customer360_warehouse -c "SELECT COUNT(*) FROM fact_orders;"

# Check table sizes
docker exec -it postgres psql -U customer360 -d customer360_warehouse -c "
SELECT 
    schemaname, 
    tablename, 
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
LIMIT 10;
"

# Backup database
docker exec -t postgres pg_dump -U customer360 -d customer360_warehouse > backup_$(date +%Y%m%d).sql

# Restore database
docker exec -i postgres psql -U customer360 -d customer360_warehouse < backup.sql
```

---

## MinIO Operations

Access MinIO Console: http://localhost:9001 (customer360 / customer360secret)

```bash
# List buckets (using mc CLI inside minio container)
docker exec minio mc ls myminio

# List objects in bronze bucket
docker exec minio mc ls --recursive myminio/customer360-bronze

# Copy file from MinIO to local
docker exec minio mc cp myminio/customer360-bronze/events/year=2026/month=06/day=07/hour=12/batch_001.parquet ./local_backup.parquet
```

---

## Monitoring

```bash
# Access Prometheus
# http://localhost:9090

# Access Grafana
# http://localhost:3000 (admin / admin)

# Check Kafka metrics
# http://localhost:9090/targets

# View producer metrics
curl http://localhost:8000/metrics
```

---

## Service Management

```bash
# View all running services
docker-compose ps

# View logs for specific service
docker logs kafka
docker logs spark-master
docker logs airflow-scheduler
docker logs postgres

# Follow logs in real-time
docker logs -f kafka

# Restart specific service
docker-compose restart kafka

# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v

# Start specific service
docker-compose up -d kafka

# Scale Spark workers
docker-compose up -d --scale spark-worker=3
```

---

## Troubleshooting Commands

```bash
# Check Docker resource usage
docker stats

# Check disk usage
docker system df

# Clean up unused Docker resources
docker system prune -a

# Check Kafka topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Describe Kafka topic
docker exec kafka kafka-topics --describe --topic customer-login --bootstrap-server localhost:9092

# Check consumer group lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group customer360-bronze-writer \
  --describe

# Reset consumer group offset (CAUTION)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group customer360-bronze-writer \
  --reset-offsets --to-earliest --execute --all-topics

# Check PostgreSQL connections
docker exec postgres psql -U customer360 -d customer360_warehouse -c "SELECT count(*) FROM pg_stat_activity;"

# Kill idle PostgreSQL connections
docker exec postgres psql -U customer360 -d customer360_warehouse -c "
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = 'customer360_warehouse' 
  AND state = 'idle' 
  AND state_change < now() - interval '5 minutes';
"

# Check Spark job status
curl http://localhost:8082/api/v1/applications

# Check Airflow scheduler health
docker exec airflow-scheduler airflow jobs check --job-type SchedulerJob --hostname $(hostname)
```

---

## Performance Testing

```bash
# Test Kafka producer throughput
python producer/kafka_producer.py --source data/synthetic/ --rate 0  # unlimited

# Benchmark event generation
time python producer/event_generator.py --events 1000000 --output /tmp/test/

# Check Spark processing rate
# View Spark UI → Streaming tab → http://localhost:8082
```

---

## Data Validation

```bash
# Count events in Bronze layer
docker exec minio mc ls --recursive myminio/customer360-bronze | wc -l

# Count rows in warehouse
docker exec postgres psql -U customer360 -d customer360_warehouse -c "
SELECT 
    'fact_orders' AS table_name, COUNT(*) AS row_count FROM fact_orders
UNION ALL
SELECT 'fact_transactions', COUNT(*) FROM fact_transactions
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'revenue_metrics', COUNT(*) FROM revenue_metrics;
"

# Check latest feature store snapshot
docker exec postgres psql -U customer360 -d customer360_warehouse -c "
SELECT snapshot_date, COUNT(*) AS customer_count
FROM feature_store
GROUP BY snapshot_date
ORDER BY snapshot_date DESC
LIMIT 5;
"

# Check churn scores distribution
docker exec postgres psql -U customer360 -d customer360_warehouse -c "
SELECT 
    churn_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(churn_probability), 3) AS avg_probability
FROM customer_churn_scores
GROUP BY churn_segment
ORDER BY churn_segment;
"
```

---

## Quick Smoke Test

Run this after initial setup to verify everything works:

```bash
# 1. Generate small dataset
python producer/event_generator.py --events 10000 --output data/test/

# 2. Produce to Kafka
timeout 30s python producer/kafka_producer.py --source data/test/ --rate 1000

# 3. Check Kafka UI
# http://localhost:8080 → should see messages in topics

# 4. Trigger Airflow DAG
docker exec airflow-webserver airflow dags trigger dag_kafka_to_bronze

# 5. Check MinIO
# http://localhost:9001 → should see files in bronze bucket

# 6. Run dbt
cd dbt && dbt run --select stg_events

# 7. Check warehouse
docker exec postgres psql -U customer360 -d customer360_warehouse -c "SELECT COUNT(*) FROM fact_orders;"
```

---

## URLs Quick Reference

| Service | URL | Credentials |
|---------|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| Airflow | http://localhost:8081 | admin / admin |
| Spark UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | customer360 / customer360secret |
| Grafana | http://localhost:3000 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| Prometheus | http://localhost:9090 | — |
| Qdrant UI | http://localhost:6333/dashboard | — |
| Admin AI Panel | http://localhost:5000 | — |
| Producer Metrics | http://localhost:8000/metrics | — |

---

## Environment Variables

Create `.env.local` for custom overrides:

```bash
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=customer360
POSTGRES_PASSWORD=customer360secret
POSTGRES_DB=customer360_warehouse

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=customer360
MINIO_SECRET_KEY=customer360secret

# ML
CHURN_THRESHOLD_DAYS=90

# Event Generator
NUM_CUSTOMERS=500000
NUM_PRODUCTS=50000
TOTAL_EVENTS=10000000
```

---

## Production Deployment Checklist

Before deploying to production:

- [ ] Replace `.env` with production credentials
- [ ] Configure external PostgreSQL (not Docker)
- [ ] Use AWS S3 instead of MinIO
- [ ] Set up Airflow on Kubernetes (KEDA/Celery Executor)
- [ ] Configure Kafka cluster (3+ brokers)
- [ ] Set up Spark on EMR/Databricks
- [ ] Enable SSL/TLS for all services
- [ ] Configure backup/disaster recovery
- [ ] Set up alerting (PagerDuty/Slack)
- [ ] Implement data retention policies
- [ ] Add authentication/authorization
- [ ] Set up CI/CD pipeline

---

**For more details, see:** `docs/QUICK_START.md` and `docs/ARCHITECTURE.md`
