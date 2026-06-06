# Customer360 Data Platform — Quick Start Guide

Get the platform running in 30 minutes.

## Prerequisites

- Docker Desktop (16GB RAM)
- Python 3.11+
- Git

## 5-Step Setup

### Step 1: Clone and Start Infrastructure (5 min)

```bash
git clone https://github.com/ark5234/Customer360-Data-Platform.git
cd Customer360-Data-Platform
docker-compose up -d
```

Wait for all services to be healthy (~2-3 minutes).

### Step 2: Initialize Database (2 min)

```bash
# Wait for PostgreSQL
docker exec -it postgres pg_isready -U customer360 -d customer360_warehouse

# Create schema
docker exec -i postgres psql -U customer360 -d customer360_warehouse < warehouse/migrations/01_create_schema.sql
```

### Step 3: Install Python Dependencies (3 min)

```bash
pip install -r requirements.txt
```

### Step 4: Create Kafka Topics (1 min)

```bash
python scripts/setup_kafka_topics.py
```

### Step 5: Generate and Stream Data (15 min)

```bash
# Generate 100K events for testing
python producer/event_generator.py --events 100000 --output data/synthetic/

# Start producer (in new terminal)
python producer/kafka_producer.py --source data/synthetic/ --rate 5000

# Start Spark streaming (in new terminal)
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-apps/streaming_processor.py
```

## Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Kafka UI** | http://localhost:8080 | — |
| **Airflow** | http://localhost:8081 | admin / admin |
| **Spark UI** | http://localhost:8082 | — |
| **MinIO** | http://localhost:9001 | customer360 / customer360secret |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Superset** | http://localhost:8088 | admin / admin |

## Next Steps

1. Enable Airflow DAGs in the UI
2. Run dbt transformations: `cd dbt && dbt run`
3. Train ML model: `python ml/models/churn_predictor.py --train`
4. View Grafana dashboards at http://localhost:3000

## Troubleshooting

**Kafka connection refused?**
```bash
docker logs kafka
python scripts/setup_kafka_topics.py
```

**PostgreSQL not ready?**
```bash
docker logs postgres
# Wait 1 more minute and retry
```

**Need to restart everything?**
```bash
docker-compose down
docker-compose up -d
```

## Full Documentation

See `docs/ARCHITECTURE.md` for complete system documentation.
