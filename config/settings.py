"""
Customer360 Data Platform — Central Configuration
All tunable parameters in one place. Override via environment variables or .env.
"""

import os
from dataclasses import dataclass, field
from typing import List


# ──────────────────────────────────────────────────────────────────────────────
# Kafka Configuration
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topics: List[str] = field(default_factory=lambda: [
        "customer-login",
        "product-events",
        "cart-events",
        "purchase-events",
        "payment-events",
        "refund-events",
    ])
    group_id: str = os.getenv("KAFKA_GROUP_ID", "customer360-consumer")
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    max_poll_records: int = 500
    session_timeout_ms: int = 30_000
    request_timeout_ms: int = 40_000


# ──────────────────────────────────────────────────────────────────────────────
# MinIO / Data Lake Configuration
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class MinIOConfig:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY", "customer360")
    secret_key: str = os.getenv("MINIO_SECRET_KEY", "customer360secret")
    secure: bool = False

    bucket_bronze: str = "customer360-bronze"
    bucket_silver: str = "customer360-silver"
    bucket_gold: str = "customer360-gold"
    bucket_features: str = "customer360-features"


# ──────────────────────────────────────────────────────────────────────────────
# PostgreSQL Configuration
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class PostgresConfig:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB", "customer360_warehouse")
    user: str = os.getenv("POSTGRES_USER", "customer360")
    password: str = os.getenv("POSTGRES_PASSWORD", "customer360secret")

    @property
    def dsn(self) -> str:
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


# ──────────────────────────────────────────────────────────────────────────────
# Spark Configuration
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class SparkConfig:
    master: str = os.getenv("SPARK_MASTER", "local[*]")
    app_name: str = "Customer360-StreamProcessor"
    trigger_interval_seconds: int = 30
    checkpoint_dir: str = "/tmp/customer360-checkpoints"
    batch_size: int = 10_000


# ──────────────────────────────────────────────────────────────────────────────
# ML Configuration
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class MLConfig:
    churn_threshold_days: int = int(os.getenv("CHURN_THRESHOLD_DAYS", "90"))
    model_version: str = "latest"
    feature_snapshot_days: int = 1      # How far back to look for features
    min_training_samples: int = 1_000   # Minimum samples required to train
    cv_folds: int = 5

    xgb_params: dict = field(default_factory=lambda: {
        "n_estimators": 300,
        "max_depth": 6,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "min_child_weight": 5,
        "scale_pos_weight": 3,
        "eval_metric": "auc",
        "random_state": 42,
        "n_jobs": -1,
    })


# ──────────────────────────────────────────────────────────────────────────────
# Event Generator Configuration
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class GeneratorConfig:
    num_customers: int = int(os.getenv("NUM_CUSTOMERS", "50000"))
    num_events: int = int(os.getenv("NUM_EVENTS", "10_000_000"))
    events_per_second: int = int(os.getenv("EVENTS_PER_SECOND", "5000"))
    batch_size: int = 1_000
    regions: List[str] = field(default_factory=lambda: [
        "Maharashtra", "Karnataka", "Delhi", "Tamil Nadu", "West Bengal",
        "Telangana", "Gujarat", "Rajasthan", "Uttar Pradesh", "Kerala",
    ])


# ──────────────────────────────────────────────────────────────────────────────
# Singleton Instances
# ──────────────────────────────────────────────────────────────────────────────
kafka_config = KafkaConfig()
minio_config = MinIOConfig()
postgres_config = PostgresConfig()
spark_config = SparkConfig()
ml_config = MLConfig()
generator_config = GeneratorConfig()
