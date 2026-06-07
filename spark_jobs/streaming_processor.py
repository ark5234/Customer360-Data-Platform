"""
Customer360 Data Platform
Spark Structured Streaming Processor

Consumes from Kafka, applies:
- Schema validation
- Null handling
- Deduplication
- Timestamp normalization
- Data quality checks

Then writes to MinIO:
  Bronze → raw JSON
  Silver → cleaned Parquet

Submit with:
    spark-submit \
      --master spark://localhost:7077 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
                 org.apache.hadoop:hadoop-aws:3.3.4 \
      streaming_processor.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# ─────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Customer360-StreamingProcessor")
        .config(
            "spark.sql.streaming.checkpointLocation", "/tmp/customer360-checkpoints"
        )
        # MinIO / S3A config
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "customer360")
        .config("spark.hadoop.fs.s3a.secret.key", "customer360secret")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Performance
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.backpressure.enabled", "true")
        .config(
            "spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false"
        )
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# Event Schemas
# ─────────────────────────────────────────────

BASE_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("device", StringType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("kafka_topic", StringType(), True),
        StructField("ingestion_time", StringType(), True),
        StructField("schema_version", StringType(), True),
        # Event-specific fields
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
        StructField("order_id", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("search_query", StringType(), True),
        StructField("results_count", IntegerType(), True),
        StructField("session_duration_seconds", IntegerType(), True),
        StructField("login_method", StringType(), True),
        StructField("success", BooleanType(), True),
        StructField("failure_reason", StringType(), True),
        StructField("cart_total", DoubleType(), True),
        StructField("refund_amount", DoubleType(), True),
        StructField("plan", StringType(), True),
        StructField("amount", DoubleType(), True),
    ]
)


# ─────────────────────────────────────────────
# Data Quality Checks
# ─────────────────────────────────────────────


def apply_quality_checks(df):
    """
    Tag each event with quality flags.
    Events with critical failures are routed to DLQ.
    """
    return (
        df.withColumn(
            "dq_missing_customer_id",
            F.col("customer_id").isNull() | (F.col("customer_id") == ""),
        )
        .withColumn("dq_missing_event_type", F.col("event_type").isNull())
        .withColumn("dq_invalid_timestamp", F.col("event_timestamp").isNull())
        .withColumn(
            "dq_negative_amount",
            F.when(
                F.col("total_amount").isNotNull(), F.col("total_amount") < 0
            ).otherwise(F.lit(False)),
        )
        .withColumn(
            "dq_future_timestamp", F.col("event_timestamp") > F.current_timestamp()
        )
        .withColumn(
            "dq_passed",
            ~F.col("dq_missing_customer_id")
            & ~F.col("dq_missing_event_type")
            & ~F.col("dq_invalid_timestamp")
            & ~F.col("dq_negative_amount"),
        )
    )


# ─────────────────────────────────────────────
# Transformations
# ─────────────────────────────────────────────


def clean_and_normalize(df):
    """Apply all cleaning and normalization transformations."""
    return (
        df
        # Parse and normalize timestamp
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
        # Normalize customer_id (uppercase, trim)
        .withColumn("customer_id", F.upper(F.trim(F.col("customer_id"))))
        # Normalize event_type (uppercase, trim)
        .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
        # Normalize device type
        .withColumn(
            "device",
            F.when(F.lower(F.col("device")).isin(["mobile", "app"]), "Mobile")
            .when(F.lower(F.col("device")) == "desktop", "Desktop")
            .when(F.lower(F.col("device")) == "tablet", "Tablet")
            .otherwise("Unknown"),
        )
        # Clean region
        .withColumn("region", F.initcap(F.trim(F.col("region"))))
        # Ensure non-negative amounts
        .withColumn("total_amount", F.abs(F.col("total_amount")))
        .withColumn("price", F.abs(F.col("price")))
        # Add processing metadata
        .withColumn("processing_timestamp", F.current_timestamp())
        .withColumn("processing_date", F.current_date())
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
        .withColumn("event_hour", F.hour(F.col("event_timestamp")))
        # Add partition columns
        .withColumn("year", F.year(F.col("event_timestamp")))
        .withColumn("month", F.month(F.col("event_timestamp")))
        .withColumn("day", F.dayofmonth(F.col("event_timestamp")))
    )


def deduplicate(df):
    """Remove duplicate events based on event_id."""
    return df.dropDuplicates(["event_id"])


# ─────────────────────────────────────────────
# Streaming Sources
# ─────────────────────────────────────────────

KAFKA_TOPICS = (
    "customer-login,"
    "product-events,"
    "cart-events,"
    "purchase-events,"
    "payment-events,"
    "refund-events"
)


def read_kafka_stream(spark: SparkSession):
    """Create unified Kafka stream from all topics."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 50000)
        .option("failOnDataLoss", "false")
        .load()
    )


# ─────────────────────────────────────────────
# Streaming Sinks
# ─────────────────────────────────────────────


def write_bronze(df, checkpoint: str):
    """Write raw events to bronze layer (MinIO)."""
    return (
        df.writeStream.format("parquet")
        .option("path", "s3a://customer360-bronze/events/")
        .option("checkpointLocation", f"{checkpoint}/bronze")
        .partitionBy("kafka_topic", "year", "month", "day")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )


def write_silver(df, checkpoint: str):
    """Write cleaned events to silver layer (MinIO)."""
    return (
        df.filter(F.col("dq_passed"))
        .writeStream.format("parquet")
        .option("path", "s3a://customer360-silver/events/")
        .option("checkpointLocation", f"{checkpoint}/silver")
        .partitionBy("event_type", "year", "month", "day")
        .trigger(processingTime="30 seconds")
        .outputMode("append")
        .start()
    )


def write_dlq(df, checkpoint: str):
    """Route failed quality checks to dead letter queue."""
    return (
        df.filter(~F.col("dq_passed"))
        .writeStream.format("parquet")
        .option("path", "s3a://customer360-bronze/dlq/")
        .option("checkpointLocation", f"{checkpoint}/dlq")
        .trigger(processingTime="60 seconds")
        .outputMode("append")
        .start()
    )


def write_console(df):
    """Debug sink — print to console."""
    return (
        df.writeStream.format("console")
        .option("truncate", "false")
        .option("numRows", 5)
        .trigger(processingTime="10 seconds")
        .outputMode("append")
        .start()
    )


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 60)
    print("  Customer360 — Spark Structured Streaming")
    print("=" * 60)
    print(f"  Topics: {KAFKA_TOPICS}")
    print("=" * 60)

    # 1. Read from Kafka
    raw_stream = read_kafka_stream(spark)

    # 2. Parse JSON payload
    parsed = raw_stream.select(
        F.col("topic").alias("kafka_topic_received"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), BASE_SCHEMA).alias("data"),
    ).select("kafka_topic_received", "partition", "offset", "kafka_timestamp", "data.*")

    # 3. Clean and normalize
    cleaned = clean_and_normalize(parsed)

    # 4. Quality checks
    quality_checked = apply_quality_checks(cleaned)

    # 5. Deduplicate
    deduplicated = deduplicate(quality_checked)

    CHECKPOINT = "/tmp/customer360-checkpoints"

    # 6. Write to sinks
    bronze_query = write_bronze(deduplicated, CHECKPOINT)
    silver_query = write_silver(deduplicated, CHECKPOINT)
    dlq_query = write_dlq(deduplicated, CHECKPOINT)

    print("Streaming queries started:")
    print(f"  Bronze: {bronze_query.id}")
    print(f"  Silver: {silver_query.id}")
    print(f"  DLQ:    {dlq_query.id}")

    # Wait for termination
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
