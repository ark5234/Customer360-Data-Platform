"""
Customer360 Data Platform
Spark Streaming Aggregations

Calculates real-time business metrics:
- Revenue per hour / per region
- Most viewed products
- Conversion rate
- Active users
"""

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Customer360-Aggregations")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def revenue_per_hour(df):
    """Calculate revenue aggregated by event hour."""
    return (
        df.filter(F.col("event_type") == "PURCHASE")
        .filter(F.col("total_amount").isNotNull())
        .withWatermark("event_timestamp", "1 hour")
        .groupBy(F.window(F.col("event_timestamp"), "1 hour"), F.col("region"))
        .agg(
            F.sum("total_amount").alias("total_revenue"),
            F.count("event_id").alias("order_count"),
            F.avg("total_amount").alias("avg_order_value"),
            F.max("total_amount").alias("max_order_value"),
            F.min("total_amount").alias("min_order_value"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


def revenue_per_region(df):
    """Revenue breakdown by region (5-minute windows)."""
    return (
        df.filter(F.col("event_type") == "PURCHASE")
        .withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window(F.col("event_timestamp"), "5 minutes"),
            F.col("region"),
            F.col("country"),
        )
        .agg(
            F.sum("total_amount").alias("revenue"),
            F.count("order_id").alias("orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("window_start", F.col("window.start"))
        .drop("window")
    )


def most_viewed_products(df):
    """Top viewed products in rolling 1-hour windows."""
    return (
        df.filter(F.col("event_type") == "PRODUCT_VIEW")
        .filter(F.col("product_id").isNotNull())
        .withWatermark("event_timestamp", "1 hour")
        .groupBy(
            F.window(F.col("event_timestamp"), "1 hour", "15 minutes"),
            F.col("product_id"),
            F.col("product_name"),
            F.col("category"),
        )
        .agg(
            F.count("event_id").alias("view_count"),
            F.avg("price").alias("avg_price"),
            F.countDistinct("customer_id").alias("unique_viewers"),
        )
        .withColumn("window_start", F.col("window.start"))
        .drop("window")
    )


def conversion_rate(df):
    """
    Conversion funnel: views → cart → purchase per 1-hour window.
    """
    return (
        df.withWatermark("event_timestamp", "1 hour")
        .groupBy(F.window(F.col("event_timestamp"), "1 hour"), F.col("region"))
        .agg(
            F.sum(F.when(F.col("event_type") == "PRODUCT_VIEW", 1).otherwise(0)).alias(
                "views"
            ),
            F.sum(F.when(F.col("event_type") == "ADD_TO_CART", 1).otherwise(0)).alias(
                "cart_adds"
            ),
            F.sum(F.when(F.col("event_type") == "PURCHASE", 1).otherwise(0)).alias(
                "purchases"
            ),
        )
        .withColumn(
            "view_to_cart_rate",
            F.when(F.col("views") > 0, F.col("cart_adds") / F.col("views")).otherwise(
                0
            ),
        )
        .withColumn(
            "cart_to_purchase_rate",
            F.when(
                F.col("cart_adds") > 0, F.col("purchases") / F.col("cart_adds")
            ).otherwise(0),
        )
        .withColumn(
            "overall_conversion_rate",
            F.when(F.col("views") > 0, F.col("purchases") / F.col("views")).otherwise(
                0
            ),
        )
        .withColumn("window_start", F.col("window.start"))
        .drop("window")
    )


def active_users(df):
    """Count of unique active users per 5-minute window."""
    return (
        df.withWatermark("event_timestamp", "10 minutes")
        .groupBy(
            F.window(F.col("event_timestamp"), "5 minutes"),
            F.col("device"),
            F.col("region"),
        )
        .agg(
            F.countDistinct("customer_id").alias("active_users"),
            F.count("event_id").alias("total_events"),
            F.countDistinct("session_id").alias("active_sessions"),
        )
        .withColumn("window_start", F.col("window.start"))
        .drop("window")
    )


def run_aggregations(spark: SparkSession, silver_path: str, output_path: str):
    """Read from silver layer, compute aggregations, write to gold."""
    silver_df = (
        spark.readStream.format("parquet")
        .schema(...)  # Use the silver schema
        .option("path", silver_path)
        .load()
    )

    checkpoint_base = "/tmp/customer360-agg-checkpoints"

    # Revenue per hour
    (
        revenue_per_hour(silver_df)
        .writeStream.format("parquet")
        .option("path", f"{output_path}/revenue_per_hour/")
        .option("checkpointLocation", f"{checkpoint_base}/revenue_hour")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
    )

    # Revenue per region
    (
        revenue_per_region(silver_df)
        .writeStream.format("parquet")
        .option("path", f"{output_path}/revenue_per_region/")
        .option("checkpointLocation", f"{checkpoint_base}/revenue_region")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
    )

    # Most viewed products
    (
        most_viewed_products(silver_df)
        .writeStream.format("parquet")
        .option("path", f"{output_path}/top_products/")
        .option("checkpointLocation", f"{checkpoint_base}/top_products")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
    )

    # Conversion rate
    (
        conversion_rate(silver_df)
        .writeStream.format("parquet")
        .option("path", f"{output_path}/conversion_rate/")
        .option("checkpointLocation", f"{checkpoint_base}/conversion")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    spark = create_spark_session()
    run_aggregations(
        spark,
        silver_path="s3a://customer360-silver/events/",
        output_path="s3a://customer360-gold/",
    )
