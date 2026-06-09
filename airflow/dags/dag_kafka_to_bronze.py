"""
DAG 1: Kafka → Bronze
Runs every 5 minutes.
Triggers the Kafka consumer to flush buffered events to MinIO bronze layer.
"""

from datetime import timedelta

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    "owner": "customer360",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="dag_kafka_to_bronze",
    description="Flush Kafka events to MinIO Bronze layer",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    max_active_runs=1,
    tags=["customer360", "ingestion", "bronze", "kafka"],
) as dag:

    def check_kafka_health(**context):
        """Verify Kafka is reachable before processing."""
        import socket

        try:
            sock = socket.create_connection(("kafka", 29092), timeout=5)
            sock.close()
            print("Kafka is reachable.")
            return True
        except OSError as e:
            raise RuntimeError(f"Kafka not reachable: {e}") from e

    def check_minio_health(**context):
        """Verify MinIO is reachable."""
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )
        buckets = [b.name for b in client.list_buckets()]
        print(f"MinIO buckets: {buckets}")
        required = ["customer360-bronze", "customer360-silver", "customer360-gold"]
        for b in required:
            if b not in buckets:
                raise ValueError(f"Required bucket missing: {b}")
        return True

    def get_kafka_lag(**context):
        """
        Check consumer group lag as a data quality signal.
        (Bypassed because kafka CLI is not installed in the Airflow container)
        """
        lag_info = "Lag check bypassed: kafka-consumer-groups CLI not available in Airflow container."
        print(lag_info)
        context["task_instance"].xcom_push(key="kafka_lag_output", value=lag_info)

    def flush_events_to_bronze(**context):
        """
        Read buffered Parquet files from local staging and upload to MinIO bronze.
        In production, this triggers the consumer process or reads from Kafka directly.
        """
        import glob
        import os
        from datetime import datetime
        from io import BytesIO

        import pandas as pd
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )

        staging_dir = "/opt/airflow/data/staging/"
        os.makedirs(staging_dir, exist_ok=True)

        files = glob.glob(f"{staging_dir}*.parquet")
        total_events = 0

        if not files:
            print("No staging files to process. Producer may not have written yet.")
            return

        now = datetime.utcnow()
        partition = (
            f"year={now.year}/month={now.month:02d}/"
            f"day={now.day:02d}/hour={now.hour:02d}"
        )

        for f in files:
            df = pd.read_parquet(f)
            buf = BytesIO()
            df.to_parquet(buf, index=False)
            buf.seek(0)
            obj_name = f"events/{partition}/{os.path.basename(f)}"
            client.put_object(
                "customer360-bronze",
                obj_name,
                buf,
                length=buf.getbuffer().nbytes,
            )
            total_events += len(df)
            os.remove(f)  # Clear after successful upload
            print(f"Uploaded {len(df):,} events → bronze/{obj_name}")

        print(f"Total events flushed to bronze: {total_events:,}")
        context["task_instance"].xcom_push(key="events_flushed", value=total_events)

    def log_bronze_metrics(**context):
        """Log summary metrics for monitoring."""
        events = (
            context["task_instance"].xcom_pull(
                task_ids="flush_events_to_bronze", key="events_flushed"
            )
            or 0
        )
        execution_date = context["execution_date"]
        print(f"[{execution_date}] Bronze flush complete: {events:,} events written")

    # Tasks
    t_check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    t_check_minio = PythonOperator(
        task_id="check_minio_health",
        python_callable=check_minio_health,
    )

    t_kafka_lag = PythonOperator(
        task_id="get_kafka_lag",
        python_callable=get_kafka_lag,
    )

    t_flush = PythonOperator(
        task_id="flush_events_to_bronze",
        python_callable=flush_events_to_bronze,
    )

    t_log = PythonOperator(
        task_id="log_bronze_metrics",
        python_callable=log_bronze_metrics,
    )

    # Dependencies
    [t_check_kafka, t_check_minio] >> t_kafka_lag >> t_flush >> t_log
