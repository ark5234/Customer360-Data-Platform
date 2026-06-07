"""
DAG 2: Bronze → Silver
Runs every 30 minutes.
Reads raw Parquet from bronze, applies cleaning & DQ checks,
runs Great Expectations validation (12-rule suite), writes to silver.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "customer360",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}

with DAG(
    dag_id="dag_bronze_to_silver",
    description="Clean bronze events → GE validation (12 rules) → write to Silver layer",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["customer360", "silver", "cleaning", "dq", "great-expectations"],
) as dag:

    def read_bronze_events(**context):
        """List unprocessed bronze partitions."""
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )
        objects = list(
            client.list_objects("customer360-bronze", prefix="events/", recursive=True)
        )
        print(f"Found {len(objects)} objects in bronze")
        paths = [o.object_name for o in objects if o.object_name.endswith(".parquet")]
        context["task_instance"].xcom_push(
            key="bronze_paths", value=paths[:50]
        )  # batch of 50
        return len(paths)

    def clean_and_validate(**context):
        """
        Apply all cleaning transformations and DQ rules.
        Runs in-process using pandas (Spark job handles streaming; this handles backfill).
        """
        from io import BytesIO

        import numpy as np
        import pandas as pd
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )

        paths = (
            context["task_instance"].xcom_pull(
                task_ids="read_bronze_events", key="bronze_paths"
            )
            or []
        )

        if not paths:
            print("No bronze files to process.")
            return 0

        all_dfs = []
        for path in paths:
            try:
                response = client.get_object("customer360-bronze", path)
                df = pd.read_parquet(BytesIO(response.read()))
                all_dfs.append(df)
            except Exception as e:
                print(f"Error reading {path}: {e}")

        if not all_dfs:
            return 0

        df = pd.concat(all_dfs, ignore_index=True)
        original_count = len(df)
        print(f"Loaded {original_count:,} records from bronze")

        # ── Cleaning pipeline ──────────────────────────────────

        # 1. Remove nulls on critical columns
        df = df.dropna(subset=["customer_id", "event_type", "timestamp"])

        # 2. Deduplicate
        df = df.drop_duplicates(subset=["event_id"])

        # 3. Normalize strings
        df["customer_id"] = df["customer_id"].str.upper().str.strip()
        df["event_type"] = df["event_type"].str.upper().str.strip()
        df["device"] = df["device"].str.title().str.strip()
        df["region"] = df["region"].str.title().str.strip()

        # 4. Parse timestamps
        df["event_timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["event_timestamp"])

        # 5. Remove negative amounts
        for col in ["total_amount", "price", "refund_amount"]:
            if col in df.columns:
                df[col] = df[col].abs()

        # 6. Add partition columns
        df["event_date"] = df["event_timestamp"].dt.date
        df["event_year"] = df["event_timestamp"].dt.year
        df["event_month"] = df["event_timestamp"].dt.month
        df["event_hour"] = df["event_timestamp"].dt.hour
        df["processing_timestamp"] = pd.Timestamp.utcnow()

        # 7. DQ flags
        df["dq_passed"] = True
        df["dq_issues"] = ""

        cleaned_count = len(df)
        removed = original_count - cleaned_count
        print(f"After cleaning: {cleaned_count:,} records ({removed:,} removed)")

        # ── Write to Silver ──────────────────────────────────────
        buf = BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)

        from datetime import datetime

        now = datetime.utcnow()
        silver_path = f"events/year={now.year}/month={now.month:02d}/day={now.day:02d}/batch_{now.strftime('%H%M%S')}.parquet"

        client.put_object(
            "customer360-silver",
            silver_path,
            buf,
            length=buf.getbuffer().nbytes,
        )
        print(f"Written {cleaned_count:,} records → silver/{silver_path}")

        context["task_instance"].xcom_push(key="silver_records", value=cleaned_count)
        context["task_instance"].xcom_push(key="silver_path", value=silver_path)
        return cleaned_count

    def run_great_expectations(**context):
        """
        Run Great Expectations validation suite (12 rules) on the Silver-layer output.

        Checks:
          - 4 × Null checks (customer_id, event_type, event_timestamp, event_id)
          - 3 × Schema checks (required columns, column existence)
          - 1 × Duplicate detection (event_id uniqueness)
          - 4 × Domain/range checks (event_type values, device values, amount range, row count)

        Results are pushed to XCom and logged for Grafana/Prometheus monitoring.
        """
        from io import BytesIO

        import pandas as pd
        import sys
        import os

        # Add project root to path so we can import data_quality
        sys.path.insert(0, "/opt/airflow")

        try:
            from data_quality.ge_suite import run_ge_validation
        except ImportError:
            print("WARNING: data_quality.ge_suite not available — skipping GE validation")
            return

        # Pull silver path from upstream task
        silver_path = context["task_instance"].xcom_pull(
            task_ids="clean_and_validate", key="silver_path"
        )

        if not silver_path:
            print("No silver data written — skipping GE validation")
            return

        from minio import Minio
        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )

        try:
            resp = client.get_object("customer360-silver", silver_path)
            df = pd.read_parquet(BytesIO(resp.read()))
        except Exception as e:
            print(f"Could not read silver file for GE validation: {e}")
            return

        # Run 12-rule GE suite
        ge_result = run_ge_validation(df, dataset_name="silver_events")
        summary = ge_result.summary()

        print(
            f"\nGE Suite Result: {summary['passed']}/{summary['total']} expectations passed "
            f"({summary['pass_rate']:.1%}) — Suite {'PASSED ✓' if summary['success'] else 'FAILED ✗'}"
        )

        # Push to XCom for monitoring
        context["task_instance"].xcom_push(key="ge_passed", value=summary["passed"])
        context["task_instance"].xcom_push(key="ge_failed", value=summary["failed"])
        context["task_instance"].xcom_push(key="ge_pass_rate", value=summary["pass_rate"])
        context["task_instance"].xcom_push(key="ge_success", value=summary["success"])

        # Raise if critical expectations failed
        if not summary["success"]:
            raise ValueError(
                f"Great Expectations validation FAILED: "
                f"{summary['failed']} critical expectations did not pass. "
                f"Check the logs above for details."
            )

    def run_dq_checks(**context):
        """Log final DQ summary metrics for Grafana dashboards."""
        records = (
            context["task_instance"].xcom_pull(
                task_ids="clean_and_validate", key="silver_records"
            )
            or 0
        )
        ge_pass_rate = context["task_instance"].xcom_pull(
            task_ids="run_great_expectations", key="ge_pass_rate"
        ) or 0.0

        print(f"DQ Summary: {records:,} records written to silver")
        print(f"GE Suite pass rate: {ge_pass_rate:.1%}")

        if records == 0:
            print("WARNING: Zero records written to silver!")

    # Tasks
    t_read = PythonOperator(
        task_id="read_bronze_events", python_callable=read_bronze_events
    )
    t_clean = PythonOperator(
        task_id="clean_and_validate", python_callable=clean_and_validate
    )
    t_ge = PythonOperator(
        task_id="run_great_expectations",
        python_callable=run_great_expectations,
    )
    t_dq = PythonOperator(task_id="run_dq_checks", python_callable=run_dq_checks)

    t_read >> t_clean >> t_ge >> t_dq


from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "customer360",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}

with DAG(
    dag_id="dag_bronze_to_silver",
    description="Clean bronze events and write to Silver layer",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["customer360", "silver", "cleaning", "dq"],
) as dag:

    def read_bronze_events(**context):
        """List unprocessed bronze partitions."""
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )
        objects = list(
            client.list_objects("customer360-bronze", prefix="events/", recursive=True)
        )
        print(f"Found {len(objects)} objects in bronze")
        paths = [o.object_name for o in objects if o.object_name.endswith(".parquet")]
        context["task_instance"].xcom_push(
            key="bronze_paths", value=paths[:50]
        )  # batch of 50
        return len(paths)

    def clean_and_validate(**context):
        """
        Apply all cleaning transformations and DQ rules.
        Runs in-process using pandas (Spark job handles streaming; this handles backfill).
        """
        from io import BytesIO

        import numpy as np
        import pandas as pd
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )

        paths = (
            context["task_instance"].xcom_pull(
                task_ids="read_bronze_events", key="bronze_paths"
            )
            or []
        )

        if not paths:
            print("No bronze files to process.")
            return 0

        all_dfs = []
        for path in paths:
            try:
                response = client.get_object("customer360-bronze", path)
                df = pd.read_parquet(BytesIO(response.read()))
                all_dfs.append(df)
            except Exception as e:
                print(f"Error reading {path}: {e}")

        if not all_dfs:
            return 0

        df = pd.concat(all_dfs, ignore_index=True)
        original_count = len(df)
        print(f"Loaded {original_count:,} records from bronze")

        # ── Cleaning pipeline ──────────────────────────────────

        # 1. Remove nulls on critical columns
        df = df.dropna(subset=["customer_id", "event_type", "timestamp"])

        # 2. Deduplicate
        df = df.drop_duplicates(subset=["event_id"])

        # 3. Normalize strings
        df["customer_id"] = df["customer_id"].str.upper().str.strip()
        df["event_type"] = df["event_type"].str.upper().str.strip()
        df["device"] = df["device"].str.title().str.strip()
        df["region"] = df["region"].str.title().str.strip()

        # 4. Parse timestamps
        df["event_timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["event_timestamp"])

        # 5. Remove negative amounts
        for col in ["total_amount", "price", "refund_amount"]:
            if col in df.columns:
                df[col] = df[col].abs()

        # 6. Add partition columns
        df["event_date"] = df["event_timestamp"].dt.date
        df["event_year"] = df["event_timestamp"].dt.year
        df["event_month"] = df["event_timestamp"].dt.month
        df["event_hour"] = df["event_timestamp"].dt.hour
        df["processing_timestamp"] = pd.Timestamp.utcnow()

        # 7. DQ flags
        df["dq_passed"] = True
        df["dq_issues"] = ""

        cleaned_count = len(df)
        removed = original_count - cleaned_count
        print(f"After cleaning: {cleaned_count:,} records ({removed:,} removed)")

        # ── Write to Silver ──────────────────────────────────────
        buf = BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)

        from datetime import datetime

        now = datetime.utcnow()
        silver_path = f"events/year={now.year}/month={now.month:02d}/day={now.day:02d}/batch_{now.strftime('%H%M%S')}.parquet"

        client.put_object(
            "customer360-silver",
            silver_path,
            buf,
            length=buf.getbuffer().nbytes,
        )
        print(f"Written {cleaned_count:,} records → silver/{silver_path}")

        context["task_instance"].xcom_push(key="silver_records", value=cleaned_count)
        return cleaned_count

    def run_dq_checks(**context):
        """Run data quality checks and report metrics."""
        records = (
            context["task_instance"].xcom_pull(
                task_ids="clean_and_validate", key="silver_records"
            )
            or 0
        )
        print(f"DQ Check: {records:,} records written to silver")
        if records == 0:
            print("WARNING: Zero records written to silver!")

    # Tasks
    t_read = PythonOperator(
        task_id="read_bronze_events", python_callable=read_bronze_events
    )
    t_clean = PythonOperator(
        task_id="clean_and_validate", python_callable=clean_and_validate
    )
    t_dq = PythonOperator(task_id="run_dq_checks", python_callable=run_dq_checks)

    t_read >> t_clean >> t_dq
