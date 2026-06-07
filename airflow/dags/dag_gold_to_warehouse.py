"""
DAG 4: Gold → PostgreSQL Warehouse
Runs every 2 hours.
Loads gold-layer aggregates into the PostgreSQL star schema.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "customer360",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="dag_gold_to_warehouse",
    description="Load Gold layer data into PostgreSQL Warehouse",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 */2 * * *",  # every 2 hours
    catchup=False,
    max_active_runs=1,
    tags=["customer360", "warehouse", "postgres", "load"],
) as dag:

    def load_dim_customer(**context):
        """Upsert customer dimension from customer_360 gold view."""
        from io import BytesIO

        import pandas as pd
        import psycopg2
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="customer360_warehouse",
            user="customer360",
            password="customer360secret",
        )
        cursor = conn.cursor()

        objects = list(
            client.list_objects(
                "customer360-gold", prefix="customer_360/", recursive=True
            )
        )
        if not objects:
            print("No customer_360 gold data found.")
            conn.close()
            return

        resp = client.get_object("customer360-gold", objects[-1].object_name)
        df = pd.read_parquet(BytesIO(resp.read()))
        df = df.fillna({"total_spend": 0, "avg_spend": 0, "total_purchases": 0})

        upsert_sql = """
            INSERT INTO dim_customer (
                customer_id, region, preferred_device,
                total_events, total_purchases, total_spend,
                avg_spend, days_active, first_seen_date, last_seen_date,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (customer_id) DO UPDATE SET
                total_events = EXCLUDED.total_events,
                total_purchases = EXCLUDED.total_purchases,
                total_spend = EXCLUDED.total_spend,
                avg_spend = EXCLUDED.avg_spend,
                days_active = EXCLUDED.days_active,
                last_seen_date = EXCLUDED.last_seen_date,
                updated_at = NOW()
        """

        rows = 0
        for _, row in df.iterrows():
            cursor.execute(
                upsert_sql,
                (
                    row["customer_id"],
                    row.get("region", "Unknown"),
                    row.get("preferred_device", "Unknown"),
                    int(row.get("total_events", 0)),
                    int(row.get("total_purchases", 0)),
                    float(row.get("total_spend", 0)),
                    float(row.get("avg_spend", 0)),
                    int(row.get("days_active", 0)),
                    row.get("first_event_date"),
                    row.get("last_event_date"),
                ),
            )
            rows += 1

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Upserted {rows:,} customers into dim_customer")

    def load_fact_orders(**context):
        """Load purchase events into fact_orders."""
        from io import BytesIO

        import pandas as pd
        import psycopg2
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="customer360_warehouse",
            user="customer360",
            password="customer360secret",
        )
        cursor = conn.cursor()

        objects = list(
            client.list_objects("customer360-silver", prefix="events/", recursive=True)
        )
        dfs = []
        for obj in objects[:10]:
            try:
                resp = client.get_object("customer360-silver", obj.object_name)
                df = pd.read_parquet(BytesIO(resp.read()))
                dfs.append(df[df["event_type"] == "PURCHASE"])
            except Exception:
                pass

        if not dfs:
            print("No purchase events to load.")
            conn.close()
            return

        purchases = pd.concat(dfs, ignore_index=True)
        purchases = purchases.dropna(subset=["order_id", "customer_id"])

        insert_sql = """
            INSERT INTO fact_orders (
                order_id, customer_id, event_timestamp,
                total_amount, discount_amount, tax_amount,
                payment_method, items_count, region, device,
                event_date, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (order_id) DO NOTHING
        """

        rows = 0
        for _, row in purchases.iterrows():
            try:
                cursor.execute(
                    insert_sql,
                    (
                        row.get("order_id"),
                        row.get("customer_id"),
                        row.get("event_timestamp"),
                        float(row.get("total_amount", 0) or 0),
                        float(row.get("discount_amount", 0) or 0),
                        float(row.get("tax_amount", 0) or 0),
                        row.get("payment_method", "unknown"),
                        int(row.get("items_count", 1) or 1),
                        row.get("region", "Unknown"),
                        row.get("device", "Unknown"),
                        row.get("event_date"),
                    ),
                )
                rows += 1
            except Exception as e:
                print(f"Skip row: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Loaded {rows:,} orders into fact_orders")

    def load_revenue_metrics(**context):
        """Load revenue metrics into the warehouse."""
        from datetime import datetime
        from io import BytesIO

        import pandas as pd
        import psycopg2
        from minio import Minio

        client = Minio(
            "minio:9000",
            access_key="customer360",
            secret_key="customer360secret",
            secure=False,
        )
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="customer360_warehouse",
            user="customer360",
            password="customer360secret",
        )
        cursor = conn.cursor()

        objects = list(
            client.list_objects(
                "customer360-gold", prefix="revenue_by_region/", recursive=True
            )
        )
        if not objects:
            conn.close()
            return

        resp = client.get_object("customer360-gold", objects[-1].object_name)
        df = pd.read_parquet(BytesIO(resp.read()))

        insert_sql = """
            INSERT INTO revenue_metrics (region, year, month, total_revenue, order_count, avg_order_value, unique_customers, loaded_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (region, year, month) DO UPDATE SET
                total_revenue = EXCLUDED.total_revenue,
                order_count = EXCLUDED.order_count,
                avg_order_value = EXCLUDED.avg_order_value,
                unique_customers = EXCLUDED.unique_customers,
                loaded_at = NOW()
        """

        for _, row in df.iterrows():
            cursor.execute(
                insert_sql,
                (
                    row.get("region", "Unknown"),
                    int(row.get("event_year", datetime.utcnow().year)),
                    int(row.get("event_month", datetime.utcnow().month)),
                    float(row.get("total_revenue", 0)),
                    int(row.get("order_count", 0)),
                    float(row.get("avg_order_value", 0)),
                    int(row.get("unique_customers", 0)),
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Revenue metrics loaded: {len(df)} region-month rows")

    def publish_lineage(**context):
        """
        Publish pipeline lineage to DataHub after warehouse load completes.
        Non-critical: failures are logged but do not block the pipeline.
        DataHub must be running (docker compose --profile lineage up -d).
        """
        import os
        import sys
        sys.path.insert(0, "/opt/airflow")

        try:
            from lineage.publish_lineage import publish_stage_lineage
            datahub_url = os.getenv("DATAHUB_GMS_URL", "http://datahub-gms:8085")
            ok = publish_stage_lineage("gold_to_warehouse", gms_url=datahub_url)
            if ok:
                print("✓ DataHub lineage published for gold_to_warehouse")
            else:
                print("⚠ DataHub lineage skipped (DataHub not running — start with --profile lineage)")
        except Exception as e:
            print(f"⚠ Lineage publication skipped: {e}")

    t_dim_customer = PythonOperator(
        task_id="load_dim_customer", python_callable=load_dim_customer
    )
    t_fact_orders = PythonOperator(
        task_id="load_fact_orders", python_callable=load_fact_orders
    )
    t_revenue = PythonOperator(
        task_id="load_revenue_metrics", python_callable=load_revenue_metrics
    )
    t_lineage = PythonOperator(
        task_id="publish_datahub_lineage",
        python_callable=publish_lineage,
    )

    t_dim_customer >> t_fact_orders >> t_revenue >> t_lineage

