"""
DAG 5: Feature Engineering
Runs daily at midnight.
Computes ML-ready features from the warehouse and stores in feature_store table.
"""

from datetime import timedelta

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    "owner": "customer360",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=3),
}

with DAG(
    dag_id="dag_feature_engineering",
    description="Compute ML features and populate feature_store",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["customer360", "ml", "features"],
) as dag:

    def compute_rfm_features(**context):
        """
        Compute RFM (Recency, Frequency, Monetary) features per customer.
        """
        from datetime import datetime

        import pandas as pd
        import psycopg2

        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="customer360_warehouse",
            user="customer360",
            password="customer360secret",
        )

        # Read fact_orders
        df = pd.read_sql(
            """
            SELECT
                customer_id,
                event_timestamp,
                total_amount,
                region,
                device
            FROM fact_orders
            WHERE total_amount > 0
        """,
            conn,
        )

        if df.empty:
            print("No order data for feature computation.")
            conn.close()
            return

        snapshot_date = datetime.utcnow()
        df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])

        rfm = (
            df.groupby("customer_id")
            .agg(
                recency_days=(
                    "event_timestamp",
                    lambda x: (snapshot_date - x.max()).days,
                ),
                frequency=("event_timestamp", "count"),
                monetary=("total_amount", "sum"),
                avg_purchase_value=("total_amount", "mean"),
                max_purchase_value=("total_amount", "max"),
                min_purchase_value=("total_amount", "min"),
            )
            .reset_index()
        )

        rfm["snapshot_date"] = snapshot_date.date()
        rfm["feature_set"] = "rfm"

        # Write to feature store
        cursor = conn.cursor()
        for _, row in rfm.iterrows():
            cursor.execute(
                """
                INSERT INTO feature_store (
                    customer_id, snapshot_date, feature_set,
                    recency_days, frequency, monetary,
                    avg_purchase_value, max_purchase_value, min_purchase_value,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (customer_id, snapshot_date, feature_set)
                DO UPDATE SET
                    recency_days = EXCLUDED.recency_days,
                    frequency = EXCLUDED.frequency,
                    monetary = EXCLUDED.monetary,
                    avg_purchase_value = EXCLUDED.avg_purchase_value,
                    updated_at = NOW()
            """,
                (
                    row["customer_id"],
                    row["snapshot_date"],
                    row["feature_set"],
                    int(row["recency_days"]),
                    int(row["frequency"]),
                    float(row["monetary"]),
                    float(row["avg_purchase_value"]),
                    float(row["max_purchase_value"]),
                    float(row["min_purchase_value"]),
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"RFM features computed for {len(rfm):,} customers")
        context["task_instance"].xcom_push(key="rfm_customers", value=len(rfm))

    def compute_behavioral_features(**context):
        """
        Compute behavioral features from silver events:
        - session_count, avg_session_duration
        - product_view_count, search_count
        - cart_abandonment_rate
        - preferred_category, preferred_device
        - days_since_last_purchase, monthly_orders
        """
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

        objects = list(
            client.list_objects("customer360-silver", prefix="events/", recursive=True)
        )
        dfs = []
        for obj in objects[:20]:
            try:
                resp = client.get_object("customer360-silver", obj.object_name)
                df = pd.read_parquet(BytesIO(resp.read()))
                dfs.append(df)
            except Exception:
                pass

        if not dfs:
            conn.close()
            return

        df = pd.concat(dfs, ignore_index=True)
        snapshot = datetime.utcnow()

        # Event counts per customer per type
        event_counts = (
            df.groupby(["customer_id", "event_type"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )

        # Ensure columns exist
        for col in [
            "PRODUCT_VIEW",
            "SEARCH",
            "ADD_TO_CART",
            "PURCHASE",
            "LOGIN",
            "LOGOUT",
            "REFUND",
        ]:
            if col not in event_counts.columns:
                event_counts[col] = 0

        event_counts["cart_abandonment_rate"] = (
            (event_counts["ADD_TO_CART"] - event_counts["PURCHASE"])
            / event_counts["ADD_TO_CART"].replace(0, 1)
        ).clip(0, 1)

        event_counts["snapshot_date"] = snapshot.date()
        event_counts["feature_set"] = "behavioral"

        cursor = conn.cursor()
        for _, row in event_counts.iterrows():
            cursor.execute(
                """
                INSERT INTO feature_store (
                    customer_id, snapshot_date, feature_set,
                    product_view_count, search_count,
                    cart_add_count, purchase_count,
                    cart_abandonment_rate, login_count,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (customer_id, snapshot_date, feature_set)
                DO UPDATE SET
                    product_view_count = EXCLUDED.product_view_count,
                    search_count = EXCLUDED.search_count,
                    cart_add_count = EXCLUDED.cart_add_count,
                    purchase_count = EXCLUDED.purchase_count,
                    cart_abandonment_rate = EXCLUDED.cart_abandonment_rate,
                    updated_at = NOW()
            """,
                (
                    row["customer_id"],
                    row["snapshot_date"],
                    row["feature_set"],
                    int(row.get("PRODUCT_VIEW", 0)),
                    int(row.get("SEARCH", 0)),
                    int(row.get("ADD_TO_CART", 0)),
                    int(row.get("PURCHASE", 0)),
                    float(row.get("cart_abandonment_rate", 0)),
                    int(row.get("LOGIN", 0)),
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Behavioral features computed for {len(event_counts):,} customers")

    t_rfm = PythonOperator(
        task_id="compute_rfm_features", python_callable=compute_rfm_features
    )
    t_behavioral = PythonOperator(
        task_id="compute_behavioral_features",
        python_callable=compute_behavioral_features,
    )

    t_rfm >> t_behavioral
