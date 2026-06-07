"""
DAG 3: Silver → Gold
Runs hourly.
Aggregates cleaned data into business-ready gold layer datasets.
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
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="dag_silver_to_gold",
    description="Aggregate silver data into Gold layer business datasets",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["customer360", "gold", "aggregation"],
) as dag:

    def build_revenue_metrics(**context):
        """Compute revenue aggregations by region, category, device."""
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

        # Read all silver purchase events
        objects = list(
            client.list_objects("customer360-silver", prefix="events/", recursive=True)
        )
        dfs = []
        for obj in objects[:20]:  # Process in manageable chunks
            try:
                resp = client.get_object("customer360-silver", obj.object_name)
                df = pd.read_parquet(BytesIO(resp.read()))
                dfs.append(df)
            except Exception as e:
                print(f"Skip {obj.object_name}: {e}")

        if not dfs:
            print("No silver data to aggregate.")
            return

        df = pd.concat(dfs, ignore_index=True)
        purchases = df[df["event_type"] == "PURCHASE"].copy()

        if purchases.empty:
            print("No purchase events found.")
            return

        # Revenue by region
        revenue_by_region = (
            purchases.groupby(["region", "event_year", "event_month"])
            .agg(
                total_revenue=("total_amount", "sum"),
                order_count=("event_id", "count"),
                avg_order_value=("total_amount", "mean"),
                unique_customers=("customer_id", "nunique"),
            )
            .reset_index()
        )

        # Revenue by category
        revenue_by_category = (
            purchases.groupby(["category", "event_year", "event_month"])
            .agg(
                total_revenue=("total_amount", "sum"),
                order_count=("event_id", "count"),
            )
            .reset_index()
        )

        # Write to gold
        now = datetime.utcnow()
        for name, gold_df in [
            ("revenue_by_region", revenue_by_region),
            ("revenue_by_category", revenue_by_category),
        ]:
            buf = BytesIO()
            gold_df.to_parquet(buf, index=False)
            buf.seek(0)
            path = f"{name}/year={now.year}/month={now.month:02d}/day={now.day:02d}/{now.hour:02d}00.parquet"
            client.put_object(
                "customer360-gold", path, buf, length=buf.getbuffer().nbytes
            )
            print(f"Written {len(gold_df)} rows → gold/{path}")

    def build_customer_360(**context):
        """Build customer 360 view aggregating all event types."""
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

        objects = list(
            client.list_objects("customer360-silver", prefix="events/", recursive=True)
        )
        dfs = []
        for obj in objects[:30]:
            try:
                resp = client.get_object("customer360-silver", obj.object_name)
                df = pd.read_parquet(BytesIO(resp.read()))
                dfs.append(df)
            except Exception:
                pass

        if not dfs:
            return

        df = pd.concat(dfs, ignore_index=True)

        customer_360 = (
            df.groupby("customer_id")
            .agg(
                total_events=("event_id", "count"),
                total_purchases=(
                    "event_id",
                    lambda x: (df.loc[x.index, "event_type"] == "PURCHASE").sum(),
                ),
                total_spend=("total_amount", "sum"),
                avg_spend=("total_amount", "mean"),
                last_event_date=("event_timestamp", "max"),
                first_event_date=("event_timestamp", "min"),
                preferred_device=(
                    "device",
                    lambda x: x.mode()[0] if not x.empty else None,
                ),
                region=("region", "first"),
            )
            .reset_index()
        )

        customer_360["days_active"] = (
            pd.to_datetime(customer_360["last_event_date"])
            - pd.to_datetime(customer_360["first_event_date"])
        ).dt.days

        now = datetime.utcnow()
        buf = BytesIO()
        customer_360.to_parquet(buf, index=False)
        buf.seek(0)
        path = f"customer_360/year={now.year}/month={now.month:02d}/day={now.day:02d}/customer_360.parquet"
        client.put_object("customer360-gold", path, buf, length=buf.getbuffer().nbytes)
        print(
            f"Customer 360 view: {len(customer_360):,} customers written to gold/{path}"
        )

    def build_product_performance(**context):
        """Build product performance metrics."""
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
            return

        df = pd.concat(dfs, ignore_index=True)
        product_df = df[df["product_id"].notna()].copy()

        if product_df.empty:
            return

        views = (
            product_df[product_df["event_type"] == "PRODUCT_VIEW"]
            .groupby("product_id")
            .agg(
                view_count=("event_id", "count"),
                avg_price=("price", "mean"),
            )
            .reset_index()
        )

        carts = (
            product_df[product_df["event_type"] == "ADD_TO_CART"]
            .groupby("product_id")
            .agg(
                cart_count=("event_id", "count"),
            )
            .reset_index()
        )

        purchases = (
            product_df[product_df["event_type"] == "PURCHASE"]
            .groupby("product_id")
            .agg(
                purchase_count=("event_id", "count"),
                total_revenue=("total_amount", "sum"),
            )
            .reset_index()
        )

        product_perf = (
            views.merge(carts, on="product_id", how="left")
            .merge(purchases, on="product_id", how="left")
            .fillna(0)
        )

        product_perf["cart_rate"] = product_perf["cart_count"] / product_perf[
            "view_count"
        ].replace(0, 1)
        product_perf["conversion_rate"] = product_perf["purchase_count"] / product_perf[
            "view_count"
        ].replace(0, 1)

        now = datetime.utcnow()
        buf = BytesIO()
        product_perf.to_parquet(buf, index=False)
        buf.seek(0)
        path = f"product_performance/year={now.year}/month={now.month:02d}/day={now.day:02d}/product_perf.parquet"
        client.put_object("customer360-gold", path, buf, length=buf.getbuffer().nbytes)
        print(f"Product performance: {len(product_perf):,} products → gold/{path}")

    t_revenue = PythonOperator(
        task_id="build_revenue_metrics", python_callable=build_revenue_metrics
    )
    t_customer = PythonOperator(
        task_id="build_customer_360", python_callable=build_customer_360
    )
    t_product = PythonOperator(
        task_id="build_product_performance", python_callable=build_product_performance
    )
