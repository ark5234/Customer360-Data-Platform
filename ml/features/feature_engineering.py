"""
Customer360 Data Platform - Feature Engineering Pipeline
Generates ML-ready features from warehouse and stores in feature_store table
"""

import os
from datetime import datetime

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://customer360:customer360secret@localhost:5432/customer360_warehouse",
)


def get_engine():
    return create_engine(DB_URL)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "customer360_warehouse"),
        user=os.getenv("POSTGRES_USER", "customer360"),
        password=os.getenv("POSTGRES_PASSWORD", "customer360secret"),
    )


class RFMFeatureExtractor:
    def __init__(self, snapshot_date=None):
        self.snapshot_date = snapshot_date or datetime.utcnow()

    def extract(self, engine):
        print("Extracting RFM features...")
        query = """
            SELECT customer_id, COUNT(order_id) AS frequency, SUM(total_amount) AS monetary,
                   MAX(event_timestamp) AS last_purchase_date, MIN(event_timestamp) AS first_purchase_date,
                   AVG(total_amount) AS avg_purchase_value, MAX(total_amount) AS max_purchase_value,
                   MIN(total_amount) AS min_purchase_value
            FROM fact_orders WHERE total_amount > 0 GROUP BY customer_id
        """
        df = pd.read_sql(query, engine)
        df["recency_days"] = (
            (self.snapshot_date - pd.to_datetime(df["last_purchase_date"]))
            .dt.days.fillna(9999)
            .astype(int)
        )
        df["customer_lifespan_days"] = (
            (
                pd.to_datetime(df["last_purchase_date"])
                - pd.to_datetime(df["first_purchase_date"])
            )
            .dt.days.fillna(0)
            .astype(int)
        )
        df["monthly_orders"] = np.where(
            df["customer_lifespan_days"] > 0,
            df["frequency"] / (df["customer_lifespan_days"] / 30),
            df["frequency"],
        ).round(4)
        print(f"✓ RFM features: {len(df):,} customers")
        return df


class BehavioralFeatureExtractor:
    def extract(self, engine):
        print("Extracting behavioral features...")
        query = """
            SELECT customer_id,
                   COUNT(*) FILTER (WHERE event_type = 'PRODUCT_VIEW') AS product_view_count,
                   COUNT(*) FILTER (WHERE event_type = 'SEARCH') AS search_count,
                   COUNT(*) FILTER (WHERE event_type = 'ADD_TO_CART') AS cart_add_count,
                   COUNT(*) FILTER (WHERE event_type = 'PURCHASE') AS purchase_count,
                   COUNT(*) FILTER (WHERE event_type = 'LOGIN') AS login_count
            FROM raw_events GROUP BY customer_id
        """
        df = pd.read_sql(query, engine)
        df["cart_abandonment_rate"] = (
            (
                (df["cart_add_count"] - df["purchase_count"])
                / df["cart_add_count"].replace(0, np.nan)
            )
            .fillna(0)
            .clip(0, 1)
            .round(4)
        )
        print(f"✓ Behavioral features: {len(df):,} customers")
        return df


class FeaturePipeline:
    def __init__(self, snapshot_date=None):
        self.snapshot_date = snapshot_date or datetime.utcnow()
        self.engine = get_engine()

    def run(self):
        print("=" * 60)
        print("  Customer360 — Feature Engineering Pipeline")
        print("=" * 60)
        rfm = RFMFeatureExtractor(self.snapshot_date).extract(self.engine)
        behavioral = BehavioralFeatureExtractor().extract(self.engine)
        features = rfm.merge(behavioral, on="customer_id", how="outer")
        numeric_cols = features.select_dtypes(include=[np.number]).columns
        features[numeric_cols] = features[numeric_cols].fillna(0)
        features["snapshot_date"] = self.snapshot_date.date()
        features["created_at"] = datetime.utcnow()
        print(f"\nTotal features: {len(features.columns)} columns")
        print(f"Total customers: {len(features):,}")
        self._write_to_store(features)
        return features

    def _write_to_store(self, df):
        conn = get_conn()
        cursor = conn.cursor()
        written = 0
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO feature_store (customer_id, snapshot_date, feature_set, recency_days, frequency, monetary,
                    avg_purchase_value, max_purchase_value, min_purchase_value, product_view_count, search_count,
                    cart_add_count, purchase_count, login_count, cart_abandonment_rate, monthly_orders, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (customer_id, snapshot_date, feature_set) DO UPDATE SET
                    recency_days = EXCLUDED.recency_days, frequency = EXCLUDED.frequency, monetary = EXCLUDED.monetary, updated_at = NOW()
            """,
                (
                    row["customer_id"],
                    row["snapshot_date"],
                    "combined_v2",
                    int(row.get("recency_days", 9999)),
                    int(row.get("frequency", 0)),
                    float(row.get("monetary", 0)),
                    float(row.get("avg_purchase_value", 0)),
                    float(row.get("max_purchase_value", 0)),
                    float(row.get("min_purchase_value", 0)),
                    int(row.get("product_view_count", 0)),
                    int(row.get("search_count", 0)),
                    int(row.get("cart_add_count", 0)),
                    int(row.get("purchase_count", 0)),
                    int(row.get("login_count", 0)),
                    float(row.get("cart_abandonment_rate", 0)),
                    float(row.get("monthly_orders", 0)),
                ),
            )
            written += 1
            if written % 10000 == 0:
                conn.commit()
                print(f"  Written {written:,} features...")
        conn.commit()
        cursor.close()
        conn.close()
        print(f"\n✓ Feature store updated: {written:,} customer records")


if __name__ == "__main__":
    pipeline = FeaturePipeline()
    features = pipeline.run()
    print("\n" + str(features.describe()))
