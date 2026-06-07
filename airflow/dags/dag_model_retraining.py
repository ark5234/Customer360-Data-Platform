"""
DAG 6: Model Retraining
Runs weekly on Sunday at 2 AM.
Retrains the churn prediction model using latest feature store data.
"""

from datetime import timedelta

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from airflow import DAG

default_args = {
    "owner": "customer360",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=4),
}

with DAG(
    dag_id="dag_model_retraining",
    description="Retrain churn prediction model on fresh feature store data",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 2 * * 0",  # Weekly, Sunday 2AM
    catchup=False,
    tags=["customer360", "ml", "training", "churn"],
) as dag:

    def fetch_training_data(**context):
        """Load feature store data for training."""
        import pandas as pd
        import psycopg2

        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="customer360_warehouse",
            user="customer360",
            password="customer360secret",
        )

        # Pull combined features
        df = pd.read_sql(
            """
            SELECT
                customer_id,
                MAX(recency_days) as recency_days,
                MAX(frequency) as frequency,
                MAX(monetary) as monetary,
                MAX(avg_purchase_value) as avg_purchase_value,
                MAX(cart_abandonment_rate) as cart_abandonment_rate,
                MAX(product_view_count) as product_view_count,
                MAX(search_count) as search_count
            FROM feature_store
            GROUP BY customer_id
        """,
            conn,
        )

        conn.close()

        if df.empty:
            print("No feature store data available for training.")
            return 0

        # Define churn label: recency > 90 days = churned
        df["is_churned"] = (df["recency_days"] > 90).astype(int)
        df.to_csv("/tmp/training_data.csv", index=False)

        print(
            f"Training data: {len(df):,} customers, churn rate: {df['is_churned'].mean():.1%}"
        )
        context["task_instance"].xcom_push(key="training_size", value=len(df))
        return len(df)

    def train_churn_model(**context):
        """Train XGBoost churn prediction model."""
        import json
        import os
        from datetime import datetime

        import joblib
        import pandas as pd
        import xgboost as xgb
        from sklearn.metrics import classification_report, roc_auc_score
        from sklearn.model_selection import train_test_split

        training_size = (
            context["task_instance"].xcom_pull(
                task_ids="fetch_training_data", key="training_size"
            )
            or 0
        )

        if training_size < 100:
            print("Insufficient training data. Skipping model training.")
            return

        df = pd.read_csv("/tmp/training_data.csv")
        df = df.fillna(0)

        features = [
            "recency_days",
            "frequency",
            "monetary",
            "avg_purchase_value",
            "cart_abandonment_rate",
            "product_view_count",
            "search_count",
        ]
        target = "is_churned"

        available_features = [f for f in features if f in df.columns]
        X = df[available_features]
        y = df[target]

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        model = xgb.XGBClassifier(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            use_label_encoder=False,
            eval_metric="logloss",
            random_state=42,
        )

        model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_pred_proba)
        report = classification_report(y_test, model.predict(X_test))

        print(f"Model AUC-ROC: {auc:.4f}")
        print(report)

        # Save model
        model_path = "/opt/airflow/models/churn_model_latest.joblib"
        os.makedirs("/opt/airflow/models", exist_ok=True)
        joblib.dump(model, model_path)

        # Save metrics
        metrics = {
            "auc_roc": auc,
            "training_size": training_size,
            "features": available_features,
            "trained_at": datetime.utcnow().isoformat(),
        }
        with open("/opt/airflow/models/churn_model_metrics.json", "w") as f:
            json.dump(metrics, f, indent=2)

        print(f"Model saved to {model_path}")
        context["task_instance"].xcom_push(key="model_auc", value=auc)

    def write_predictions(**context):
        """Score all customers and write churn probabilities to warehouse."""
        import os

        import joblib
        import pandas as pd
        import psycopg2

        model_path = "/opt/airflow/models/churn_model_latest.joblib"
        if not os.path.exists(model_path):
            print("No trained model found. Run training first.")
            return

        model = joblib.load(model_path)

        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            dbname="customer360_warehouse",
            user="customer360",
            password="customer360secret",
        )

        df = pd.read_sql(
            """
            SELECT customer_id,
                MAX(recency_days) as recency_days,
                MAX(frequency) as frequency,
                MAX(monetary) as monetary,
                MAX(avg_purchase_value) as avg_purchase_value,
                MAX(cart_abandonment_rate) as cart_abandonment_rate,
                MAX(product_view_count) as product_view_count,
                MAX(search_count) as search_count
            FROM feature_store
            GROUP BY customer_id
        """,
            conn,
        )

        if df.empty:
            conn.close()
            return

        features = [
            "recency_days",
            "frequency",
            "monetary",
            "avg_purchase_value",
            "cart_abandonment_rate",
            "product_view_count",
            "search_count",
        ]
        available = [f for f in features if f in df.columns]
        X = df[available].fillna(0)

        df["churn_probability"] = model.predict_proba(X)[:, 1]
        df["churn_segment"] = pd.cut(
            df["churn_probability"],
            bins=[0, 0.3, 0.6, 1.0],
            labels=["low_risk", "medium_risk", "high_risk"],
        )

        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO customer_churn_scores (customer_id, churn_probability, churn_segment, scored_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (customer_id) DO UPDATE SET
                    churn_probability = EXCLUDED.churn_probability,
                    churn_segment = EXCLUDED.churn_segment,
                    scored_at = NOW()
            """,
                (
                    row["customer_id"],
                    float(row["churn_probability"]),
                    str(row["churn_segment"]),
                ),
            )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Churn scores written for {len(df):,} customers")
        high_risk = (df["churn_segment"] == "high_risk").sum()
        print(f"High-risk customers: {high_risk:,} ({high_risk / len(df):.1%})")

    t_data = PythonOperator(
        task_id="fetch_training_data", python_callable=fetch_training_data
    )
    t_train = PythonOperator(
        task_id="train_churn_model", python_callable=train_churn_model
    )
    t_predict = PythonOperator(
        task_id="write_predictions", python_callable=write_predictions
    )

    t_data >> t_train >> t_predict
