"""
Customer360 Data Platform - Churn Prediction Model (XGBoost)
Usage: python churn_predictor.py --train | --predict | --evaluate
"""

import json
import os
from datetime import datetime
from pathlib import Path

import click
import joblib
import pandas as pd
import psycopg2
import sqlalchemy
import xgboost as xgb
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score, train_test_split

MODEL_DIR = Path(__file__).parent
CHURN_THRESHOLD_DAYS = int(os.getenv("CHURN_THRESHOLD_DAYS", 90))
FEATURE_COLUMNS = [
    "recency_days",
    "frequency",
    "monetary",
    "avg_purchase_value",
    "max_purchase_value",
    "min_purchase_value",
    "cart_abandonment_rate",
    "product_view_count",
    "search_count",
    "cart_add_count",
    "purchase_count",
    "login_count",
    "monthly_orders",
    "days_since_last_login",
    "avg_session_duration",
]
TARGET_COLUMN = "is_churned"


def load_features():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    dbname = os.getenv("POSTGRES_DB", "customer360_warehouse")
    user = os.getenv("POSTGRES_USER", "customer360")
    password = os.getenv("POSTGRES_PASSWORD", "customer360secret")
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )
    df = pd.read_sql(
        """SELECT customer_id, MAX(recency_days) AS recency_days, MAX(frequency) AS frequency,
        MAX(monetary) AS monetary, MAX(avg_purchase_value) AS avg_purchase_value, MAX(max_purchase_value) AS max_purchase_value,
        MAX(min_purchase_value) AS min_purchase_value, MAX(cart_abandonment_rate) AS cart_abandonment_rate, 
        MAX(product_view_count) AS product_view_count, MAX(search_count) AS search_count, MAX(cart_add_count) AS cart_add_count, 
        MAX(purchase_count) AS purchase_count, MAX(login_count) AS login_count, MAX(monthly_orders) AS monthly_orders,
        MAX(days_since_last_login) AS days_since_last_login, MAX(avg_session_duration) AS avg_session_duration
        FROM feature_store
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM feature_store) GROUP BY customer_id""",
        engine,
    )
    engine.dispose()
    print(f"Loaded {len(df):,} customer feature records")
    return df


def prepare_training_data(df):
    df = df.fillna(0)
    df[TARGET_COLUMN] = (df["recency_days"] > CHURN_THRESHOLD_DAYS).astype(int)
    print(
        f"Churn rate: {df[TARGET_COLUMN].mean():.1%} | Threshold: {CHURN_THRESHOLD_DAYS} days"
    )
    available = [f for f in FEATURE_COLUMNS if f in df.columns]
    return df[available], df[TARGET_COLUMN]


def build_model():
    return xgb.XGBClassifier(
        n_estimators=300,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        scale_pos_weight=3,
        use_label_encoder=False,
        eval_metric="auc",
        random_state=42,
        n_jobs=-1,
    )


def train(df):
    X, y = prepare_training_data(df)
    print(f"Training set: {len(X):,} samples | {X.shape[1]} features")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    model = build_model()
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=50)
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_proba)
    ap = average_precision_score(y_test, y_proba)
    print(
        f"\n✓ AUC-ROC: {auc:.4f}\n✓ Avg Precision: {ap:.4f}\n"
        + classification_report(y_test, y_pred)
    )
    cv_scores = cross_val_score(
        build_model(),
        X,
        y,
        cv=StratifiedKFold(n_splits=5, shuffle=True, random_state=42),
        scoring="roc_auc",
        n_jobs=-1,
    )
    print(f"CV AUC: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")
    importance = pd.DataFrame(
        {"feature": X.columns, "importance": model.feature_importances_}
    ).sort_values("importance", ascending=False)
    print(f"\nTop 10 features:\n{importance.head(10).to_string(index=False)}")
    metrics = {
        "auc_roc": float(auc),
        "avg_precision": float(ap),
        "cv_auc_mean": float(cv_scores.mean()),
        "cv_auc_std": float(cv_scores.std()),
        "training_samples": len(X_train),
        "test_samples": len(X_test),
        "features": list(X.columns),
        "churn_threshold_days": CHURN_THRESHOLD_DAYS,
        "trained_at": datetime.utcnow().isoformat(),
    }
    return model, metrics


def save_model(model, metrics, version="latest"):
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODEL_DIR / f"churn_model_{version}.joblib"
    metrics_path = MODEL_DIR / f"churn_model_{version}_metrics.json"
    joblib.dump(model, model_path)
    with open(metrics_path, "w") as f:
        json.dump(metrics, f, indent=2)
    print(f"✓ Model saved: {model_path}\n✓ Metrics saved: {metrics_path}")
    return model_path


def load_model(version="latest"):
    model_path = MODEL_DIR / f"churn_model_{version}.joblib"
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    return joblib.load(model_path)


def predict_and_store(model, df):
    df = df.fillna(0)
    available = [f for f in FEATURE_COLUMNS if f in df.columns]
    X = df[available]
    df["churn_probability"] = model.predict_proba(X)[:, 1]
    df["churn_segment"] = pd.cut(
        df["churn_probability"],
        bins=[0, 0.3, 0.6, 1.0001],
        labels=["low_risk", "medium_risk", "high_risk"],
    ).astype(str)
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        dbname=os.getenv("POSTGRES_DB", "customer360_warehouse"),
        user=os.getenv("POSTGRES_USER", "customer360"),
        password=os.getenv("POSTGRES_PASSWORD", "customer360secret"),
    )
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute(
            """INSERT INTO customer_churn_scores (customer_id, churn_probability, churn_segment, scored_at)
            VALUES (%s, %s, %s, NOW()) ON CONFLICT (customer_id) DO UPDATE SET
            churn_probability = EXCLUDED.churn_probability, churn_segment = EXCLUDED.churn_segment, scored_at = NOW()""",
            (row["customer_id"], float(row["churn_probability"]), row["churn_segment"]),
        )
    conn.commit()
    cursor.close()
    conn.close()
    print(
        f"\n✓ Scored {len(df):,} customers\n{df['churn_segment'].value_counts().to_string()}"
    )
    return df


@click.command()
@click.option("--train", "mode", flag_value="train", help="Train a new model")
@click.option("--predict", "mode", flag_value="predict", help="Score all customers")
@click.option("--evaluate", "mode", flag_value="evaluate", help="Run model evaluation")
@click.option("--version", default="latest", help="Model version tag")
def main(mode, version):
    if mode == "train":
        df = load_features()
        model, metrics = train(df)
        save_model(model, metrics, version)
    elif mode == "predict":
        model = load_model(version)
        df = load_features()
        predict_and_store(model, df)
    elif mode == "evaluate":
        model = load_model(version)
        df = load_features()
        X, y = prepare_training_data(df)
        auc = roc_auc_score(y, model.predict_proba(X.fillna(0))[:, 1])
        print(f"Current AUC-ROC: {auc:.4f}")
    else:
        click.echo("Specify --train, --predict, or --evaluate")


if __name__ == "__main__":
    main()
