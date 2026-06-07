# ruff: noqa: E402
"""
Customer360 — Churn Prediction EDA & Model Analysis
=====================================================
Exploratory analysis of customer features and churn model performance.

This notebook covers:
1. Feature distribution analysis (RFM + behavioural signals)
2. Correlation heatmap of features vs churn label
3. Customer segment profiling
4. Model performance curves (ROC, Precision-Recall)
5. SHAP feature importance plots
6. Churn risk segment distribution

Usage:
    jupyter notebook ml/notebooks/01_churn_eda.ipynb
    OR: jupyter lab ml/notebooks/01_churn_eda.ipynb
"""

# ── 1. Imports ────────────────────────────────────────────────────────────────
import warnings

warnings.filterwarnings("ignore")

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns  # type: ignore
import shap
import sqlalchemy
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split

# Set plotting style
sns.set_theme(style="darkgrid", palette="husl")
plt.rcParams.update({
    "figure.figsize": (14, 6),
    "axes.titlesize": 14,
    "axes.labelsize": 12,
    "font.family": "DejaVu Sans",
})

print("✓ Libraries loaded")

# ── 2. Load Feature Data ──────────────────────────────────────────────────────
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql+psycopg2://customer360:customer360secret@localhost:5432/customer360_warehouse"
)
engine = sqlalchemy.create_engine(POSTGRES_DSN)

with engine.connect() as conn:
    df = pd.read_sql("""
        SELECT
            customer_id,
            MAX(recency_days)           AS recency_days,
            MAX(frequency)              AS frequency,
            MAX(monetary)               AS monetary,
            MAX(avg_purchase_value)     AS avg_purchase_value,
            MAX(max_purchase_value)     AS max_purchase_value,
            MAX(min_purchase_value)     AS min_purchase_value,
            MAX(cart_abandonment_rate)  AS cart_abandonment_rate,
            MAX(product_view_count)     AS product_view_count,
            MAX(search_count)           AS search_count,
            MAX(cart_add_count)         AS cart_add_count,
            MAX(purchase_count)         AS purchase_count,
            MAX(login_count)            AS login_count,
            MAX(monthly_orders)         AS monthly_orders,
            MAX(days_since_last_login)  AS days_since_last_login,
            MAX(avg_session_duration)   AS avg_session_duration
        FROM feature_store
        WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM feature_store)
        GROUP BY customer_id
    """, conn)  # type: ignore

engine.dispose()
df = df.fillna(0)
print(f"✓ Loaded {len(df):,} customer records  |  {df.shape[1]} features")
print(df.describe().round(2))

# ── 3. Label: is_churned ─────────────────────────────────────────────────────
CHURN_THRESHOLD = 90
df["is_churned"] = (df["recency_days"] > CHURN_THRESHOLD).astype(int)
churn_rate = df["is_churned"].mean()
print(f"\nChurn rate ({CHURN_THRESHOLD}d threshold): {churn_rate:.1%}")

# ── 4. Feature Distributions ─────────────────────────────────────────────────
FEATURES = [
    "recency_days", "frequency", "monetary", "avg_purchase_value",
    "cart_abandonment_rate", "product_view_count", "login_count",
    "monthly_orders", "days_since_last_login",
]

fig, axes = plt.subplots(3, 3, figsize=(18, 14))
fig.suptitle("Customer Feature Distributions (Churn vs. Retained)", fontsize=16, y=1.01)

for ax, feat in zip(axes.flatten(), FEATURES, strict=False):
    for label, color, name in [(0, "#10b981", "Retained"), (1, "#ef4444", "Churned")]:
        subset = df[df["is_churned"] == label][feat].clip(upper=df[feat].quantile(0.99))
        ax.hist(subset, bins=40, alpha=0.6, color=color, label=name, density=True)
    ax.set_title(feat.replace("_", " ").title())
    ax.set_xlabel("Value")
    ax.set_ylabel("Density")
    ax.legend(fontsize=9)

plt.tight_layout()
plt.savefig("feature_distributions.png", dpi=150, bbox_inches="tight")
plt.show()
print("✓ Feature distribution plot saved")

# ── 5. Correlation Heatmap ────────────────────────────────────────────────────
plt.figure(figsize=(14, 10))
corr = df[FEATURES + ["is_churned"]].corr()
mask = np.triu(np.ones_like(corr, dtype=bool))
sns.heatmap(
    corr, mask=mask, annot=True, fmt=".2f",  # type: ignore
    cmap="RdYlGn", center=0, square=True,
    linewidths=0.5, cbar_kws={"shrink": 0.8}
)
plt.title("Feature Correlation Matrix (incl. Churn Label)", fontsize=14)
plt.tight_layout()
plt.savefig("correlation_heatmap.png", dpi=150, bbox_inches="tight")
plt.show()
print("✓ Correlation heatmap saved")

# ── 6. RFM Segment Profiles ───────────────────────────────────────────────────
# Quick segment by recency + frequency + monetary quintiles
df["r_score"] = pd.qcut(df["recency_days"].rank(ascending=False), 5, labels=[5, 4, 3, 2, 1]).astype(int)
df["f_score"] = pd.qcut(df["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]).astype(int)
df["m_score"] = pd.qcut(df["monetary"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5]).astype(int)
df["rfm_score"] = df["r_score"] + df["f_score"] + df["m_score"]

rfm_bins = [0, 6, 9, 12, 15]
rfm_labels = ["Bronze", "Silver", "Gold", "Platinum"]
df["rfm_segment"] = pd.cut(df["rfm_score"], bins=rfm_bins, labels=rfm_labels)

seg_summary = df.groupby("rfm_segment", observed=True).agg(
    count=("customer_id", "count"),
    churn_rate=("is_churned", "mean"),
    avg_spend=("monetary", "mean"),
    avg_recency=("recency_days", "mean"),
).round(2)

print("\nRFM Segment Summary:")
print(seg_summary.to_string())

fig, axes = plt.subplots(1, 2, figsize=(16, 6))
seg_summary["count"].plot(kind="bar", ax=axes[0], color=["#cd7f32", "#c0c0c0", "#ffd700", "#e5e4e2"])
axes[0].set_title("Customer Count by RFM Segment")
axes[0].set_ylabel("# Customers")
axes[0].tick_params(axis="x", rotation=0)

seg_summary["churn_rate"].plot(kind="bar", ax=axes[1], color=["#10b981", "#3b82f6", "#f59e0b", "#ef4444"])
axes[1].set_title("Churn Rate by RFM Segment")
axes[1].set_ylabel("Churn Rate")
axes[1].tick_params(axis="x", rotation=0)

plt.tight_layout()
plt.savefig("rfm_segments.png", dpi=150, bbox_inches="tight")
plt.show()
print("✓ RFM segment plot saved")

# ── 7. Model Performance (if model exists) ───────────────────────────────────
MODEL_PATH = os.path.join(os.path.dirname(__file__), "../models/churn_model_latest.joblib")

if os.path.exists(MODEL_PATH):
    model = joblib.load(MODEL_PATH)
    X = df[FEATURES]
    y = df["is_churned"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    y_prob = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)

    auc    = roc_auc_score(y_test, y_prob)
    ap     = average_precision_score(y_test, y_prob)
    print(f"\n✓ Model loaded  |  AUC-ROC: {auc:.4f}  |  Avg Precision: {ap:.4f}")
    print(classification_report(y_test, y_pred, target_names=["Retained", "Churned"]))

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))

    # ROC Curve
    fpr, tpr, _ = roc_curve(y_test, y_prob)
    axes[0].plot(fpr, tpr, lw=2, color="#3b82f6", label=f"AUC = {auc:.3f}")
    axes[0].plot([0, 1], [0, 1], "--", color="gray")
    axes[0].set(title="ROC Curve", xlabel="False Positive Rate", ylabel="True Positive Rate")
    axes[0].legend(loc="lower right")

    # Precision-Recall Curve
    prec, rec, _ = precision_recall_curve(y_test, y_prob)
    axes[1].plot(rec, prec, lw=2, color="#10b981", label=f"AP = {ap:.3f}")
    axes[1].axhline(churn_rate, ls="--", color="gray", label=f"Baseline ({churn_rate:.2%})")
    axes[1].set(title="Precision-Recall Curve", xlabel="Recall", ylabel="Precision")
    axes[1].legend()

    plt.tight_layout()
    plt.savefig("model_performance.png", dpi=150, bbox_inches="tight")
    plt.show()

    # SHAP feature importance
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X_test[:500])
    plt.figure(figsize=(12, 7))
    shap.summary_plot(shap_values, X_test[:500], plot_type="bar", show=False)
    plt.title("SHAP Feature Importance (XGBoost Churn Model)")
    plt.tight_layout()
    plt.savefig("shap_importance.png", dpi=150, bbox_inches="tight")
    plt.show()
    print("✓ Model performance + SHAP plots saved")
else:
    print(f"⚠️  Model not found at {MODEL_PATH}. Run: python ml/models/churn_predictor.py --train")

print("\n✅ EDA complete. Check generated PNG files for visuals.")
