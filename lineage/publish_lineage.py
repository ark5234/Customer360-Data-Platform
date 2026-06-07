"""
Customer360 Data Platform
DataHub Data Lineage Publisher

Emits pipeline lineage to DataHub after each major pipeline stage.
Shows the full data journey:

  Synthetic Events (Kafka Producer)
       ↓
  Kafka Topics (6 topics)
       ↓
  MinIO Bronze  (raw Parquet)
       ↓
  MinIO Silver  (cleaned, validated, GE-checked)
       ↓
  MinIO Gold    (business aggregates)
       ↓
  PostgreSQL Warehouse (star schema)
       ↓
  dbt Marts (LTV, retention, revenue, product analytics)

Usage:
    # From Airflow DAG (after pipeline stage completes):
    from lineage.publish_lineage import publish_stage_lineage
    publish_stage_lineage("bronze_to_silver")

    # Standalone:
    python lineage/publish_lineage.py

DataHub endpoint: http://localhost:8080 (when running with --profile lineage)
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import TypedDict

import requests

# ── DataHub GMS endpoint ──────────────────────────────────────────────────────
DATAHUB_GMS_URL = os.getenv("DATAHUB_GMS_URL", "http://datahub-gms:8080")
PLATFORM = "customer360"
ENV = "PROD"


# ─────────────────────────────────────────────────────────────────────────────
# Dataset URN helpers
# ─────────────────────────────────────────────────────────────────────────────

def _urn(platform: str, name: str, env: str = ENV) -> str:
    """Build a DataHub dataset URN."""
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


# ── Dataset URNs for the Customer360 pipeline ────────────────────────────────
DATASETS = {
    # Source data
    "kafka_events":        _urn("kafka",    "customer360.events"),
    "event_generator":     _urn("custom",   "customer360.synthetic_events"),

    # Data Lake layers
    "bronze_events":       _urn("minio",    "customer360-bronze/events"),
    "silver_events":       _urn("minio",    "customer360-silver/events"),
    "gold_customer_360":   _urn("minio",    "customer360-gold/customer_360"),
    "gold_revenue":        _urn("minio",    "customer360-gold/revenue_by_region"),
    "gold_products":       _urn("minio",    "customer360-gold/product_performance"),

    # Warehouse tables
    "dim_customer":        _urn("postgres", "customer360_warehouse.public.dim_customer"),
    "dim_product":         _urn("postgres", "customer360_warehouse.public.dim_product"),
    "fact_orders":         _urn("postgres", "customer360_warehouse.public.fact_orders"),
    "fact_transactions":   _urn("postgres", "customer360_warehouse.public.fact_transactions"),
    "fact_sessions":       _urn("postgres", "customer360_warehouse.public.fact_sessions"),
    "revenue_metrics":     _urn("postgres", "customer360_warehouse.public.revenue_metrics"),
    "feature_store":       _urn("postgres", "customer360_warehouse.public.feature_store"),
    "churn_scores":        _urn("postgres", "customer360_warehouse.public.customer_churn_scores"),

    # dbt Marts
    "mart_ltv":            _urn("dbt",      "customer360.marts.customer_lifetime_value"),
    "mart_retention":      _urn("dbt",      "customer360.marts.customer_retention"),
    "mart_revenue":        _urn("dbt",      "customer360.marts.monthly_revenue"),
    "mart_products":       _urn("dbt",      "customer360.marts.product_analytics"),

    # Qdrant (RAG)
    "qdrant_customer360":  _urn("custom",   "qdrant.customer360"),
}

# ── Pipeline stage definitions ────────────────────────────────────────────────

class StageConfig(TypedDict):
    inputs: list[str]
    outputs: list[str]
    job_name: str
    description: str

PIPELINE_STAGES: dict[str, StageConfig] = {
    "event_generation": {
        "inputs":  [],
        "outputs": [DATASETS["event_generator"]],
        "job_name": "event_generator",
        "description": "Generates 10M+ synthetic customer events (Python + Faker)",
    },
    "kafka_ingest": {
        "inputs":  [DATASETS["event_generator"]],
        "outputs": [DATASETS["kafka_events"]],
        "job_name": "kafka_producer",
        "description": "Publishes events to 6 Kafka topics at 5K events/sec",
    },
    "spark_bronze": {
        "inputs":  [DATASETS["kafka_events"]],
        "outputs": [DATASETS["bronze_events"]],
        "job_name": "spark_streaming_processor",
        "description": "Spark Structured Streaming: Kafka → MinIO Bronze (30s micro-batches)",
    },
    "bronze_to_silver": {
        "inputs":  [DATASETS["bronze_events"]],
        "outputs": [DATASETS["silver_events"]],
        "job_name": "dag_bronze_to_silver",
        "description": (
            "Airflow DAG: Cleans & validates Bronze events → Silver. "
            "Runs Great Expectations suite (12 rules: null checks, schema validation, "
            "duplicate detection, domain/range checks). 30-min schedule."
        ),
    },
    "silver_to_gold": {
        "inputs":  [DATASETS["silver_events"]],
        "outputs": [
            DATASETS["gold_customer_360"],
            DATASETS["gold_revenue"],
            DATASETS["gold_products"],
        ],
        "job_name": "dag_silver_to_gold",
        "description": "Airflow DAG: Aggregates Silver events → Gold business datasets. Hourly.",
    },
    "gold_to_warehouse": {
        "inputs":  [
            DATASETS["gold_customer_360"],
            DATASETS["gold_revenue"],
            DATASETS["gold_products"],
            DATASETS["silver_events"],
        ],
        "outputs": [
            DATASETS["dim_customer"],
            DATASETS["fact_orders"],
            DATASETS["fact_transactions"],
            DATASETS["revenue_metrics"],
        ],
        "job_name": "dag_gold_to_warehouse",
        "description": "Airflow DAG: Loads Gold → PostgreSQL star schema via upserts. Every 2hrs.",
    },
    "dbt_transforms": {
        "inputs":  [
            DATASETS["dim_customer"],
            DATASETS["fact_orders"],
            DATASETS["fact_transactions"],
            DATASETS["fact_sessions"],
        ],
        "outputs": [
            DATASETS["mart_ltv"],
            DATASETS["mart_retention"],
            DATASETS["mart_revenue"],
            DATASETS["mart_products"],
        ],
        "job_name": "dbt_run",
        "description": "dbt transformations: 3 staging + 4 mart models (LTV, retention, revenue, products)",
    },
    "feature_engineering": {
        "inputs":  [DATASETS["dim_customer"], DATASETS["fact_orders"]],
        "outputs": [DATASETS["feature_store"]],
        "job_name": "dag_feature_engineering",
        "description": "Airflow DAG: Computes 13 RFM + behavioral features daily → feature_store",
    },
    "model_retraining": {
        "inputs":  [DATASETS["feature_store"]],
        "outputs": [DATASETS["churn_scores"]],
        "job_name": "dag_model_retraining",
        "description": "Airflow DAG: Weekly XGBoost churn model retraining → customer_churn_scores",
    },
    "llm_ingestion": {
        "inputs":  [DATASETS["dim_customer"], DATASETS["mart_ltv"], DATASETS["revenue_metrics"]],
        "outputs": [DATASETS["qdrant_customer360"]],
        "job_name": "dag_llm_ingestion",
        "description": "Airflow DAG: Embeds warehouse data into Qdrant VectorDB for RAG queries",
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# DataHub REST emitter
# ─────────────────────────────────────────────────────────────────────────────

def _emit_lineage_event(payload: dict, gms_url: str = DATAHUB_GMS_URL) -> bool:
    """
    Emit a lineage event to DataHub GMS via REST API.
    Returns True on success, False on failure (non-blocking).
    """
    endpoint = f"{gms_url}/aspects?action=ingestProposal"
    try:
        resp = requests.post(
            endpoint,
            json={"proposal": payload},
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        if resp.status_code == 200:
            return True
        else:
            print(f"DataHub emit failed ({resp.status_code}): {resp.text[:200]}")
            return False
    except requests.exceptions.ConnectionError:
        print(f"DataHub not reachable at {gms_url}. Lineage skipped (non-critical).")
        return False
    except Exception as e:
        print(f"DataHub emit error: {e}")
        return False


def _build_lineage_mce(
    job_name: str,
    inputs: list[str],
    outputs: list[str],
    description: str,
    run_id: str | None = None,
) -> dict:
    """Build a DataHub MetadataChangeEvent for data process lineage."""
    now = datetime.now()  # timezone aware/naive is fine since we just get timestamp
    now_ms = int(now.timestamp() * 1000)
    run_id = run_id or f"{job_name}_{now.strftime('%Y%m%d_%H%M%S')}"

    return {
        "entityType": "dataProcessInstance",
        "entityUrn": f"urn:li:dataProcessInstance:{run_id}",
        "changeType": "UPSERT",
        "aspectName": "dataProcessInstanceRunEvent",
        "aspect": {
            "com.linkedin.dataprocess.DataProcessInstanceRunEvent": {
                "timestampMillis": now_ms,
                "status": "COMPLETE",
                "result": {"type": "SUCCESS"},
            }
        },
    }


def _build_dataset_lineage_mce(
    output_urn: str,
    input_urns: list[str],
) -> dict:
    """Build a DataHub upstream lineage aspect for a dataset."""
    return {
        "entityType": "dataset",
        "entityUrn": output_urn,
        "changeType": "UPSERT",
        "aspectName": "upstreamLineage",
        "aspect": {
            "com.linkedin.dataset.UpstreamLineage": {
                "upstreams": [
                    {
                        "dataset": urn,
                        "type": "TRANSFORMED",
                        "auditStamp": {
                            "time": int(datetime.now().timestamp() * 1000),
                            "actor": "urn:li:corpuser:customer360-pipeline",
                        },
                    }
                    for urn in input_urns
                ]
            }
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def publish_stage_lineage(
    stage: str,
    gms_url: str = DATAHUB_GMS_URL,
    run_id: str | None = None,
) -> bool:
    """
    Publish lineage for a named pipeline stage to DataHub.

    Args:
        stage: One of the keys in PIPELINE_STAGES (e.g. 'bronze_to_silver')
        gms_url: DataHub GMS URL (default: http://datahub-gms:8080)
        run_id: Optional run identifier (defaults to stage + timestamp)

    Returns:
        True if lineage was successfully emitted, False otherwise.
    """
    if stage not in PIPELINE_STAGES:
        print(f"Unknown stage '{stage}'. Available: {list(PIPELINE_STAGES)}")
        return False

    config = PIPELINE_STAGES[stage]
    print(f"Publishing lineage for stage: {stage}")
    print(f"  → {len(config['inputs'])} inputs, {len(config['outputs'])} outputs")

    success = True

    # Emit upstream lineage for each output dataset
    for output_urn in config["outputs"]:
        if config["inputs"]:
            mce = _build_dataset_lineage_mce(output_urn, config["inputs"])
            ok = _emit_lineage_event(mce, gms_url)
            if not ok:
                success = False

    # Emit process run event
    mce = _build_lineage_mce(
        job_name=config["job_name"],
        inputs=config["inputs"],
        outputs=config["outputs"],
        description=config["description"],
        run_id=run_id,
    )
    ok = _emit_lineage_event(mce, gms_url)
    if not ok:
        success = False

    if success:
        print(f"✓ Lineage published for '{stage}'")
    return success


def publish_full_pipeline_lineage(gms_url: str = DATAHUB_GMS_URL) -> dict[str, bool]:
    """
    Publish lineage for all pipeline stages at once.
    Useful for initial setup or re-registration.

    Returns a dict of {stage_name: success_bool}.
    """
    print(f"\nPublishing full Customer360 pipeline lineage to DataHub ({gms_url})")
    print("=" * 60)

    results = {}
    for stage in PIPELINE_STAGES:
        results[stage] = publish_stage_lineage(stage, gms_url)

    passed = sum(results.values())
    print(f"\nLineage published: {passed}/{len(results)} stages successful")
    return results


def print_lineage_map() -> None:
    """Print the full pipeline lineage map to stdout (no DataHub needed)."""
    print("\nCustomer360 Data Platform — Full Pipeline Lineage")
    print("=" * 60)
    for stage, config in PIPELINE_STAGES.items():
        print(f"\n  [{stage}]  {config['job_name']}")
        print(f"  Description: {config['description']}")
        if config["inputs"]:
            print("  Inputs:")
            for urn in config["inputs"]:
                print(f"    ← {urn}")
        print("  Outputs:")
        for urn in config["outputs"]:
            print(f"    → {urn}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Customer360 DataHub Lineage Publisher")
    parser.add_argument(
        "--stage",
        default="all",
        help="Stage to publish (e.g. 'bronze_to_silver') or 'all' for full pipeline",
    )
    parser.add_argument(
        "--gms-url",
        default=DATAHUB_GMS_URL,
        help=f"DataHub GMS URL (default: {DATAHUB_GMS_URL})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print lineage map without publishing to DataHub",
    )
    args = parser.parse_args()

    if args.dry_run:
        print_lineage_map()
    elif args.stage == "all":
        publish_full_pipeline_lineage(args.gms_url)
    else:
        publish_stage_lineage(args.stage, args.gms_url)
