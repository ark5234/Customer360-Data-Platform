"""
Customer360 Data Platform
Data Quality Framework

Computes DQ metrics across bronze/silver datasets.
Results are written to PostgreSQL for Grafana dashboards.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass
class DQRule:
    name: str
    description: str
    check_fn: Callable[[DataFrame], DataFrame]
    severity: str = "error"  # error | warning | info
    threshold: float = 0.0  # max allowed failure rate (0.0 = zero tolerance)


@dataclass
class DQResult:
    rule_name: str
    table: str
    total_records: int
    failed_records: int
    failure_rate: float
    passed: bool
    severity: str
    checked_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            "rule_name": self.rule_name,
            "table": self.table,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "failure_rate": round(self.failure_rate, 4),
            "passed": self.passed,
            "severity": self.severity,
            "checked_at": self.checked_at.isoformat(),
        }


class DataQualityEngine:
    """Run a suite of DQ rules against a Spark DataFrame."""

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.rules: list[DQRule] = []
        self.results: list[DQResult] = []

    def add_rule(self, rule: DQRule) -> "DataQualityEngine":
        self.rules.append(rule)
        return self

    def run(self, df: DataFrame, table_name: str) -> list[DQResult]:
        total = df.count()
        self.results = []

        print(
            f"\nRunning {len(self.rules)} DQ rules on '{table_name}' ({total:,} records)..."
        )

        for rule in self.rules:
            failed_df = rule.check_fn(df)
            failed_count = failed_df.count()
            failure_rate = failed_count / total if total > 0 else 0
            passed = failure_rate <= rule.threshold

            result = DQResult(
                rule_name=rule.name,
                table=table_name,
                total_records=total,
                failed_records=failed_count,
                failure_rate=failure_rate,
                passed=passed,
                severity=rule.severity,
            )
            self.results.append(result)

            icon = "✓" if passed else "✗"
            print(
                f"  {icon} {rule.name}: {failed_count:,} failures ({failure_rate:.2%}) — {'PASS' if passed else 'FAIL'}"
            )

        return self.results

    def summary(self) -> dict:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        return {
            "total_rules": total,
            "passed": passed,
            "failed": total - passed,
            "pass_rate": passed / total if total > 0 else 0,
        }


# ─────────────────────────────────────────────
# Standard Rules Library
# ─────────────────────────────────────────────

STANDARD_RULES = [
    DQRule(
        name="no_null_customer_id",
        description="customer_id must never be null",
        check_fn=lambda df: df.filter(F.col("customer_id").isNull()),
        severity="error",
        threshold=0.0,
    ),
    DQRule(
        name="no_null_event_type",
        description="event_type must never be null",
        check_fn=lambda df: df.filter(F.col("event_type").isNull()),
        severity="error",
        threshold=0.0,
    ),
    DQRule(
        name="no_null_timestamp",
        description="timestamp must never be null",
        check_fn=lambda df: df.filter(F.col("event_timestamp").isNull()),
        severity="error",
        threshold=0.0,
    ),
    DQRule(
        name="valid_event_types",
        description="event_type must be one of the known types",
        check_fn=lambda df: df.filter(
            ~F.col("event_type").isin(
                [
                    "LOGIN",
                    "LOGOUT",
                    "PRODUCT_VIEW",
                    "SEARCH",
                    "ADD_TO_CART",
                    "PURCHASE",
                    "REFUND",
                    "SUBSCRIPTION",
                    "PAYMENT_FAILURE",
                ]
            )
        ),
        severity="error",
        threshold=0.0,
    ),
    DQRule(
        name="no_negative_amounts",
        description="Monetary amounts must be >= 0",
        check_fn=lambda df: df.filter(
            F.col("total_amount").isNotNull() & (F.col("total_amount") < 0)
        ),
        severity="error",
        threshold=0.0,
    ),
    DQRule(
        name="no_future_timestamps",
        description="event_timestamp must not be in the future",
        check_fn=lambda df: df.filter(F.col("event_timestamp") > F.current_timestamp()),
        severity="warning",
        threshold=0.001,  # Allow 0.1% — could be clock skew
    ),
    DQRule(
        name="duplicate_events",
        description="event_id must be unique",
        check_fn=lambda df: (
            df.groupBy("event_id")
            .count()
            .filter(F.col("count") > 1)
            .join(df, "event_id")
        ),
        severity="warning",
        threshold=0.0,
    ),
    DQRule(
        name="valid_customer_id_format",
        description="customer_id must match pattern C[0-9]+",
        check_fn=lambda df: df.filter(
            F.col("customer_id").isNotNull() & ~F.col("customer_id").rlike(r"^C\d+$")
        ),
        severity="warning",
        threshold=0.001,
    ),
    DQRule(
        name="purchase_has_order_id",
        description="PURCHASE events must have an order_id",
        check_fn=lambda df: df.filter(
            (F.col("event_type") == "PURCHASE") & F.col("order_id").isNull()
        ),
        severity="error",
        threshold=0.0,
    ),
    DQRule(
        name="purchase_has_amount",
        description="PURCHASE events must have total_amount > 0",
        check_fn=lambda df: df.filter(
            (F.col("event_type") == "PURCHASE")
            & (F.col("total_amount").isNull() | (F.col("total_amount") <= 0))
        ),
        severity="error",
        threshold=0.0,
    ),
]


def run_standard_dq(spark: SparkSession, path: str, table_name: str) -> list[DQResult]:
    """Run all standard DQ rules on a dataset."""
    df = spark.read.parquet(path)
    engine = DataQualityEngine(spark)
    for rule in STANDARD_RULES:
        engine.add_rule(rule)
    results = engine.run(df, table_name)

    summary = engine.summary()
    print(
        f"\nSummary: {summary['passed']}/{summary['total_rules']} rules passed ({summary['pass_rate']:.1%})"
    )

    return results


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Customer360-DQ").getOrCreate()
    run_standard_dq(
        spark, path="s3a://customer360-silver/events/", table_name="silver_events"
    )
