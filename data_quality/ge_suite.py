"""
Customer360 Data Platform
Great Expectations Data Quality Suite

Validates Silver-layer events using GE expectation suites.
Runs as part of the Bronze→Silver Airflow DAG after cleaning.
Results are stored as GE Data Docs (HTML reports) in MinIO.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

# ── Great Expectations imports ────────────────────────────────────────────────
try:
    import great_expectations as gx  # noqa: F401
    from great_expectations.core.batch import RuntimeBatchRequest  # noqa: F401
    from great_expectations.data_context.types.base import (  # noqa: F401
        DataContextConfig,
        InMemoryStoreBackendDefaults,
    )

    GE_AVAILABLE = True
except ImportError:
    GE_AVAILABLE = False
    print("WARNING: great-expectations not installed. DQ suite skipped.")

SUITE_NAME = "customer360_silver_suite"
SUITE_FILE = Path(__file__).parent / "expectations" / "customer360_suite.json"


# ─────────────────────────────────────────────────────────────────────────────
# Expectation Suite Builder
# ─────────────────────────────────────────────────────────────────────────────


def build_expectation_suite() -> dict:
    """
    Build the Customer360 Silver-layer expectation suite programmatically.

    12 expectations covering:
    - Null / completeness checks (4 rules)
    - Schema / type checks (3 rules)
    - Duplicate detection (1 rule)
    - Value-range / domain checks (4 rules)
    """
    return {
        "expectation_suite_name": SUITE_NAME,
        "expectations": [
            # ── 1. Null / Completeness ───────────────────────────────────────
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "customer_id"},
                "meta": {"rule": "null_check", "severity": "critical"},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "event_type"},
                "meta": {"rule": "null_check", "severity": "critical"},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "event_timestamp"},
                "meta": {"rule": "null_check", "severity": "critical"},
            },
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "event_id"},
                "meta": {"rule": "null_check", "severity": "critical"},
            },
            # ── 2. Schema / Type Checks ──────────────────────────────────────
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "customer_id"},
                "meta": {"rule": "schema_check"},
            },
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "event_type"},
                "meta": {"rule": "schema_check"},
            },
            {
                "expectation_type": "expect_table_columns_to_match_set",
                "kwargs": {
                    "column_set": [
                        "event_id",
                        "customer_id",
                        "event_type",
                        "event_timestamp",
                        "session_id",
                        "device",
                        "region",
                        "total_amount",
                        "dq_passed",
                    ],
                    "exact_match": False,  # allow additional columns
                },
                "meta": {"rule": "schema_check"},
            },
            # ── 3. Duplicate Detection ───────────────────────────────────────
            {
                "expectation_type": "expect_column_values_to_be_unique",
                "kwargs": {"column": "event_id"},
                "meta": {"rule": "duplicate_detection", "severity": "warning"},
            },
            # ── 4. Domain / Value-Range Checks ──────────────────────────────
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "event_type",
                    "value_set": [
                        "LOGIN",
                        "LOGOUT",
                        "PRODUCT_VIEW",
                        "SEARCH",
                        "ADD_TO_CART",
                        "PURCHASE",
                        "REFUND",
                        "SUBSCRIPTION",
                        "PAYMENT_FAILURE",
                    ],
                },
                "meta": {"rule": "domain_check", "severity": "critical"},
            },
            {
                "expectation_type": "expect_column_values_to_be_in_set",
                "kwargs": {
                    "column": "device",
                    "value_set": ["Mobile", "Desktop", "Tablet"],
                },
                "meta": {"rule": "domain_check", "severity": "warning"},
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "total_amount",
                    "min_value": 0,
                    "max_value": 1_000_000,
                    "mostly": 0.999,  # allow 0.1% outliers
                },
                "meta": {"rule": "range_check", "severity": "warning"},
            },
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"min_value": 1, "max_value": 50_000_000},
                "meta": {"rule": "volume_check", "severity": "info"},
            },
        ],
        "meta": {
            "great_expectations_version": "0.18.x",
            "created": datetime.utcnow().isoformat(),
            "description": "Customer360 Silver-layer validation suite — 12 expectations",
        },
    }


def save_suite_to_file() -> None:
    """Persist the expectation suite to JSON (version-controlled)."""
    SUITE_FILE.parent.mkdir(parents=True, exist_ok=True)
    suite = build_expectation_suite()
    with open(SUITE_FILE, "w") as f:
        json.dump(suite, f, indent=2)
    print(f"Expectation suite saved → {SUITE_FILE}")


# ─────────────────────────────────────────────────────────────────────────────
# Pandas-based Validation (used inside Airflow without Spark)
# ─────────────────────────────────────────────────────────────────────────────


class GESuiteResult:
    """Lightweight result object returned by run_ge_validation."""

    def __init__(self, suite_name: str):
        self.suite_name = suite_name
        self.results: list[dict] = []
        self.validated_at = datetime.utcnow()

    def add(self, expectation: str, passed: bool, details: dict) -> None:
        self.results.append({"expectation": expectation, "passed": passed, **details})

    @property
    def passed_count(self) -> int:
        return sum(1 for r in self.results if r["passed"])

    @property
    def failed_count(self) -> int:
        return len(self.results) - self.passed_count

    @property
    def success(self) -> bool:
        """Suite passes if all *critical* expectations pass."""
        return all(r["passed"] for r in self.results if r.get("severity") == "critical")

    def summary(self) -> dict:
        return {
            "suite": self.suite_name,
            "total": len(self.results),
            "passed": self.passed_count,
            "failed": self.failed_count,
            "pass_rate": (
                self.passed_count / len(self.results) if self.results else 0.0
            ),
            "success": self.success,
            "validated_at": self.validated_at.isoformat(),
        }

    def print_report(self) -> None:
        print(f"\n{'=' * 60}")
        print("  Great Expectations Validation Report")
        print(f"  Suite: {self.suite_name}")
        print(f"{'=' * 60}")
        for r in self.results:
            icon = "✓" if r["passed"] else "✗"
            sev = r.get("severity", "info")
            print(f"  {icon} [{sev.upper():8s}] {r['expectation']}")
            if not r["passed"] and r.get("detail"):
                print(f"           → {r['detail']}")
        s = self.summary()
        print(
            f"\n  Result: {s['passed']}/{s['total']} passed "
            f"({s['pass_rate']:.1%}) | Suite {'PASSED ✓' if s['success'] else 'FAILED ✗'}"
        )
        print(f"{'=' * 60}\n")


def run_ge_validation(
    df: pd.DataFrame, dataset_name: str = "silver_events"
) -> GESuiteResult:
    """
    Run the Customer360 GE suite against a pandas DataFrame.

    This is the Airflow-compatible path (no live GE context needed).
    For full HTML Data Docs, use run_ge_checkpoint() instead.
    """
    suite = build_expectation_suite()
    result = GESuiteResult(suite["expectation_suite_name"])
    total = len(df)

    for exp in suite["expectations"]:
        etype = exp["expectation_type"]
        kwargs = exp.get("kwargs", {})
        meta = exp.get("meta", {})
        severity = meta.get("severity", "info")

        passed = True
        detail = ""

        try:
            if etype == "expect_column_values_to_not_be_null":
                col = kwargs["column"]
                if col in df.columns:
                    null_count = df[col].isna().sum()
                    passed = null_count == 0
                    detail = f"{null_count:,} nulls in '{col}'"
                else:
                    passed = False
                    detail = f"Column '{col}' missing"

            elif etype == "expect_column_to_exist":
                col = kwargs["column"]
                passed = col in df.columns
                detail = f"Column '{col}' {'exists' if passed else 'MISSING'}"

            elif etype == "expect_column_values_to_be_unique":
                col = kwargs["column"]
                if col in df.columns:
                    dups = df[col].duplicated().sum()
                    passed = dups == 0
                    detail = f"{dups:,} duplicate values in '{col}'"
                else:
                    passed = False

            elif etype == "expect_column_values_to_be_in_set":
                col = kwargs["column"]
                value_set = set(kwargs["value_set"])
                if col in df.columns:
                    invalid = (~df[col].isin(value_set)).sum()
                    passed = invalid == 0
                    detail = f"{invalid:,} invalid values in '{col}'"
                else:
                    passed = False

            elif etype == "expect_column_values_to_be_between":
                col = kwargs["column"]
                mostly = kwargs.get("mostly", 1.0)
                if col in df.columns:
                    numeric = pd.to_numeric(df[col], errors="coerce").dropna()
                    violations = (
                        (numeric < kwargs.get("min_value", float("-inf")))
                        | (numeric > kwargs.get("max_value", float("inf")))
                    ).sum()
                    violation_rate = (
                        violations / len(numeric) if len(numeric) > 0 else 0
                    )
                    passed = violation_rate <= (1 - mostly)
                    detail = (
                        f"{violations:,} out-of-range values ({violation_rate:.2%})"
                    )
                else:
                    passed = True  # column absent → skip range check

            elif etype == "expect_table_row_count_to_be_between":
                min_v = kwargs.get("min_value", 0)
                max_v = kwargs.get("max_value", float("inf"))
                passed = min_v <= total <= max_v
                detail = f"{total:,} rows (expected {min_v}–{max_v})"

            elif etype == "expect_table_columns_to_match_set":
                required = set(kwargs.get("column_set", []))
                actual = set(df.columns)
                missing = required - actual
                passed = len(missing) == 0
                detail = (
                    f"Missing columns: {missing}"
                    if missing
                    else "All required columns present"
                )

        except Exception as e:
            passed = False
            detail = f"Exception: {e}"

        result.add(etype, passed, {"severity": severity, "detail": detail})

    result.print_report()
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Entry point for standalone use
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Save suite definition to file
    save_suite_to_file()

    # Demo: validate a synthetic DataFrame
    import numpy as np

    demo_df = pd.DataFrame(
        {
            "event_id": [f"E{i}" for i in range(1000)],
            "customer_id": [f"C{i % 500}" for i in range(1000)],
            "event_type": np.random.choice(
                ["LOGIN", "PRODUCT_VIEW", "PURCHASE", "REFUND"], 1000
            ),
            "event_timestamp": pd.date_range("2026-01-01", periods=1000, freq="1min"),
            "session_id": [f"S{i}" for i in range(1000)],
            "device": np.random.choice(["Mobile", "Desktop", "Tablet"], 1000),
            "region": np.random.choice(["Mumbai", "Delhi", "Bangalore"], 1000),
            "total_amount": np.random.uniform(0, 10000, 1000),
            "dq_passed": True,
        }
    )

    result = run_ge_validation(demo_df, "demo_silver_events")
    print(f"\nFinal summary: {result.summary()}")
