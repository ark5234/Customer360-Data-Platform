import sys
from pathlib import Path

import pandas as pd

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data_quality.ge_suite import build_expectation_suite, run_ge_validation


def test_build_expectation_suite():
    """Test that the GE suite definition is built correctly."""
    suite = build_expectation_suite()
    assert suite["expectation_suite_name"] == "customer360_silver_suite"
    assert len(suite["expectations"]) == 12


def test_ge_validation_success():
    """Test GE validation passes on valid data."""
    df = pd.DataFrame(
        {
            "event_id": ["E1", "E2", "E3"],
            "customer_id": ["C1", "C2", "C3"],
            "event_type": ["LOGIN", "PURCHASE", "LOGOUT"],
            "event_timestamp": pd.to_datetime(["2026-06-07"] * 3),
            "session_id": ["S1", "S2", "S3"],
            "device": ["Mobile", "Desktop", "Tablet"],
            "region": ["Mumbai", "Delhi", "Bangalore"],
            "total_amount": [0, 100, 0],
            "dq_passed": [True, True, True],
        }
    )

    result = run_ge_validation(df)
    summary = result.summary()
    assert summary["success"] is True


def test_ge_validation_failure():
    """Test GE validation fails on invalid data."""
    df = pd.DataFrame(
        {
            "event_id": ["E1", "E1"],  # Duplicate
            "customer_id": [None, "C2"],  # Null
            "event_type": ["UNKNOWN", "PURCHASE"],  # Invalid domain
            "event_timestamp": pd.to_datetime(["2026-06-07"] * 2),
            "session_id": ["S1", "S2"],
            "device": ["SmartWatch", "Desktop"],  # Invalid device
            "region": ["Mumbai", "Delhi"],
            "total_amount": [-100, 100],  # Negative amount
            "dq_passed": [True, True],
        }
    )

    result = run_ge_validation(df)
    summary = result.summary()
    assert summary["success"] is False
    assert summary["failed"] > 0
