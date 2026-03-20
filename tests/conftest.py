"""Shared test fixtures for the data migration engine."""

import pandas as pd
import pytest


@pytest.fixture
def sample_policy_records() -> list[dict]:
    """Generate sample insurance policy records for testing."""
    return [
        {
            "policy_id": "POL-100001",
            "first_name": "JOHN",
            "last_name": "DOE",
            "premium_amount": 1250.50,
            "coverage_amount": 500000.00,
            "effective_date": "2026-01-15",
            "status": "active",
            "product_type": "whole_life",
            "phone_number": "5551234567",
            "ssn": "123-45-6789",
        },
        {
            "policy_id": "POL-100002",
            "first_name": "JANE",
            "last_name": "SMITH",
            "premium_amount": 850.00,
            "coverage_amount": 250000.00,
            "effective_date": "2025-06-01",
            "status": "active",
            "product_type": "term_life",
            "phone_number": "(555) 987-6543",
            "ssn": "987-65-4321",
        },
        {
            "policy_id": "POL-100003",
            "first_name": "BOB",
            "last_name": "JOHNSON",
            "premium_amount": 3200.75,
            "coverage_amount": 1000000.00,
            "effective_date": "2024-11-30",
            "status": "lapsed",
            "product_type": "universal_life",
            "phone_number": "555.456.7890",
            "ssn": "456-78-9012",
        },
    ]


@pytest.fixture
def sample_source_df(sample_policy_records) -> pd.DataFrame:
    """Create a source DataFrame from sample records."""
    return pd.DataFrame(sample_policy_records)


@pytest.fixture
def sample_target_df(sample_policy_records) -> pd.DataFrame:
    """Create a target DataFrame that matches source (for reconciliation)."""
    return pd.DataFrame(sample_policy_records)


@pytest.fixture
def mismatched_target_df(sample_policy_records) -> pd.DataFrame:
    """Create a target DataFrame with deliberate mismatches."""
    records = sample_policy_records.copy()
    # Modify one record to create a mismatch
    records[1] = records[1].copy()
    records[1]["premium_amount"] = 999.99
    records[1]["status"] = "cancelled"
    return pd.DataFrame(records)
