"""Tests for the record-by-record reconciliation engine."""

import pandas as pd
import pytest

from src.reconciliation.reconciler import (
    FieldMismatch,
    ReconciliationReport,
    Reconciler,
    RecordResult,
)


class TestReconciler:
    """Test suite for the Reconciler class."""

    def test_perfect_match(self, sample_source_df, sample_target_df):
        """When source and target are identical, all records should match."""
        reconciler = Reconciler(key_columns=["policy_id"])
        report = reconciler.reconcile(sample_source_df, sample_target_df)

        assert report.matched == 3
        assert report.mismatched == 0
        assert report.missing_in_target == 0
        assert report.missing_in_source == 0
        assert report.match_rate == 100.0
        assert report.is_acceptable is True

    def test_value_mismatch(self, sample_source_df, mismatched_target_df):
        """Detect field-level mismatches between source and target."""
        reconciler = Reconciler(key_columns=["policy_id"])
        report = reconciler.reconcile(sample_source_df, mismatched_target_df)

        assert report.matched == 2
        assert report.mismatched == 1
        assert report.match_rate < 100.0

    def test_missing_in_target(self, sample_source_df):
        """Detect records present in source but missing from target."""
        target_df = sample_source_df.iloc[:2].copy()  # Only first 2 records
        reconciler = Reconciler(key_columns=["policy_id"])
        report = reconciler.reconcile(sample_source_df, target_df)

        assert report.missing_in_target == 1
        assert report.is_acceptable is False  # Missing records = not acceptable

    def test_missing_in_source(self, sample_source_df):
        """Detect extra records in target not present in source."""
        source_df = sample_source_df.iloc[:2].copy()
        reconciler = Reconciler(key_columns=["policy_id"])
        report = reconciler.reconcile(source_df, sample_source_df)

        assert report.missing_in_source == 1

    def test_numeric_tolerance(self):
        """Numeric fields should match within tolerance."""
        source = pd.DataFrame({
            "id": ["1"],
            "amount": [100.0001],
        })
        target = pd.DataFrame({
            "id": ["1"],
            "amount": [100.0002],
        })

        reconciler = Reconciler(key_columns=["id"], numeric_tolerance=0.001)
        report = reconciler.reconcile(source, target)
        assert report.matched == 1

        # Tighter tolerance should catch the difference
        strict = Reconciler(key_columns=["id"], numeric_tolerance=0.00001)
        report2 = strict.reconcile(source, target)
        assert report2.mismatched == 1

    def test_null_handling(self):
        """Both null = match, one null = mismatch."""
        source = pd.DataFrame({
            "id": ["1", "2"],
            "value": [None, "hello"],
        })
        target = pd.DataFrame({
            "id": ["1", "2"],
            "value": [None, None],
        })

        reconciler = Reconciler(key_columns=["id"])
        report = reconciler.reconcile(source, target)

        # Record 1: both null = match. Record 2: one null = mismatch
        assert report.matched == 1
        assert report.mismatched == 1

    def test_composite_key(self):
        """Support multi-column composite keys."""
        source = pd.DataFrame({
            "region": ["US", "EU"],
            "policy_id": ["P1", "P1"],
            "amount": [100, 200],
        })
        target = pd.DataFrame({
            "region": ["US", "EU"],
            "policy_id": ["P1", "P1"],
            "amount": [100, 200],
        })

        reconciler = Reconciler(key_columns=["region", "policy_id"])
        report = reconciler.reconcile(source, target)
        assert report.matched == 2

    def test_ignore_columns(self):
        """Ignored columns should not be compared."""
        source = pd.DataFrame({"id": ["1"], "data": ["old"], "audit_ts": ["2026-01-01"]})
        target = pd.DataFrame({"id": ["1"], "data": ["old"], "audit_ts": ["2026-03-30"]})

        reconciler = Reconciler(key_columns=["id"], ignore_columns=["audit_ts"])
        report = reconciler.reconcile(source, target)
        assert report.matched == 1

    def test_case_insensitive_string_compare(self):
        """String comparisons should be case-insensitive."""
        source = pd.DataFrame({"id": ["1"], "name": ["JOHN DOE"]})
        target = pd.DataFrame({"id": ["1"], "name": ["John Doe"]})

        reconciler = Reconciler(key_columns=["id"])
        report = reconciler.reconcile(source, target)
        assert report.matched == 1

    def test_empty_dataframes(self):
        """Handle empty DataFrames gracefully."""
        empty = pd.DataFrame(columns=["id", "value"])
        reconciler = Reconciler(key_columns=["id"])
        report = reconciler.reconcile(empty, empty)

        assert report.matched == 0
        assert report.match_rate == 0.0


class TestReconciliationReport:
    """Test the ReconciliationReport dataclass."""

    def test_match_rate_calculation(self):
        report = ReconciliationReport(
            total_source=100,
            total_target=100,
            matched=99,
            mismatched=1,
            missing_in_target=0,
            missing_in_source=0,
        )
        assert report.match_rate == 99.0

    def test_acceptable_threshold(self):
        """99.99% match rate with zero missing = acceptable."""
        report = ReconciliationReport(
            total_source=10000,
            total_target=10000,
            matched=9999,
            mismatched=1,
            missing_in_target=0,
            missing_in_source=0,
        )
        assert report.match_rate == 99.99
        assert report.is_acceptable is True

    def test_unacceptable_missing(self):
        """Any missing_in_target = not acceptable, even with high match rate."""
        report = ReconciliationReport(
            total_source=10000,
            total_target=9999,
            matched=9999,
            mismatched=0,
            missing_in_target=1,
            missing_in_source=0,
        )
        assert report.is_acceptable is False
