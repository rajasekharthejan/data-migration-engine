"""Record-by-record reconciliation engine.

Compares source (DB2 extract) against target (FAST loaded data) field-by-field
to verify data integrity after migration. Produces detailed mismatch reports.
"""

import math
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class FieldMismatch:
    """A single field-level mismatch between source and target."""

    field_name: str
    source_value: Any
    target_value: Any
    mismatch_type: str  # "value", "type", "missing", "extra"


@dataclass
class RecordResult:
    """Reconciliation result for a single record."""

    source_id: str
    target_id: Optional[str]
    status: str  # "matched", "mismatched", "missing_in_target", "missing_in_source"
    mismatches: list[FieldMismatch] = field(default_factory=list)
    severity: str = "info"


@dataclass
class ReconciliationReport:
    """Aggregated reconciliation results for a batch or job."""

    total_source: int
    total_target: int
    matched: int
    mismatched: int
    missing_in_target: int
    missing_in_source: int
    results: list[RecordResult] = field(default_factory=list)

    @property
    def match_rate(self) -> float:
        total = self.matched + self.mismatched + self.missing_in_target + self.missing_in_source
        return (self.matched / total * 100) if total > 0 else 0.0

    @property
    def is_acceptable(self) -> bool:
        """99.99% match rate required for production promotion."""
        return self.match_rate >= 99.99 and self.missing_in_target == 0


class Reconciler:
    """Compare source and target data record-by-record, field-by-field.

    Strategy:
    1. Join source and target on key field(s)
    2. For matched keys: compare every field within tolerance
    3. Identify records in source but not target (missing_in_target)
    4. Identify records in target but not source (missing_in_source / extra)
    5. Produce detailed report for each mismatch

    Numeric tolerance:
        Floating point fields use configurable tolerance (default 0.001).
        This handles precision differences between DB2 packed decimal and
        FAST system's float/decimal representations.
    """

    def __init__(
        self,
        key_columns: list[str],
        compare_columns: Optional[list[str]] = None,
        numeric_tolerance: float = 0.001,
        ignore_columns: Optional[list[str]] = None,
    ):
        self.key_columns = key_columns
        self.compare_columns = compare_columns
        self.numeric_tolerance = numeric_tolerance
        self.ignore_columns = set(ignore_columns or [])

    def reconcile(
        self,
        source_df: pd.DataFrame,
        target_df: pd.DataFrame,
    ) -> ReconciliationReport:
        """Run full reconciliation between source and target DataFrames."""
        logger.info(
            "reconciliation_started",
            source_records=len(source_df),
            target_records=len(target_df),
            key_columns=self.key_columns,
        )

        # Build key for joining
        source_keys = self._build_composite_key(source_df)
        target_keys = self._build_composite_key(target_df)

        source_df = source_df.copy()
        target_df = target_df.copy()
        source_df["_recon_key"] = source_keys
        target_df["_recon_key"] = target_keys

        # Determine columns to compare
        if self.compare_columns:
            cols = [c for c in self.compare_columns if c not in self.ignore_columns]
        else:
            common = set(source_df.columns) & set(target_df.columns) - {"_recon_key"} - self.ignore_columns
            cols = sorted(common)

        results: list[RecordResult] = []
        matched = 0
        mismatched = 0
        missing_in_target = 0
        missing_in_source = 0

        # Find records in source but not target
        source_only = set(source_keys) - set(target_keys)
        for key in source_only:
            results.append(RecordResult(
                source_id=str(key),
                target_id=None,
                status="missing_in_target",
                severity="critical",
            ))
            missing_in_target += 1

        # Find records in target but not source
        target_only = set(target_keys) - set(source_keys)
        for key in target_only:
            results.append(RecordResult(
                source_id="",
                target_id=str(key),
                status="missing_in_source",
                severity="warning",
            ))
            missing_in_source += 1

        # Compare matched records field-by-field
        common_keys = set(source_keys) & set(target_keys)
        source_indexed = source_df.set_index("_recon_key")
        target_indexed = target_df.set_index("_recon_key")

        for key in common_keys:
            source_row = source_indexed.loc[key]
            target_row = target_indexed.loc[key]

            # Handle case where key has duplicates (take first)
            if isinstance(source_row, pd.DataFrame):
                source_row = source_row.iloc[0]
            if isinstance(target_row, pd.DataFrame):
                target_row = target_row.iloc[0]

            mismatches = self._compare_fields(source_row, target_row, cols)

            if mismatches:
                severity = "critical" if any(
                    m.mismatch_type == "value" and m.field_name in self.key_columns
                    for m in mismatches
                ) else "warning"

                results.append(RecordResult(
                    source_id=str(key),
                    target_id=str(key),
                    status="mismatched",
                    mismatches=mismatches,
                    severity=severity,
                ))
                mismatched += 1
            else:
                matched += 1

        report = ReconciliationReport(
            total_source=len(source_df),
            total_target=len(target_df),
            matched=matched,
            mismatched=mismatched,
            missing_in_target=missing_in_target,
            missing_in_source=missing_in_source,
            results=results,
        )

        logger.info(
            "reconciliation_complete",
            matched=matched,
            mismatched=mismatched,
            missing_in_target=missing_in_target,
            missing_in_source=missing_in_source,
            match_rate=f"{report.match_rate:.4f}%",
            acceptable=report.is_acceptable,
        )

        return report

    def _build_composite_key(self, df: pd.DataFrame) -> pd.Series:
        """Build a composite key from multiple columns."""
        if len(self.key_columns) == 1:
            return df[self.key_columns[0]].astype(str)
        return df[self.key_columns].astype(str).agg("|".join, axis=1)

    def _compare_fields(
        self,
        source_row: pd.Series,
        target_row: pd.Series,
        columns: list[str],
    ) -> list[FieldMismatch]:
        """Compare all fields between source and target row."""
        mismatches: list[FieldMismatch] = []

        for col in columns:
            src_val = source_row.get(col)
            tgt_val = target_row.get(col)

            # Both null = match
            if pd.isna(src_val) and pd.isna(tgt_val):
                continue

            # One null, other not = mismatch
            if pd.isna(src_val) != pd.isna(tgt_val):
                mismatches.append(FieldMismatch(
                    field_name=col,
                    source_value=src_val,
                    target_value=tgt_val,
                    mismatch_type="missing" if pd.isna(tgt_val) else "extra",
                ))
                continue

            # Numeric comparison with tolerance
            if self._is_numeric(src_val) and self._is_numeric(tgt_val):
                if not math.isclose(float(src_val), float(tgt_val), abs_tol=self.numeric_tolerance):
                    mismatches.append(FieldMismatch(
                        field_name=col,
                        source_value=src_val,
                        target_value=tgt_val,
                        mismatch_type="value",
                    ))
                continue

            # String comparison (case-insensitive, trimmed)
            if str(src_val).strip().lower() != str(tgt_val).strip().lower():
                mismatches.append(FieldMismatch(
                    field_name=col,
                    source_value=src_val,
                    target_value=tgt_val,
                    mismatch_type="value",
                ))

        return mismatches

    @staticmethod
    def _is_numeric(value: Any) -> bool:
        try:
            float(value)
            return True
        except (TypeError, ValueError):
            return False
