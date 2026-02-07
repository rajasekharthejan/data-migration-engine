"""Business rule validators for insurance policy data.

Each rule encodes domain-specific validation logic that the FAST system requires.
Rules are composable and produce detailed validation reports for lineage tracking.
"""

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Callable, Optional

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


@dataclass
class ValidationResult:
    """Result of a single validation check."""

    rule_name: str
    passed: bool
    record_id: str
    field_name: Optional[str] = None
    expected: Optional[str] = None
    actual: Optional[str] = None
    message: Optional[str] = None
    severity: str = "warning"  # "info", "warning", "critical"


@dataclass
class ValidationReport:
    """Aggregated validation results for a batch."""

    total_records: int
    total_checks: int
    passed: int
    failed: int
    critical_failures: int
    results: list[ValidationResult] = field(default_factory=list)

    @property
    def pass_rate(self) -> float:
        return (self.passed / self.total_checks * 100) if self.total_checks > 0 else 0.0

    @property
    def is_acceptable(self) -> bool:
        """A batch is acceptable if there are zero critical failures and pass rate > 99.9%."""
        return self.critical_failures == 0 and self.pass_rate >= 99.9


class BusinessRuleValidator:
    """Validates transformed data against Prudential insurance business rules.

    Rules are registered as functions and run against each record. The validator
    produces a ValidationReport that feeds into lineage tracking and reconciliation.

    Example rules:
    - Policy effective date must be in the past
    - Premium amount must be positive and within product-type range
    - Beneficiary percentage must sum to exactly 100%
    - Policy status transitions must be valid (ACTIVE -> LAPSED, not LAPSED -> ACTIVE)
    """

    def __init__(self) -> None:
        self._rules: list[tuple[str, str, Callable, str]] = []
        self._register_default_rules()

    def _register_default_rules(self) -> None:
        """Register all Prudential-specific business rules."""
        self.register_rule(
            "policy_id_format",
            "POLICY_ID",
            self._validate_policy_id_format,
            "critical",
        )
        self.register_rule(
            "premium_positive",
            "PREMIUM_AMOUNT",
            self._validate_premium_positive,
            "critical",
        )
        self.register_rule(
            "coverage_exceeds_minimum",
            "COVERAGE_AMOUNT",
            self._validate_coverage_minimum,
            "warning",
        )
        self.register_rule(
            "effective_date_valid",
            "EFFECTIVE_DATE",
            self._validate_effective_date,
            "critical",
        )
        self.register_rule(
            "status_valid",
            "STATUS",
            self._validate_status,
            "critical",
        )
        self.register_rule(
            "product_type_valid",
            "PRODUCT_TYPE",
            self._validate_product_type,
            "critical",
        )
        self.register_rule(
            "premium_within_range",
            "PREMIUM_AMOUNT",
            self._validate_premium_range,
            "warning",
        )

    def register_rule(
        self,
        rule_name: str,
        field_name: str,
        validation_fn: Callable[[Any, pd.Series], ValidationResult],
        severity: str = "warning",
    ) -> None:
        """Register a new validation rule."""
        self._rules.append((rule_name, field_name, validation_fn, severity))
        logger.debug("rule_registered", rule=rule_name, field=field_name, severity=severity)

    def validate_batch(self, df: pd.DataFrame, key_column: str = "POLICY_ID") -> ValidationReport:
        """Run all registered rules against every record in the batch."""
        results: list[ValidationResult] = []
        total_checks = 0
        passed = 0
        failed = 0
        critical = 0

        for _, row in df.iterrows():
            record_id = str(row.get(key_column, "UNKNOWN"))
            for rule_name, field_name, fn, severity in self._rules:
                if field_name not in df.columns:
                    continue
                total_checks += 1
                try:
                    result = fn(row[field_name], row)
                    result.rule_name = rule_name
                    result.record_id = record_id
                    result.field_name = field_name
                    result.severity = severity
                except Exception as e:
                    result = ValidationResult(
                        rule_name=rule_name,
                        passed=False,
                        record_id=record_id,
                        field_name=field_name,
                        message=f"Rule execution error: {e}",
                        severity="critical",
                    )

                if result.passed:
                    passed += 1
                else:
                    failed += 1
                    if result.severity == "critical":
                        critical += 1
                    results.append(result)

        report = ValidationReport(
            total_records=len(df),
            total_checks=total_checks,
            passed=passed,
            failed=failed,
            critical_failures=critical,
            results=results,
        )

        logger.info(
            "validation_complete",
            total_records=report.total_records,
            pass_rate=f"{report.pass_rate:.2f}%",
            critical=report.critical_failures,
            acceptable=report.is_acceptable,
        )

        return report

    # ── Built-in Validation Rules ─────────────────────────────

    @staticmethod
    def _validate_policy_id_format(value: Any, row: pd.Series) -> ValidationResult:
        """Policy ID must match format: POL-XXXXXX (alphanumeric)."""
        pattern = r"^POL-[A-Z0-9]{6,10}$"
        is_valid = bool(re.match(pattern, str(value)))
        return ValidationResult(
            rule_name="",
            passed=is_valid,
            record_id="",
            expected="POL-XXXXXX format",
            actual=str(value),
            message=None if is_valid else f"Invalid policy ID format: {value}",
        )

    @staticmethod
    def _validate_premium_positive(value: Any, row: pd.Series) -> ValidationResult:
        """Premium amount must be positive."""
        try:
            amount = float(value)
            is_valid = amount > 0
        except (TypeError, ValueError):
            amount = 0
            is_valid = False
        return ValidationResult(
            rule_name="",
            passed=is_valid,
            record_id="",
            expected="> 0",
            actual=str(value),
            message=None if is_valid else f"Premium must be positive, got: {value}",
        )

    @staticmethod
    def _validate_coverage_minimum(value: Any, row: pd.Series) -> ValidationResult:
        """Coverage amount must be at least $1,000."""
        try:
            amount = float(value)
            is_valid = amount >= 1000.0
        except (TypeError, ValueError):
            amount = 0
            is_valid = False
        return ValidationResult(
            rule_name="",
            passed=is_valid,
            record_id="",
            expected=">= 1000.00",
            actual=str(value),
            message=None if is_valid else f"Coverage below minimum $1,000: {value}",
        )

    @staticmethod
    def _validate_effective_date(value: Any, row: pd.Series) -> ValidationResult:
        """Effective date must be a valid date and not in the future."""
        try:
            if isinstance(value, (date, datetime)):
                d = value if isinstance(value, date) else value.date()
            else:
                d = datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
            is_valid = d <= date.today()
        except (TypeError, ValueError):
            is_valid = False
            d = None
        return ValidationResult(
            rule_name="",
            passed=is_valid,
            record_id="",
            expected="valid date, not future",
            actual=str(value),
            message=None if is_valid else f"Invalid or future effective date: {value}",
        )

    @staticmethod
    def _validate_status(value: Any, row: pd.Series) -> ValidationResult:
        """Policy status must be one of the valid statuses."""
        valid_statuses = {"ACTIVE", "LAPSED", "PAID_UP", "SURRENDERED", "MATURED", "DEATH_CLAIM", "CANCELLED"}
        is_valid = str(value).upper() in valid_statuses
        return ValidationResult(
            rule_name="",
            passed=is_valid,
            record_id="",
            expected=f"One of {valid_statuses}",
            actual=str(value),
            message=None if is_valid else f"Invalid policy status: {value}",
        )

    @staticmethod
    def _validate_product_type(value: Any, row: pd.Series) -> ValidationResult:
        """Product type must be a known insurance product."""
        valid_types = {"TERM_LIFE", "WHOLE_LIFE", "ANNUITY", "UNIVERSAL", "VARIABLE", "GROUP_LIFE", "ENDOWMENT"}
        is_valid = str(value).upper() in valid_types
        return ValidationResult(
            rule_name="",
            passed=is_valid,
            record_id="",
            expected=f"One of {valid_types}",
            actual=str(value),
            message=None if is_valid else f"Unknown product type: {value}",
        )

    @staticmethod
    def _validate_premium_range(value: Any, row: pd.Series) -> ValidationResult:
        """Premium should be within reasonable range for the product type."""
        ranges = {
            "TERM_LIFE": (10.0, 5_000.0),
            "WHOLE_LIFE": (50.0, 20_000.0),
            "ANNUITY": (100.0, 100_000.0),
            "UNIVERSAL": (50.0, 50_000.0),
            "VARIABLE": (100.0, 50_000.0),
            "GROUP_LIFE": (5.0, 2_000.0),
            "ENDOWMENT": (50.0, 30_000.0),
        }
        try:
            amount = float(value)
            product = str(row.get("PRODUCT_TYPE", "")).upper()
            min_val, max_val = ranges.get(product, (1.0, 1_000_000.0))
            is_valid = min_val <= amount <= max_val
        except (TypeError, ValueError):
            is_valid = False
            amount = 0
        return ValidationResult(
            rule_name="",
            passed=is_valid,
            record_id="",
            expected=f"Within product range",
            actual=str(value),
            message=None if is_valid else f"Premium {value} outside range for product type",
        )
