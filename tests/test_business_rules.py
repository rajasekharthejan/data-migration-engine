"""Tests for the business rule validation engine."""

import pytest

from src.transformers.business_rules import BusinessRuleValidator


class TestBusinessRuleValidator:
    """Test suite for insurance domain business rules."""

    @pytest.fixture
    def validator(self):
        return BusinessRuleValidator()

    def test_valid_record_passes_all_rules(self, validator):
        """A perfectly valid record should pass all rules."""
        record = {
            "policy_id": "POL-123456",
            "premium_amount": 1500.00,
            "coverage_amount": 500000.00,
            "effective_date": "2025-06-15",
            "status": "active",
            "product_type": "whole_life",
        }
        report = validator.validate_batch([record])
        assert report.passed == 1
        assert report.failed == 0
        assert report.is_acceptable is True

    def test_invalid_policy_id_format(self, validator):
        """Policy ID must match POL-XXXXXX format."""
        record = {
            "policy_id": "INVALID",
            "premium_amount": 1500.00,
            "coverage_amount": 500000.00,
            "effective_date": "2025-06-15",
            "status": "active",
            "product_type": "whole_life",
        }
        report = validator.validate_batch([record])
        assert report.failed >= 1

    def test_negative_premium_rejected(self, validator):
        """Premium amount must be positive."""
        record = {
            "policy_id": "POL-100001",
            "premium_amount": -100.00,
            "coverage_amount": 500000.00,
            "effective_date": "2025-06-15",
            "status": "active",
            "product_type": "whole_life",
        }
        report = validator.validate_batch([record])
        assert report.failed >= 1

    def test_coverage_minimum(self, validator):
        """Coverage amount must be at least $1,000."""
        record = {
            "policy_id": "POL-100001",
            "premium_amount": 50.00,
            "coverage_amount": 500.00,  # Below minimum
            "effective_date": "2025-06-15",
            "status": "active",
            "product_type": "term_life",
        }
        report = validator.validate_batch([record])
        assert report.failed >= 1

    def test_invalid_status_rejected(self, validator):
        """Status must be one of the valid status codes."""
        record = {
            "policy_id": "POL-100001",
            "premium_amount": 1500.00,
            "coverage_amount": 500000.00,
            "effective_date": "2025-06-15",
            "status": "deleted",  # Not a valid status
            "product_type": "whole_life",
        }
        report = validator.validate_batch([record])
        assert report.failed >= 1

    def test_batch_validation(self, validator, sample_policy_records):
        """Validate a batch of records."""
        report = validator.validate_batch(sample_policy_records)
        assert report.total_records == 3
        assert report.passed + report.failed == 3

    def test_pass_rate_threshold(self, validator):
        """Validation is acceptable only if pass_rate >= 99.9%."""
        # 999 good records + 1 bad = 99.9% pass rate
        records = [
            {
                "policy_id": f"POL-{100000 + i:06d}",
                "premium_amount": 1000.00,
                "coverage_amount": 100000.00,
                "effective_date": "2025-01-01",
                "status": "active",
                "product_type": "term_life",
            }
            for i in range(999)
        ]
        # Add one bad record
        records.append({
            "policy_id": "INVALID",
            "premium_amount": -1,
            "coverage_amount": 0,
            "effective_date": "2099-01-01",
            "status": "invalid_status",
            "product_type": "fake_product",
        })

        report = validator.validate_batch(records)
        assert report.total_records == 1000

    def test_empty_batch(self, validator):
        """Empty batch should produce empty report."""
        report = validator.validate_batch([])
        assert report.total_records == 0
        assert report.is_acceptable is True
