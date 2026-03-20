"""Tests for the field transformation engine."""

import pandas as pd
import pytest

from src.transformers.field_transformer import FieldTransformer


class TestFieldTransformer:
    """Test individual field transformation functions."""

    @pytest.fixture
    def transformer(self):
        return FieldTransformer()

    def test_trim_string(self, transformer):
        assert transformer.transforms["trim_string"]("  hello  ") == "hello"
        assert transformer.transforms["trim_string"]("no_trim") == "no_trim"

    def test_name_title_case(self, transformer):
        assert transformer.transforms["name_title_case"]("JOHN DOE") == "John Doe"
        assert transformer.transforms["name_title_case"]("jane smith") == "Jane Smith"

    def test_ssn_mask(self, transformer):
        assert transformer.transforms["ssn_mask"]("123-45-6789") == "***-**-6789"
        assert transformer.transforms["ssn_mask"]("123456789") == "***-**-6789"

    def test_phone_normalize(self, transformer):
        result = transformer.transforms["phone_normalize"]("(555) 123-4567")
        assert result == "+15551234567"

        result2 = transformer.transforms["phone_normalize"]("555.123.4567")
        assert result2 == "+15551234567"

    def test_status_code_map(self, transformer):
        assert transformer.transforms["status_code_map"]("A") == "active"
        assert transformer.transforms["status_code_map"]("I") == "inactive"
        assert transformer.transforms["status_code_map"]("active") == "active"

    def test_currency_to_cents(self, transformer):
        assert transformer.transforms["currency_to_cents"](10.50) == 1050
        assert transformer.transforms["currency_to_cents"](0) == 0

    def test_boolean_from_flag(self, transformer):
        assert transformer.transforms["boolean_from_flag"]("Y") is True
        assert transformer.transforms["boolean_from_flag"]("N") is False
        assert transformer.transforms["boolean_from_flag"]("1") is True

    def test_null_coalesce(self, transformer):
        assert transformer.transforms["null_coalesce"](None) == ""
        assert transformer.transforms["null_coalesce"]("value") == "value"

    def test_db2_date_to_iso(self, transformer):
        result = transformer.transforms["db2_date_to_iso"]("2026-01-15")
        assert result == "2026-01-15"
        result2 = transformer.transforms["db2_date_to_iso"]("01/15/2026")
        assert result2 == "2026-01-15"

    def test_transform_dataframe(self, transformer, sample_source_df):
        """Transform a full DataFrame and verify lineage entries are produced."""
        transformer.column_transforms = {
            "first_name": "name_title_case",
            "last_name": "name_title_case",
            "ssn": "ssn_mask",
        }
        result_df, lineage = transformer.transform_dataframe(sample_source_df)

        assert len(result_df) == len(sample_source_df)
        assert result_df["first_name"].iloc[0] == "John"  # Was "JOHN"
        assert "6789" in result_df["ssn"].iloc[0]  # Masked
        assert len(lineage) > 0
