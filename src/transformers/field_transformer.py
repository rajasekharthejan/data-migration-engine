"""Field-level transformations: DB2 column formats → FAST system formats.

Handles data type conversions, format standardization, and default value injection.
Each transformation is logged for metadata lineage tracking.
"""

from datetime import datetime
from typing import Any, Callable, Optional

import pandas as pd
import structlog

logger = structlog.get_logger(__name__)


class FieldTransformer:
    """Transform individual fields from DB2 format to FAST format.

    DB2 mainframe data often has:
    - Fixed-width strings with trailing spaces
    - Packed decimal numbers
    - Date formats like YYYYMMDD or YYYY-MM-DD
    - EBCDIC encoding artifacts
    - NULL represented as empty strings or special values

    FAST system expects:
    - Trimmed strings
    - Standard decimal numbers
    - ISO 8601 dates
    - UTF-8 encoding
    - Proper NULL handling
    """

    def __init__(self) -> None:
        self._transforms: dict[str, Callable] = {}
        self._register_default_transforms()

    def _register_default_transforms(self) -> None:
        """Register standard DB2→FAST field transforms."""
        self.register("trim_string", self._trim_string)
        self.register("db2_date_to_iso", self._db2_date_to_iso)
        self.register("packed_decimal_to_float", self._packed_decimal_to_float)
        self.register("status_code_map", self._status_code_map)
        self.register("name_title_case", self._name_title_case)
        self.register("phone_normalize", self._phone_normalize)
        self.register("ssn_mask", self._ssn_mask)
        self.register("null_coalesce", self._null_coalesce)
        self.register("currency_to_cents", self._currency_to_cents)
        self.register("boolean_from_flag", self._boolean_from_flag)

    def register(self, name: str, fn: Callable) -> None:
        """Register a named transformation function."""
        self._transforms[name] = fn

    def apply(self, transform_name: str, value: Any, **kwargs: Any) -> tuple[Any, str]:
        """Apply a named transform to a value. Returns (result, description)."""
        if transform_name not in self._transforms:
            raise ValueError(f"Unknown transform: {transform_name}")
        fn = self._transforms[transform_name]
        result = fn(value, **kwargs)
        description = f"{transform_name}({repr(value)[:50]}) → {repr(result)[:50]}"
        return result, description

    def transform_dataframe(
        self,
        df: pd.DataFrame,
        column_transforms: dict[str, list[str]],
    ) -> tuple[pd.DataFrame, list[dict]]:
        """Apply transforms to specific columns of a DataFrame.

        Args:
            df: Input DataFrame
            column_transforms: {column_name: [transform_name, ...]}

        Returns:
            Tuple of (transformed_df, list of transform descriptions for lineage)
        """
        result = df.copy()
        lineage_entries: list[dict] = []

        for column, transforms in column_transforms.items():
            if column not in result.columns:
                logger.warning("column_not_found", column=column)
                continue

            for transform_name in transforms:
                original_values = result[column].copy()
                result[column] = result[column].apply(
                    lambda v, tn=transform_name: self.apply(tn, v)[0]
                )

                # Track what changed for lineage
                changed_mask = original_values != result[column]
                changed_count = changed_mask.sum()
                if changed_count > 0:
                    lineage_entries.append({
                        "column": column,
                        "transform": transform_name,
                        "records_changed": int(changed_count),
                        "total_records": len(result),
                    })
                    logger.info(
                        "transform_applied",
                        column=column,
                        transform=transform_name,
                        changed=changed_count,
                    )

        return result, lineage_entries

    # ── Built-in Transforms ───────────────────────────────────

    @staticmethod
    def _trim_string(value: Any) -> Optional[str]:
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return None
        return str(value).strip()

    @staticmethod
    def _db2_date_to_iso(value: Any) -> Optional[str]:
        """Convert DB2 date formats (YYYYMMDD, YYYY-MM-DD) to ISO 8601."""
        if value is None or str(value).strip() in ("", "0", "00000000"):
            return None
        s = str(value).strip().replace("-", "").replace("/", "")
        if len(s) == 8:
            try:
                dt = datetime.strptime(s, "%Y%m%d")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                return None
        return str(value)

    @staticmethod
    def _packed_decimal_to_float(value: Any) -> Optional[float]:
        """Convert DB2 packed decimal to Python float."""
        if value is None:
            return None
        try:
            return round(float(value), 4)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _status_code_map(value: Any) -> Optional[str]:
        """Map DB2 numeric status codes to FAST string statuses."""
        code_map = {
            "1": "ACTIVE", "A": "ACTIVE",
            "2": "LAPSED", "L": "LAPSED",
            "3": "PAID_UP", "P": "PAID_UP",
            "4": "SURRENDERED", "S": "SURRENDERED",
            "5": "MATURED", "M": "MATURED",
            "6": "DEATH_CLAIM", "D": "DEATH_CLAIM",
            "7": "CANCELLED", "C": "CANCELLED",
        }
        s = str(value).strip().upper()
        return code_map.get(s, s)  # Return mapped value or original

    @staticmethod
    def _name_title_case(value: Any) -> Optional[str]:
        if value is None:
            return None
        return str(value).strip().title()

    @staticmethod
    def _phone_normalize(value: Any) -> Optional[str]:
        """Normalize phone numbers to +1XXXXXXXXXX format."""
        if value is None:
            return None
        digits = "".join(c for c in str(value) if c.isdigit())
        if len(digits) == 10:
            return f"+1{digits}"
        if len(digits) == 11 and digits.startswith("1"):
            return f"+{digits}"
        return digits or None

    @staticmethod
    def _ssn_mask(value: Any) -> Optional[str]:
        """Mask SSN for FAST system: show only last 4 digits."""
        if value is None:
            return None
        digits = "".join(c for c in str(value) if c.isdigit())
        if len(digits) >= 4:
            return f"***-**-{digits[-4:]}"
        return None

    @staticmethod
    def _null_coalesce(value: Any, default: str = "") -> str:
        """Replace None/empty with a default value."""
        if value is None or (isinstance(value, str) and value.strip() == ""):
            return default
        return str(value)

    @staticmethod
    def _currency_to_cents(value: Any) -> Optional[int]:
        """Convert dollar amount to cents (integer) for precision."""
        if value is None:
            return None
        try:
            return int(round(float(value) * 100))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _boolean_from_flag(value: Any) -> Optional[bool]:
        """Convert DB2 Y/N/1/0 flags to boolean."""
        if value is None:
            return None
        s = str(value).strip().upper()
        if s in ("Y", "YES", "1", "T", "TRUE"):
            return True
        if s in ("N", "NO", "0", "F", "FALSE"):
            return False
        return None
