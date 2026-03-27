#!/usr/bin/env python3
"""Seed entity mappings for DB2 → FAST field transformations.

Run this once to populate the entity_mappings table with the field mapping
configuration for each entity type (policies, claims, premiums).
"""

import sys

sys.path.insert(0, ".")

POLICY_MAPPINGS = [
    {"source_column": "POLICY_ID", "target_column": "policy_id", "transformation_rule": "trim_string", "is_key": True},
    {"source_column": "FIRST_NM", "target_column": "first_name", "transformation_rule": "name_title_case"},
    {"source_column": "LAST_NM", "target_column": "last_name", "transformation_rule": "name_title_case"},
    {"source_column": "PREM_AMT", "target_column": "premium_amount", "transformation_rule": "packed_decimal_to_float"},
    {"source_column": "COV_AMT", "target_column": "coverage_amount", "transformation_rule": "packed_decimal_to_float"},
    {"source_column": "EFF_DT", "target_column": "effective_date", "transformation_rule": "db2_date_to_iso"},
    {"source_column": "STATUS_CD", "target_column": "status", "transformation_rule": "status_code_map"},
    {"source_column": "PROD_TYP", "target_column": "product_type", "transformation_rule": "trim_string"},
    {"source_column": "PHONE_NO", "target_column": "phone_number", "transformation_rule": "phone_normalize"},
    {"source_column": "SSN", "target_column": "ssn", "transformation_rule": "ssn_mask"},
]

CLAIM_MAPPINGS = [
    {"source_column": "CLAIM_ID", "target_column": "claim_id", "transformation_rule": "trim_string", "is_key": True},
    {"source_column": "POLICY_ID", "target_column": "policy_id", "transformation_rule": "trim_string"},
    {"source_column": "CLAIM_AMT", "target_column": "claim_amount", "transformation_rule": "packed_decimal_to_float"},
    {"source_column": "CLAIM_DT", "target_column": "claim_date", "transformation_rule": "db2_date_to_iso"},
    {"source_column": "STATUS_CD", "target_column": "status", "transformation_rule": "status_code_map"},
    {"source_column": "DIAG_CD", "target_column": "diagnosis_code", "transformation_rule": "trim_string"},
]


def main():
    print("Policy field mappings (DB2 → FAST):")
    print("-" * 60)
    for m in POLICY_MAPPINGS:
        key_flag = " [KEY]" if m.get("is_key") else ""
        print(f"  {m['source_column']:15s} → {m['target_column']:20s} ({m['transformation_rule']}){key_flag}")

    print()
    print("Claim field mappings (DB2 → FAST):")
    print("-" * 60)
    for m in CLAIM_MAPPINGS:
        key_flag = " [KEY]" if m.get("is_key") else ""
        print(f"  {m['source_column']:15s} → {m['target_column']:20s} ({m['transformation_rule']}){key_flag}")


if __name__ == "__main__":
    main()
