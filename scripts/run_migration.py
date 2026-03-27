#!/usr/bin/env python3
"""CLI script to run a data migration job.

Usage:
    python scripts/run_migration.py --entity policies --table POLICY_MASTER
    python scripts/run_migration.py --entity claims --table CLAIMS_MASTER --dry-run
    python scripts/run_migration.py --entity policies --table POLICY_MASTER --auto-promote
"""

import argparse
import json
import sys

sys.path.insert(0, ".")

from src.extractors.db2_extractor import DB2Extractor
from src.lineage.lineage_engine import LineageEngine
from src.loaders.fast_loader import FASTLoader
from src.loaders.s3_stager import S3Stager
from src.pipeline.orchestrator import MigrationOrchestrator
from src.reconciliation.reconciler import Reconciler
from src.reconciliation.rollback_manager import RollbackManager
from src.transformers.business_rules import BusinessRuleValidator
from src.transformers.field_transformer import FieldTransformer
from src.utils.logging import setup_logging


def main():
    parser = argparse.ArgumentParser(description="Run a data migration job")
    parser.add_argument("--entity", required=True, help="Entity type (policies, claims, premiums)")
    parser.add_argument("--table", required=True, help="DB2 source table name")
    parser.add_argument("--batch-size", type=int, default=10000, help="Records per batch")
    parser.add_argument("--dry-run", action="store_true", help="Validate only, don't load to FAST")
    parser.add_argument("--auto-promote", action="store_true", help="Auto-promote on success")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    args = parser.parse_args()

    setup_logging(level=args.log_level)

    # Initialize components
    extractor = DB2Extractor(
        host="localhost", port=50000, database="INSURDB",
        username="db2admin", password="", use_mock=True,
    )
    s3_stager = S3Stager(bucket="migration-staging")
    fast_loader = FASTLoader(base_url="http://localhost:8080", api_key="dev-key")
    field_transformer = FieldTransformer()
    business_validator = BusinessRuleValidator()
    reconciler = Reconciler(key_columns=["policy_id"])
    rollback_manager = RollbackManager(fast_loader=fast_loader, s3_stager=s3_stager)
    lineage_engine = LineageEngine()

    orchestrator = MigrationOrchestrator(
        extractor=extractor,
        s3_stager=s3_stager,
        glue_transformer=None,
        field_transformer=field_transformer,
        business_validator=business_validator,
        fast_loader=fast_loader,
        reconciler=reconciler,
        rollback_manager=rollback_manager,
        lineage_engine=lineage_engine,
    )

    report = orchestrator.run_migration(
        entity_type=args.entity,
        source_table=args.table,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        auto_promote=args.auto_promote,
    )

    # Remove non-serializable items for output
    def clean(obj):
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        if hasattr(obj, "__dataframe__"):
            return "<DataFrame>"
        return str(obj)

    print(json.dumps(report, indent=2, default=clean))


if __name__ == "__main__":
    main()
