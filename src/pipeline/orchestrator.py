"""Main migration pipeline orchestrator.

Coordinates the end-to-end migration flow:
    DB2 Extract → S3 Stage → Glue Transform → FAST Load → Reconcile → Promote/Rollback

This is the single entry point that runs a complete migration job.
"""

import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

logger = structlog.get_logger(__name__)


class MigrationOrchestrator:
    """Orchestrate a complete data migration job across all pipeline stages.

    Pipeline stages (in order):
        1. EXTRACT   — Pull data from DB2 mainframe in parallel batches
        2. STAGE     — Upload extracted Parquet files to S3 staging bucket
        3. TRANSFORM — Run AWS Glue ETL job (Spark) for field transformations
        4. VALIDATE  — Apply business rules to transformed data
        5. LOAD      — Push validated data into FAST (green deployment slot)
        6. RECONCILE — Compare source vs target record-by-record
        7. PROMOTE   — If reconciliation passes, promote green → production
        8. ROLLBACK  — If anything fails, discard green and log the failure

    Each stage updates the metadata store with status, timing, and error details.
    """

    def __init__(
        self,
        extractor: Any,
        s3_stager: Any,
        glue_transformer: Any,
        field_transformer: Any,
        business_validator: Any,
        fast_loader: Any,
        reconciler: Any,
        rollback_manager: Any,
        lineage_engine: Any,
    ):
        self.extractor = extractor
        self.s3_stager = s3_stager
        self.glue_transformer = glue_transformer
        self.field_transformer = field_transformer
        self.business_validator = business_validator
        self.fast_loader = fast_loader
        self.reconciler = reconciler
        self.rollback_manager = rollback_manager
        self.lineage_engine = lineage_engine

    def run_migration(
        self,
        entity_type: str,
        source_table: str,
        batch_size: int = 10000,
        dry_run: bool = False,
        auto_promote: bool = False,
    ) -> dict[str, Any]:
        """Execute a complete migration job.

        Args:
            entity_type: Type of data being migrated (policy, claim, premium, etc.)
            source_table: DB2 source table name
            batch_size: Records per extraction batch
            dry_run: If True, validate everything but don't load to FAST
            auto_promote: If True, auto-promote on successful reconciliation

        Returns:
            Complete job report with status, timing, and details per stage
        """
        job_id = f"MIG-{datetime.now(timezone.utc).strftime('%Y%m%d')}-{str(uuid.uuid4())[:8]}"
        start_time = time.monotonic()

        logger.info(
            "migration_started",
            job_id=job_id,
            entity_type=entity_type,
            source_table=source_table,
            dry_run=dry_run,
        )

        report: dict[str, Any] = {
            "job_id": job_id,
            "entity_type": entity_type,
            "source_table": source_table,
            "dry_run": dry_run,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "stages": {},
        }

        try:
            # Stage 1: EXTRACT from DB2
            report["stages"]["extract"] = self._run_extraction(
                job_id, source_table, entity_type, batch_size
            )
            if not report["stages"]["extract"]["success"]:
                raise RuntimeError("Extraction failed")

            extracted_df = report["stages"]["extract"]["dataframe"]

            # Stage 2: STAGE to S3
            report["stages"]["stage"] = self._run_staging(
                job_id, entity_type, extracted_df
            )

            # Stage 3: TRANSFORM via field transformer (local)
            report["stages"]["transform"] = self._run_transformation(
                job_id, entity_type, extracted_df
            )
            if not report["stages"]["transform"]["success"]:
                raise RuntimeError("Transformation failed")

            transformed_df = report["stages"]["transform"]["dataframe"]

            # Stage 4: VALIDATE business rules
            report["stages"]["validate"] = self._run_validation(
                job_id, entity_type, transformed_df
            )
            if not report["stages"]["validate"]["acceptable"]:
                raise RuntimeError(
                    f"Validation failed: pass_rate={report['stages']['validate']['pass_rate']}"
                )

            # Stage 5: LOAD to FAST
            if not dry_run:
                report["stages"]["load"] = self._run_loading(
                    job_id, entity_type, transformed_df
                )
                if report["stages"]["load"]["errors"]:
                    raise RuntimeError("Loading had errors")

                # Stage 6: RECONCILE source vs target
                report["stages"]["reconcile"] = self._run_reconciliation(
                    job_id, extracted_df, transformed_df
                )

                # Stage 7: PROMOTE or flag for review
                if report["stages"]["reconcile"]["acceptable"]:
                    if auto_promote:
                        report["stages"]["promote"] = self._run_promotion(job_id)
                    else:
                        report["stages"]["promote"] = {
                            "action": "awaiting_manual_promotion",
                            "message": "Reconciliation passed. Run promote_deployment() to go live.",
                        }
                else:
                    report["stages"]["promote"] = {
                        "action": "blocked",
                        "reason": "Reconciliation did not meet acceptance criteria",
                    }
            else:
                report["stages"]["load"] = {"skipped": True, "reason": "dry_run=True"}

            report["status"] = "completed"

        except Exception as e:
            logger.error("migration_failed", job_id=job_id, error=str(e))
            report["status"] = "failed"
            report["error"] = str(e)

            # Rollback if we loaded anything
            if "load" in report["stages"] and not report["stages"]["load"].get("skipped"):
                report["stages"]["rollback"] = self.rollback_manager.rollback_job(
                    job_id=job_id,
                    reason=str(e),
                )

        # Flush lineage
        self.lineage_engine.flush()

        elapsed_ms = int((time.monotonic() - start_time) * 1000)
        report["duration_ms"] = elapsed_ms
        report["completed_at"] = datetime.now(timezone.utc).isoformat()

        logger.info(
            "migration_completed",
            job_id=job_id,
            status=report["status"],
            duration_ms=elapsed_ms,
        )

        return report

    def _run_extraction(
        self,
        job_id: str,
        source_table: str,
        entity_type: str,
        batch_size: int,
    ) -> dict[str, Any]:
        """Stage 1: Extract data from DB2."""
        logger.info("stage_extract_start", job_id=job_id)
        start = time.monotonic()

        try:
            import pandas as pd

            all_records = []
            for batch_result in self.extractor.extract_all_batches(
                table_name=source_table,
                batch_size=batch_size,
            ):
                all_records.extend(batch_result.records)

            df = pd.DataFrame(all_records) if all_records else pd.DataFrame()
            elapsed = int((time.monotonic() - start) * 1000)

            return {
                "success": True,
                "records": len(df),
                "duration_ms": elapsed,
                "dataframe": df,
            }
        except Exception as e:
            return {"success": False, "error": str(e), "dataframe": None}

    def _run_staging(
        self,
        job_id: str,
        entity_type: str,
        df: Any,
    ) -> dict[str, Any]:
        """Stage 2: Upload extracted data to S3."""
        logger.info("stage_s3_start", job_id=job_id)
        start = time.monotonic()

        try:
            s3_path = self.s3_stager.upload_dataframe(
                df=df,
                job_id=job_id,
                entity_type=entity_type,
                batch_number=1,
                stage="extract",
            )
            elapsed = int((time.monotonic() - start) * 1000)
            return {"success": True, "s3_path": s3_path, "duration_ms": elapsed}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _run_transformation(
        self,
        job_id: str,
        entity_type: str,
        df: Any,
    ) -> dict[str, Any]:
        """Stage 3: Apply field transformations."""
        logger.info("stage_transform_start", job_id=job_id)
        start = time.monotonic()

        try:
            transformed_df, lineage_entries = self.field_transformer.transform_dataframe(df)
            elapsed = int((time.monotonic() - start) * 1000)

            # Track lineage
            for entry in lineage_entries:
                self.lineage_engine.track(
                    job_id=job_id,
                    record_id=entry.get("record_id", ""),
                    source_table=entry.get("source_table", f"db2_{entity_type}"),
                    source_column=entry.get("source_column", ""),
                    source_value=entry.get("source_value", ""),
                    target_table=entry.get("target_table", f"fast_{entity_type}"),
                    target_column=entry.get("target_column", ""),
                    target_value=entry.get("target_value", ""),
                    transformation=entry.get("transformation", ""),
                )

            return {
                "success": True,
                "records": len(transformed_df),
                "lineage_entries": len(lineage_entries),
                "duration_ms": elapsed,
                "dataframe": transformed_df,
            }
        except Exception as e:
            return {"success": False, "error": str(e), "dataframe": None}

    def _run_validation(
        self,
        job_id: str,
        entity_type: str,
        df: Any,
    ) -> dict[str, Any]:
        """Stage 4: Validate against business rules."""
        logger.info("stage_validate_start", job_id=job_id)
        start = time.monotonic()

        records = df.to_dict(orient="records")
        validation_report = self.business_validator.validate_batch(records)
        elapsed = int((time.monotonic() - start) * 1000)

        return {
            "acceptable": validation_report.is_acceptable,
            "total": validation_report.total_records,
            "passed": validation_report.passed,
            "failed": validation_report.failed,
            "pass_rate": validation_report.pass_rate,
            "critical_failures": validation_report.critical_failures,
            "duration_ms": elapsed,
        }

    def _run_loading(
        self,
        job_id: str,
        entity_type: str,
        df: Any,
    ) -> dict[str, Any]:
        """Stage 5: Load into FAST system."""
        logger.info("stage_load_start", job_id=job_id)

        result = self.fast_loader.load_dataframe(
            entity_type=entity_type,
            df=df,
            deployment="green",
        )
        return {
            "loaded": result["total_loaded"],
            "errors": result.get("errors", []),
            "duration_ms": result["duration_ms"],
        }

    def _run_reconciliation(
        self,
        job_id: str,
        source_df: Any,
        target_df: Any,
    ) -> dict[str, Any]:
        """Stage 6: Reconcile source vs target."""
        logger.info("stage_reconcile_start", job_id=job_id)
        start = time.monotonic()

        report = self.reconciler.reconcile(source_df, target_df)
        elapsed = int((time.monotonic() - start) * 1000)

        return {
            "acceptable": report.is_acceptable,
            "match_rate": report.match_rate,
            "matched": report.matched,
            "mismatched": report.mismatched,
            "missing_in_target": report.missing_in_target,
            "missing_in_source": report.missing_in_source,
            "duration_ms": elapsed,
        }

    def _run_promotion(self, job_id: str) -> dict[str, Any]:
        """Stage 7: Promote green deployment to production."""
        logger.info("stage_promote_start", job_id=job_id)

        result = self.fast_loader.promote_deployment("green")
        return {"action": "promoted", "result": result}
