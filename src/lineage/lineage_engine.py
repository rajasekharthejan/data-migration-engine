"""Metadata lineage tracking engine.

Tracks the complete journey of every field from source (DB2) to target (FAST),
including which transformations were applied, when, and by which ETL version.
This enables auditors and data stewards to answer: "Where did this value come from?"
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import structlog

logger = structlog.get_logger(__name__)


class LineageEntry:
    """A single lineage record tracking one field's transformation."""

    __slots__ = (
        "lineage_id",
        "job_id",
        "record_id",
        "source_table",
        "source_column",
        "source_value",
        "target_table",
        "target_column",
        "target_value",
        "transformation",
        "etl_commit_id",
        "timestamp",
    )

    def __init__(
        self,
        job_id: str,
        record_id: str,
        source_table: str,
        source_column: str,
        source_value: Any,
        target_table: str,
        target_column: str,
        target_value: Any,
        transformation: str,
        etl_commit_id: str = "",
    ):
        self.lineage_id = str(uuid.uuid4())
        self.job_id = job_id
        self.record_id = record_id
        self.source_table = source_table
        self.source_column = source_column
        self.source_value = source_value
        self.target_table = target_table
        self.target_column = target_column
        self.target_value = target_value
        self.transformation = transformation
        self.etl_commit_id = etl_commit_id
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict[str, Any]:
        return {
            "lineage_id": self.lineage_id,
            "job_id": self.job_id,
            "record_id": self.record_id,
            "source_table": self.source_table,
            "source_column": self.source_column,
            "source_value": str(self.source_value),
            "target_table": self.target_table,
            "target_column": self.target_column,
            "target_value": str(self.target_value),
            "transformation": self.transformation,
            "etl_commit_id": self.etl_commit_id,
            "timestamp": self.timestamp,
        }


class LineageEngine:
    """Track and query data lineage across the migration pipeline.

    Architecture:
        Each migration job generates lineage entries per-field per-record.
        Entries are buffered in memory, then flushed to the metadata store
        (PostgreSQL) in batches for performance.

    Query capabilities:
        - Forward lineage: "Where did source field X end up?"
        - Backward lineage: "Where did target field Y come from?"
        - Impact analysis: "Which target fields are affected if source column Z changes?"
        - Audit trail: "Show all transformations for record R in job J"
    """

    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size
        self._buffer: list[LineageEntry] = []
        self._flushed_count = 0

    def track(
        self,
        job_id: str,
        record_id: str,
        source_table: str,
        source_column: str,
        source_value: Any,
        target_table: str,
        target_column: str,
        target_value: Any,
        transformation: str,
        etl_commit_id: str = "",
    ) -> LineageEntry:
        """Record a single field-level lineage entry."""
        entry = LineageEntry(
            job_id=job_id,
            record_id=record_id,
            source_table=source_table,
            source_column=source_column,
            source_value=source_value,
            target_table=target_table,
            target_column=target_column,
            target_value=target_value,
            transformation=transformation,
            etl_commit_id=etl_commit_id,
        )
        self._buffer.append(entry)

        if len(self._buffer) >= self.batch_size:
            self.flush()

        return entry

    def track_batch(
        self,
        job_id: str,
        record_id: str,
        source_table: str,
        target_table: str,
        field_mappings: list[dict[str, Any]],
        etl_commit_id: str = "",
    ) -> list[LineageEntry]:
        """Track lineage for multiple fields of a single record at once.

        Args:
            field_mappings: List of dicts with keys:
                source_column, source_value, target_column, target_value, transformation
        """
        entries = []
        for mapping in field_mappings:
            entry = self.track(
                job_id=job_id,
                record_id=record_id,
                source_table=source_table,
                source_column=mapping["source_column"],
                source_value=mapping["source_value"],
                target_table=target_table,
                target_column=mapping["target_column"],
                target_value=mapping["target_value"],
                transformation=mapping.get("transformation", "direct_copy"),
                etl_commit_id=etl_commit_id,
            )
            entries.append(entry)
        return entries

    def flush(self) -> int:
        """Flush buffered lineage entries to the metadata store.

        In production, this writes to PostgreSQL via SQLAlchemy.
        Returns the number of entries flushed.
        """
        count = len(self._buffer)
        if count == 0:
            return 0

        logger.info("lineage_flush", entries=count)
        # In production: bulk insert to lineage_records table
        self._flushed_count += count
        self._buffer.clear()
        return count

    def query_forward(
        self,
        source_table: str,
        source_column: str,
        job_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Forward lineage: find all target fields derived from a source field.

        "If I change column X in DB2, what breaks in FAST?"
        """
        results = []
        all_entries = self._get_all_entries()

        for entry in all_entries:
            if entry.source_table == source_table and entry.source_column == source_column:
                if job_id is None or entry.job_id == job_id:
                    results.append(entry.to_dict())

        logger.info(
            "forward_lineage_query",
            source=f"{source_table}.{source_column}",
            results=len(results),
        )
        return results

    def query_backward(
        self,
        target_table: str,
        target_column: str,
        job_id: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Backward lineage: find the source of a target field.

        "Where did this value in FAST come from?"
        """
        results = []
        all_entries = self._get_all_entries()

        for entry in all_entries:
            if entry.target_table == target_table and entry.target_column == target_column:
                if job_id is None or entry.job_id == job_id:
                    results.append(entry.to_dict())

        logger.info(
            "backward_lineage_query",
            target=f"{target_table}.{target_column}",
            results=len(results),
        )
        return results

    def query_record(self, record_id: str, job_id: Optional[str] = None) -> list[dict[str, Any]]:
        """Get complete lineage for a single record across all fields."""
        results = []
        all_entries = self._get_all_entries()

        for entry in all_entries:
            if entry.record_id == record_id:
                if job_id is None or entry.job_id == job_id:
                    results.append(entry.to_dict())

        return results

    def impact_analysis(self, source_table: str, source_column: str) -> dict[str, Any]:
        """Analyze the downstream impact of changing a source column.

        Returns summary of all target tables/columns affected, with counts.
        """
        forward = self.query_forward(source_table, source_column)

        targets: dict[str, set[str]] = {}
        for entry in forward:
            table = entry["target_table"]
            col = entry["target_column"]
            if table not in targets:
                targets[table] = set()
            targets[table].add(col)

        impact = {
            "source": f"{source_table}.{source_column}",
            "affected_tables": len(targets),
            "affected_columns": sum(len(cols) for cols in targets.values()),
            "details": {table: sorted(cols) for table, cols in targets.items()},
        }

        logger.info("impact_analysis", source=impact["source"], tables=impact["affected_tables"])
        return impact

    def get_stats(self) -> dict[str, Any]:
        """Return lineage statistics."""
        return {
            "buffered": len(self._buffer),
            "flushed": self._flushed_count,
            "total_tracked": self._flushed_count + len(self._buffer),
        }

    def _get_all_entries(self) -> list[LineageEntry]:
        """Get all entries (buffered + would query DB in production)."""
        # In production, this queries PostgreSQL
        # For now, return buffer contents
        return list(self._buffer)
