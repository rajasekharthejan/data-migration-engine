"""Parallel extraction coordinator using ProcessPoolExecutor."""

import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

import structlog

from src.extractors.db2_extractor import DB2Extractor, ExtractionResult

logger = structlog.get_logger(__name__)


@dataclass
class ParallelExtractionResult:
    """Aggregated result from parallel extraction."""

    results: list[ExtractionResult]
    total_records: int
    total_duration_ms: int
    combined_checksum: str
    failed_batches: list[int]


class ParallelExtractor:
    """Coordinate parallel extraction from DB2 using multiple workers.

    Each worker gets its own DB2 connection. Batches are distributed
    round-robin across workers. Failed batches are retried up to max_retries.

    Architecture:
        ┌─────────────────────────────────────────────┐
        │           ParallelExtractor                  │
        │                                              │
        │   ┌──────────┐ ┌──────────┐ ┌──────────┐   │
        │   │ Worker 1  │ │ Worker 2  │ │ Worker N  │  │
        │   │ DB2 conn  │ │ DB2 conn  │ │ DB2 conn  │  │
        │   │ Batch 0,3 │ │ Batch 1,4 │ │ Batch 2,5 │  │
        │   └─────┬─────┘ └─────┬─────┘ └─────┬─────┘  │
        │         │             │             │         │
        │         └─────────────┼─────────────┘         │
        │                       ▼                       │
        │              S3 Staging Bucket                │
        └─────────────────────────────────────────────┘
    """

    def __init__(
        self,
        connection_string: str,
        schema: str = "INSURANCE",
        batch_size: int = 10_000,
        max_workers: int = 8,
        max_retries: int = 3,
    ):
        self.connection_string = connection_string
        self.schema = schema
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.max_retries = max_retries

    def extract_table(
        self,
        table: str,
        columns: list[str],
        key_column: str = "POLICY_ID",
        where_clause: Optional[str] = None,
    ) -> ParallelExtractionResult:
        """Extract an entire table using parallel workers."""
        start_time = time.monotonic()

        # Get total count to determine batch count
        extractor = DB2Extractor(
            self.connection_string, self.schema, self.batch_size, self.max_retries
        )
        extractor.connect()
        total_count = extractor.get_table_count(table, where_clause)
        extractor.disconnect()

        num_batches = (total_count + self.batch_size - 1) // self.batch_size

        logger.info(
            "parallel_extraction_started",
            table=table,
            total_records=total_count,
            num_batches=num_batches,
            max_workers=self.max_workers,
        )

        results: list[ExtractionResult] = []
        failed_batches: list[int] = []

        # Use min(workers, batches) to avoid idle workers
        effective_workers = min(self.max_workers, num_batches)

        with ProcessPoolExecutor(max_workers=effective_workers) as executor:
            future_to_batch = {}
            for batch_num in range(num_batches):
                future = executor.submit(
                    _extract_batch_worker,
                    self.connection_string,
                    self.schema,
                    self.batch_size,
                    self.max_retries,
                    table,
                    columns,
                    batch_num,
                    key_column,
                    where_clause,
                )
                future_to_batch[future] = batch_num

            for future in as_completed(future_to_batch):
                batch_num = future_to_batch[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(
                        "batch_complete",
                        batch=batch_num,
                        records=result.record_count,
                        progress=f"{len(results)}/{num_batches}",
                    )
                except Exception as e:
                    logger.error("batch_failed", batch=batch_num, error=str(e))
                    failed_batches.append(batch_num)

        # Sort results by batch number for deterministic ordering
        results.sort(key=lambda r: r.batch_number)

        # Compute combined checksum across all batches
        import xxhash

        combined = xxhash.xxh64()
        for r in results:
            combined.update(r.checksum.encode())
        combined_checksum = combined.hexdigest()

        total_records = sum(r.record_count for r in results)
        duration_ms = int((time.monotonic() - start_time) * 1000)

        logger.info(
            "parallel_extraction_complete",
            table=table,
            total_records=total_records,
            failed_batches=len(failed_batches),
            duration_ms=duration_ms,
            checksum=combined_checksum[:12],
        )

        return ParallelExtractionResult(
            results=results,
            total_records=total_records,
            total_duration_ms=duration_ms,
            combined_checksum=combined_checksum,
            failed_batches=failed_batches,
        )


def _extract_batch_worker(
    connection_string: str,
    schema: str,
    batch_size: int,
    max_retries: int,
    table: str,
    columns: list[str],
    batch_number: int,
    key_column: str,
    where_clause: Optional[str],
) -> ExtractionResult:
    """Worker function for parallel extraction (runs in separate process)."""
    extractor = DB2Extractor(connection_string, schema, batch_size, max_retries)
    extractor.connect()
    try:
        return extractor.extract_batch(table, columns, batch_number, key_column, where_clause)
    finally:
        extractor.disconnect()
