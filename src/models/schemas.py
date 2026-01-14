"""Pydantic schemas for API requests/responses and data validation."""

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    PENDING = "pending"
    EXTRACTING = "extracting"
    TRANSFORMING = "transforming"
    LOADING = "loading"
    RECONCILING = "reconciling"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class BatchStatus(str, Enum):
    PENDING = "pending"
    EXTRACTING = "extracting"
    EXTRACTED = "extracted"
    TRANSFORMING = "transforming"
    TRANSFORMED = "transformed"
    LOADING = "loading"
    LOADED = "loaded"
    RECONCILING = "reconciling"
    COMPLETED = "completed"
    FAILED = "failed"


class ReconStatus(str, Enum):
    MATCHED = "matched"
    MISMATCHED = "mismatched"
    MISSING_IN_TARGET = "missing_in_target"
    MISSING_IN_SOURCE = "missing_in_source"
    TYPE_MISMATCH = "type_mismatch"


class Severity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


# ── Migration Job Schemas ──────────────────────────────────────


class MigrationJobCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None
    entity_type: str = Field(..., min_length=1, max_length=100)
    contract_id: Optional[str] = None
    config: Optional[dict[str, Any]] = None


class MigrationJobResponse(BaseModel):
    id: uuid.UUID
    name: str
    description: Optional[str]
    source_system: str
    target_system: str
    status: JobStatus
    entity_type: str
    contract_id: Optional[str]
    total_records: int
    processed_records: int
    failed_records: int
    error_message: Optional[str]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    created_at: datetime

    class Config:
        from_attributes = True


class MigrationJobProgress(BaseModel):
    job_id: uuid.UUID
    status: JobStatus
    total_records: int
    processed_records: int
    failed_records: int
    progress_pct: float
    elapsed_seconds: Optional[float]
    estimated_remaining_seconds: Optional[float]
    batches_total: int
    batches_completed: int


# ── Batch Schemas ──────────────────────────────────────────────


class BatchResponse(BaseModel):
    id: uuid.UUID
    job_id: uuid.UUID
    batch_number: int
    status: BatchStatus
    record_count: int
    checksum_source: Optional[str]
    checksum_target: Optional[str]
    extract_duration_ms: Optional[int]
    transform_duration_ms: Optional[int]
    load_duration_ms: Optional[int]

    class Config:
        from_attributes = True


# ── Reconciliation Schemas ─────────────────────────────────────


class ReconciliationSummary(BaseModel):
    job_id: uuid.UUID
    total_records: int
    matched: int
    mismatched: int
    missing_in_target: int
    missing_in_source: int
    match_rate_pct: float
    critical_issues: int
    warnings: int


class ReconciliationDetail(BaseModel):
    id: uuid.UUID
    source_record_id: str
    target_record_id: Optional[str]
    entity_type: str
    status: ReconStatus
    mismatched_fields: Optional[dict]
    severity: Severity
    resolved: bool

    class Config:
        from_attributes = True


# ── Lineage Schemas ────────────────────────────────────────────


class LineageQuery(BaseModel):
    record_id: Optional[str] = None
    entity_type: Optional[str] = None
    field_name: Optional[str] = None
    etl_commit_id: Optional[str] = None
    job_id: Optional[uuid.UUID] = None


class LineageResponse(BaseModel):
    record_id: str
    entity_type: str
    field_name: str
    source_table: str
    source_column: str
    source_value: Optional[str]
    transformed_value: Optional[str]
    transformation_applied: Optional[str]
    validation_passed: bool
    etl_commit_id: str
    created_at: datetime

    class Config:
        from_attributes = True


class LineageMap(BaseModel):
    """Structured data map for a complete record's lineage."""

    record_id: str
    entity_type: str
    etl_commit_id: str
    job_id: uuid.UUID
    fields: list[LineageResponse]
    total_fields: int
    validation_pass_rate: float


# ── Entity Mapping Schemas ─────────────────────────────────────


class EntityMappingCreate(BaseModel):
    entity_type: str
    source_table: str
    source_column: str
    source_data_type: Optional[str] = None
    target_field: str
    target_data_type: Optional[str] = None
    transformation_rule: Optional[str] = None
    validation_rule: Optional[str] = None
    is_required: bool = False
    is_key_field: bool = False
    default_value: Optional[str] = None
    description: Optional[str] = None


class EntityMappingResponse(BaseModel):
    id: uuid.UUID
    entity_type: str
    source_table: str
    source_column: str
    target_field: str
    transformation_rule: Optional[str]
    validation_rule: Optional[str]
    is_required: bool
    is_key_field: bool

    class Config:
        from_attributes = True


# ── Chatbot Schemas ────────────────────────────────────────────


class ChatQuery(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)
    context: Optional[dict[str, Any]] = None


class ChatResponse(BaseModel):
    answer: str
    sources: list[dict[str, Any]] = []
    confidence: float = 0.0
    query_type: str = "general"  # "lineage", "reconciliation", "status", "general"
