"""SQLAlchemy models for the migration metadata store."""

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class MigrationJob(Base):
    """Top-level migration job tracking a full contract migration run."""

    __tablename__ = "migration_jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    source_system: Mapped[str] = mapped_column(String(50), default="DB2")
    target_system: Mapped[str] = mapped_column(String(50), default="FAST")
    status: Mapped[str] = mapped_column(
        Enum("pending", "extracting", "transforming", "loading", "reconciling", "completed", "failed", "rolled_back",
             name="job_status"),
        default="pending",
    )
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g., "policy", "claim", "premium"
    contract_id: Mapped[Optional[str]] = mapped_column(String(50))  # Prudential contract identifier
    total_records: Mapped[int] = mapped_column(Integer, default=0)
    processed_records: Mapped[int] = mapped_column(Integer, default=0)
    failed_records: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    config: Mapped[Optional[dict]] = mapped_column(JSONB, default=dict)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc)
    )

    batches: Mapped[list["MigrationBatch"]] = relationship(back_populates="job", cascade="all, delete-orphan")
    reconciliation_results: Mapped[list["ReconciliationResult"]] = relationship(
        back_populates="job", cascade="all, delete-orphan"
    )
    lineage_records: Mapped[list["LineageRecord"]] = relationship(back_populates="job", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_migration_jobs_status", "status"),
        Index("ix_migration_jobs_entity_type", "entity_type"),
        Index("ix_migration_jobs_contract_id", "contract_id"),
    )


class MigrationBatch(Base):
    """Individual batch within a migration job (e.g., records 0-10000)."""

    __tablename__ = "migration_batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("migration_jobs.id"), nullable=False)
    batch_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        Enum("pending", "extracting", "extracted", "transforming", "transformed", "loading", "loaded",
             "reconciling", "completed", "failed", name="batch_status"),
        default="pending",
    )
    record_count: Mapped[int] = mapped_column(Integer, default=0)
    s3_extract_path: Mapped[Optional[str]] = mapped_column(String(500))
    s3_transform_path: Mapped[Optional[str]] = mapped_column(String(500))
    checksum_source: Mapped[Optional[str]] = mapped_column(String(64))  # xxhash of source data
    checksum_target: Mapped[Optional[str]] = mapped_column(String(64))  # xxhash of loaded data
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    extract_duration_ms: Mapped[Optional[int]] = mapped_column(Integer)
    transform_duration_ms: Mapped[Optional[int]] = mapped_column(Integer)
    load_duration_ms: Mapped[Optional[int]] = mapped_column(Integer)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    job: Mapped["MigrationJob"] = relationship(back_populates="batches")

    __table_args__ = (
        UniqueConstraint("job_id", "batch_number", name="uq_batch_job_number"),
        Index("ix_migration_batches_job_status", "job_id", "status"),
    )


class EntityMapping(Base):
    """Mapping between source (DB2) and target (FAST) entity schemas."""

    __tablename__ = "entity_mappings"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    source_table: Mapped[str] = mapped_column(String(255), nullable=False)
    source_column: Mapped[str] = mapped_column(String(255), nullable=False)
    source_data_type: Mapped[str] = mapped_column(String(50))
    target_field: Mapped[str] = mapped_column(String(255), nullable=False)
    target_data_type: Mapped[str] = mapped_column(String(50))
    transformation_rule: Mapped[Optional[str]] = mapped_column(Text)  # Python expression or function ref
    validation_rule: Mapped[Optional[str]] = mapped_column(Text)  # Business rule for validation
    is_required: Mapped[bool] = mapped_column(Boolean, default=False)
    is_key_field: Mapped[bool] = mapped_column(Boolean, default=False)
    default_value: Mapped[Optional[str]] = mapped_column(String(500))
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        UniqueConstraint("entity_type", "source_table", "source_column", name="uq_entity_source_mapping"),
        Index("ix_entity_mappings_type", "entity_type"),
    )


class ReconciliationResult(Base):
    """Record-by-record reconciliation between source and target."""

    __tablename__ = "reconciliation_results"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("migration_jobs.id"), nullable=False)
    batch_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("migration_batches.id"))
    source_record_id: Mapped[str] = mapped_column(String(255), nullable=False)
    target_record_id: Mapped[Optional[str]] = mapped_column(String(255))
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    status: Mapped[str] = mapped_column(
        Enum("matched", "mismatched", "missing_in_target", "missing_in_source", "type_mismatch",
             name="recon_status"),
        nullable=False,
    )
    mismatched_fields: Mapped[Optional[dict]] = mapped_column(JSONB)
    source_values: Mapped[Optional[dict]] = mapped_column(JSONB)
    target_values: Mapped[Optional[dict]] = mapped_column(JSONB)
    severity: Mapped[str] = mapped_column(
        Enum("info", "warning", "critical", name="recon_severity"),
        default="info",
    )
    resolved: Mapped[bool] = mapped_column(Boolean, default=False)
    resolution_notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    job: Mapped["MigrationJob"] = relationship(back_populates="reconciliation_results")

    __table_args__ = (
        Index("ix_recon_results_job_status", "job_id", "status"),
        Index("ix_recon_results_source_id", "source_record_id"),
        Index("ix_recon_results_severity", "severity"),
    )


class LineageRecord(Base):
    """Metadata lineage tracking: where did each field come from, how was it transformed?"""

    __tablename__ = "lineage_records"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("migration_jobs.id"), nullable=False)
    batch_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("migration_batches.id"))
    entity_type: Mapped[str] = mapped_column(String(100), nullable=False)
    record_id: Mapped[str] = mapped_column(String(255), nullable=False)
    field_name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_table: Mapped[str] = mapped_column(String(255), nullable=False)
    source_column: Mapped[str] = mapped_column(String(255), nullable=False)
    source_value: Mapped[Optional[str]] = mapped_column(Text)
    transformed_value: Mapped[Optional[str]] = mapped_column(Text)
    transformation_applied: Mapped[Optional[str]] = mapped_column(Text)  # description of transform
    validation_passed: Mapped[bool] = mapped_column(Boolean, default=True)
    validation_message: Mapped[Optional[str]] = mapped_column(Text)
    etl_commit_id: Mapped[str] = mapped_column(String(64), nullable=False)  # links to git commit or run ID
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    job: Mapped["MigrationJob"] = relationship(back_populates="lineage_records")

    __table_args__ = (
        Index("ix_lineage_record_id", "record_id"),
        Index("ix_lineage_etl_commit", "etl_commit_id"),
        Index("ix_lineage_job_entity", "job_id", "entity_type"),
    )


class AuditEvent(Base):
    """Audit trail for all migration operations."""

    __tablename__ = "audit_events"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_type: Mapped[str] = mapped_column(String(50), nullable=False)
    entity_type: Mapped[Optional[str]] = mapped_column(String(100))
    job_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    batch_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    actor: Mapped[str] = mapped_column(String(100), default="system")
    action: Mapped[str] = mapped_column(String(255), nullable=False)
    details: Mapped[Optional[dict]] = mapped_column(JSONB)
    metadata: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    __table_args__ = (
        Index("ix_audit_events_type", "event_type"),
        Index("ix_audit_events_job", "job_id"),
        Index("ix_audit_events_created", "created_at"),
    )
