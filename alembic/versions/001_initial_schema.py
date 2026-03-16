"""Initial schema: migration jobs, batches, reconciliation, lineage, audit.

Revision ID: 001_initial
Revises:
Create Date: 2026-01-15 10:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Migration Jobs
    op.create_table(
        "migration_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("entity_type", sa.String(100), nullable=False),
        sa.Column("source_table", sa.String(255), nullable=False),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("total_records", sa.Integer),
        sa.Column("processed_records", sa.Integer, server_default="0"),
        sa.Column("error_message", sa.Text),
        sa.Column("config", postgresql.JSONB),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("completed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Migration Batches
    op.create_table(
        "migration_batches",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("migration_jobs.id")),
        sa.Column("batch_number", sa.Integer, nullable=False),
        sa.Column("status", sa.String(50), nullable=False, server_default="pending"),
        sa.Column("record_count", sa.Integer),
        sa.Column("source_checksum", sa.String(64)),
        sa.Column("target_checksum", sa.String(64)),
        sa.Column("s3_path", sa.String(500)),
        sa.Column("duration_ms", sa.Integer),
        sa.Column("error_message", sa.Text),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Entity Mappings
    op.create_table(
        "entity_mappings",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("entity_type", sa.String(100), nullable=False),
        sa.Column("source_table", sa.String(255), nullable=False),
        sa.Column("source_column", sa.String(255), nullable=False),
        sa.Column("target_table", sa.String(255), nullable=False),
        sa.Column("target_column", sa.String(255), nullable=False),
        sa.Column("transformation_rule", sa.String(255)),
        sa.Column("validation_rule", sa.String(255)),
        sa.Column("is_key", sa.Boolean, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Reconciliation Results
    op.create_table(
        "reconciliation_results",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("migration_jobs.id")),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("migration_batches.id")),
        sa.Column("source_record_id", sa.String(255)),
        sa.Column("target_record_id", sa.String(255)),
        sa.Column("status", sa.String(50), nullable=False),
        sa.Column("mismatched_fields", postgresql.JSONB),
        sa.Column("severity", sa.String(20), server_default="info"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Lineage Records
    op.create_table(
        "lineage_records",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("migration_jobs.id")),
        sa.Column("record_id", sa.String(255), nullable=False),
        sa.Column("source_table", sa.String(255), nullable=False),
        sa.Column("source_column", sa.String(255), nullable=False),
        sa.Column("source_value", sa.Text),
        sa.Column("target_table", sa.String(255), nullable=False),
        sa.Column("target_column", sa.String(255), nullable=False),
        sa.Column("target_value", sa.Text),
        sa.Column("transformation", sa.String(255)),
        sa.Column("etl_commit_id", sa.String(64)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Audit Events
    op.create_table(
        "audit_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("action", sa.String(100), nullable=False),
        sa.Column("entity_type", sa.String(100)),
        sa.Column("entity_id", sa.String(255)),
        sa.Column("actor", sa.String(255)),
        sa.Column("details", postgresql.JSONB),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Indexes
    op.create_index("ix_jobs_status", "migration_jobs", ["status"])
    op.create_index("ix_jobs_entity", "migration_jobs", ["entity_type"])
    op.create_index("ix_batches_job", "migration_batches", ["job_id"])
    op.create_index("ix_recon_job", "reconciliation_results", ["job_id"])
    op.create_index("ix_recon_status", "reconciliation_results", ["status"])
    op.create_index("ix_lineage_job", "lineage_records", ["job_id"])
    op.create_index("ix_lineage_source", "lineage_records", ["source_table", "source_column"])
    op.create_index("ix_lineage_target", "lineage_records", ["target_table", "target_column"])
    op.create_index("ix_lineage_record", "lineage_records", ["record_id"])
    op.create_index("ix_audit_event", "audit_events", ["event_type"])
    op.create_index("ix_audit_entity", "audit_events", ["entity_type", "entity_id"])


def downgrade() -> None:
    op.drop_table("audit_events")
    op.drop_table("lineage_records")
    op.drop_table("reconciliation_results")
    op.drop_table("entity_mappings")
    op.drop_table("migration_batches")
    op.drop_table("migration_jobs")
