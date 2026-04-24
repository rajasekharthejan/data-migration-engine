# Zero-Downtime Data Migration & Reconciliation Engine

A production-grade data migration pipeline that moves insurance data from DB2 mainframes to a modern cloud platform (FAST) with zero downtime, full data lineage tracking, and AI-powered data stewardship.

## Architecture

```
DB2 Mainframe → Parallel Extract → S3 Staging (Parquet)
    → AWS Glue ETL (Spark) → Field Transforms + Business Rules
    → FAST Cloud Platform (Blue/Green) → Record-by-Record Reconciliation
    → Auto-Promote or Rollback
```

## Key Features

- **Parallel DB2 Extraction**: Multi-process extraction with XXHash checksums for integrity verification
- **AWS Glue ETL**: Spark-based transformations with configurable field mapping rules
- **Business Rule Validation**: Insurance-domain validators (policy ID format, premium ranges, coverage minimums)
- **Blue/Green Deployment**: Zero-downtime loading into FAST with atomic promotion and instant rollback
- **Record-by-Record Reconciliation**: Field-level comparison with configurable numeric tolerance (0.001)
- **Metadata Lineage**: Per-field source-to-target tracking with forward/backward query support
- **AI Chatbot**: Azure OpenAI (GPT-4) powered natural language interface for lineage queries
- **99.99% Match Rate**: Production promotion requires 99.99% reconciliation match rate with zero missing records

## Project Structure

```
src/
├── api/              # FastAPI REST endpoints
├── chatbot/          # AI lineage chatbot (Azure OpenAI + ChromaDB RAG)
├── config/           # Pydantic settings management
├── extractors/       # DB2 extraction (single + parallel)
├── lineage/          # Metadata lineage tracking engine
├── loaders/          # S3 staging + FAST system loader
├── models/           # SQLAlchemy models + Pydantic schemas
├── pipeline/         # Migration orchestrator
├── reconciliation/   # Reconciler + rollback manager
├── transformers/     # Field transforms, business rules, Glue ETL
└── utils/            # Database connection, logging
tests/                # pytest test suite
scripts/              # CLI tools for running migrations
alembic/              # Database migrations
docs/                 # Learning guide with architecture diagrams
```

## Quick Start

```bash
# Start infrastructure
docker-compose up -d

# Run database migrations
alembic upgrade head

# Run a migration (dry run)
python scripts/run_migration.py --entity policies --table POLICY_MASTER --dry-run

# Run tests
pytest tests/ -v

# Start API server
uvicorn src.api.app:app --reload
```

## Tech Stack

- **Python 3.11+** with type hints
- **SQLAlchemy** + PostgreSQL (metadata store)
- **pandas** + PyArrow (data processing)
- **boto3** (AWS S3, Glue)
- **FastAPI** (REST API)
- **Azure OpenAI** GPT-4 (AI chatbot)
- **ChromaDB** (vector store for RAG)
- **structlog** (structured logging)
- **XXHash** (fast checksums, 10GB/s)
- **Docker** + docker-compose
- **GitHub Actions** CI/CD

## Entity Types

| Entity | DB2 Source Table | FAST Target | Key Fields |
|--------|-----------------|-------------|------------|
| Policies | POLICY_MASTER | fast_policies | policy_id |
| Claims | CLAIMS_MASTER | fast_claims | claim_id |
| Premiums | PREMIUM_HIST | fast_premiums | premium_id |
| Coverage | COVERAGE_DTL | fast_coverage | coverage_id |
| Agents | AGENT_MASTER | fast_agents | agent_id |

## Pipeline Stages

1. **EXTRACT** — Pull from DB2 in parallel batches with checksums
2. **STAGE** — Upload Parquet to S3 staging bucket
3. **TRANSFORM** — Apply field mappings (date formats, packed decimals, SSN masking)
4. **VALIDATE** — Run business rules (policy ID format, premium ranges)
5. **LOAD** — Push to FAST green deployment slot
6. **RECONCILE** — Compare source vs target field-by-field
7. **PROMOTE** — Atomic blue/green swap (or rollback on failure)
