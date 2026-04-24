# Data Migration Engine

A zero-downtime framework for migrating large-scale databases from legacy systems (DB2, mainframe) onto modern stacks. Orchestrates dual-write and shadow-read cutover patterns with automated validation, reconciliation, and lineage tracking.

## Why this exists

Enterprise database migrations (DB2 → PostgreSQL, Oracle → Aurora, etc.) almost always fail on reconciliation, not on the happy-path writes. Teams get the pipeline working but can't confidently prove that the new system's data matches the old — especially under concurrent writes and across long migration windows.

This engine makes verification as rigorous as the write path.

## What it does

- **Dual-write orchestration** — writes to both source and target during the migration window, with configurable retry and idempotency.
- **Shadow reads** — production traffic goes to the new system, verified against the old, with configurable drift thresholds.
- **Automated reconciliation** — row-by-row comparison between source and target, with exception queues for drift.
- **Lineage tracking** — records every transformation applied to each record for audit and debugging.
- **AI-assisted investigation** — optional LangChain-based assistant over the reconciliation output and source schema/lineage metadata, so engineers can ask questions in natural language.

## Tech

- **Language:** Python 3.11+
- **Batch processing:** Apache Spark (via AWS Glue)
- **Storage:** S3 (staging), PostgreSQL (metadata, reconciliation results)
- **Orchestration:** AWS Step Functions
- **Infra as code:** Terraform
- **AI layer (optional):** LangChain + pgvector for metadata and lineage queries

## Architecture (high level)

```
Source DB  ──►  Extract (Glue/Spark)  ──►  S3 staging  ──►  Transform  ──►  Target DB
                                                     │
                                                     └─►  Reconciliation (Spark)
                                                                  │
                                                                  └─► Exception queue / dashboard
```

## Quick start

```bash
git clone https://github.com/rajasekharthejan/data-migration-engine
cd data-migration-engine
# Requires: Python 3.11+, Terraform 1.5+, AWS CLI configured

terraform -chdir=infra init && terraform -chdir=infra apply
pip install -e .
python -m migration_engine run --config ./examples/db2-to-postgres.yaml
```

## Configuration

Migrations are defined in YAML. Example:

```yaml
name: customer-records
source:
  type: db2
  connection: ${DB2_URL}
target:
  type: postgres
  connection: ${POSTGRES_URL}
strategy: dual-write
reconciliation:
  mode: row-by-row
  drift_threshold: 0.01
  key_columns: [customer_id]
```

## Status

In development. Core reconciliation module functional. Shadow-read verification and AI assistant under active work.

## License

MIT
