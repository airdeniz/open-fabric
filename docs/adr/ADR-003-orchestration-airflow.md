# ADR-003: Orchestration Layer — Apache Airflow

## Date
2026-04-07

## Status
Accepted

## Context
We needed a pipeline orchestration tool to automate and schedule the end-to-end data pipeline. The tool needed to:
- Schedule and monitor pipeline runs
- Handle task dependencies (generate → ingest → dbt run → dbt test)
- Provide a visual UI for pipeline monitoring
- Be a realistic alternative to Microsoft Fabric's Data Factory Pipelines

## Decision
We chose **Apache Airflow** as the orchestration layer.

## Reasons
- Industry standard for data pipeline orchestration
- DAG-based approach makes dependencies explicit and visual
- Rich UI for monitoring task status, logs and run history
- Large ecosystem of operators (Bash, Python, HTTP, database connectors)
- Widely used in enterprise data engineering teams
- Runs easily via Docker

## Alternatives Considered
- **Prefect** — modern and developer-friendly but less widely adopted in enterprises
- **Dagster** — excellent for data assets but steeper learning curve
- **Cron jobs** — no UI, no retry logic, no dependency management
- **Luigi** — older, less active community

## Consequences
- `insurance_dwh_pipeline` DAG runs daily and executes all pipeline steps in order
- SQLite is used as metadata DB in local dev (not suitable for production)
- In production, Airflow should use PostgreSQL as metadata DB and CeleryExecutor or KubernetesExecutor
- Pipeline is fully observable via Airflow UI at http://localhost:8081