# ADR-001: Storage Layer — MinIO

## Date
2026-04-07

## Status
Accepted

## Context
We needed an object storage solution to serve as the data lake (Bronze layer) for raw Parquet files. The storage layer needed to be:
- S3-compatible so ingestion scripts and dbt could read from it easily
- Self-hosted to avoid cloud costs in local development
- A realistic alternative to Microsoft Fabric's OneLake

## Decision
We chose **MinIO** as the object storage layer.

## Reasons
- Fully S3-compatible API — same code works with AWS S3 in production
- Runs locally via Docker with zero configuration
- Supports bucket policies, versioning, and a web UI out of the box
- Widely used in enterprise data platforms as an on-premise S3 alternative
- Free and open source

## Alternatives Considered
- **AWS S3** — requires cloud account and incurs costs, not suitable for local dev
- **Azure Data Lake Storage** — tied to Azure ecosystem, defeats the purpose of open source stack
- **Local filesystem** — not scalable, no web UI, not production-realistic

## Consequences
- All raw Parquet files land in the `bronze` bucket under structured prefixes
- Near real-time Kafka consumer also writes to MinIO under `realtime/` prefix
- In production, switching to AWS S3 requires only changing the endpoint URL and credentials