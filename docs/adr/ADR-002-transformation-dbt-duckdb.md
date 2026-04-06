# ADR-002: Transformation Layer — dbt Core + DuckDB

## Date
2026-04-07

## Status
Accepted

## Context
We needed a transformation layer to convert raw Bronze data into clean, business-ready Gold tables. The layer needed to:
- Support SQL-based transformations
- Enable data quality testing
- Be lightweight enough to run locally without a heavy database engine
- Serve as a realistic alternative to Microsoft Fabric's Notebooks and Lakehouse

## Decision
We chose **dbt Core** for transformations and **DuckDB** as the execution engine.

## Reasons

**dbt Core:**
- Industry standard for SQL-based data transformations
- Built-in testing framework (unique, not_null, accepted_values)
- Clear separation of staging and mart layers (Medallion architecture)
- Generates documentation automatically
- Widely adopted in modern data stacks

**DuckDB:**
- Can read Parquet files directly without loading into a database first
- Extremely fast for analytical queries on local files
- Zero infrastructure — runs as an embedded database
- In production, dbt can be pointed at Snowflake, BigQuery or Redshift with minimal changes

## Alternatives Considered
- **Apache Spark** — too heavy for local development, requires cluster setup
- **PostgreSQL + dbt** — requires running a separate database server
- **Pandas** — not SQL-based, no built-in testing, harder to maintain at scale

## Consequences
- Staging models read directly from Parquet files in `data_generator/output/`
- `fct_policy` mart model is materialized as a table in DuckDB
- 11 data quality tests run on every `dbt test` execution
- Switching to a cloud warehouse in production requires only changing `profiles.yml`