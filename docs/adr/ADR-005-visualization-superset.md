# ADR-005: Visualization Layer — Apache Superset

## Date
2026-04-07

## Status
Accepted

## Context
We needed a business intelligence and dashboard tool to visualize the Gold layer data from the `fct_policy` mart table. The tool needed to:
- Connect to DuckDB directly
- Support interactive dashboards and charts
- Be self-hosted and open source
- Be a realistic alternative to Microsoft Fabric's Power BI

## Decision
We chose **Apache Superset** as the visualization layer.

## Reasons
- Fully open source and self-hosted
- Supports DuckDB via SQLAlchemy connection
- Rich chart library (pie charts, bar charts, line charts, tables, big numbers)
- Role-based access control out of the box
- Active community and widely adopted in modern data stacks
- Runs easily via Docker

## Alternatives Considered
- **Grafana** — better suited for time-series and infrastructure metrics, not ideal for business analytics
- **Metabase** — user-friendly but limited SQL customization
- **Redash** — query-focused, less rich dashboard experience
- **Power BI** — proprietary, Windows-only desktop app, tied to Microsoft ecosystem

## Consequences
- Superset connects to DuckDB via `duckdb:////tmp/insurance.duckdb`
- DuckDB file must be copied to Superset container on every `dbt run` (handled by `start.bat`)
- `fct_policy` is the primary dataset powering all dashboards
- In production, Superset should connect directly to a cloud warehouse instead of a local DuckDB file
- `Insurance DWH Dashboard` provides policy distribution by product code