# Insurance DWH — Open Source Data Platform

Open source equivalent of Microsoft Fabric, built for insurance data warehouse use cases.

## Architecture

| Layer | Tool | Fabric Equivalent |
|---|---|---|
| Storage | MinIO + Delta Lake | OneLake |
| Ingestion | Python + Parquet | Data Factory |
| Transformation | dbt Core + DuckDB | Fabric Notebooks |
| Orchestration | Apache Airflow | Data Factory Pipelines |
| BI / Dashboard | Apache Superset | Power BI |

## Stack

- **Data Generator**: Faker (Python) — 1000 synthetic insurance records
- **MinIO**: S3-compatible object storage (Bronze layer)
- **dbt Core**: Staging and mart models with tests
- **Apache Airflow**: End-to-end pipeline orchestration
- **Apache Superset**: Interactive dashboards

## Models

| Model | Layer | Description |
|---|---|---|
| stg_policies | staging | Policy master data |
| stg_policy_versions | staging | Policy version and premium data |
| stg_insureds | staging | Insured person data |
| fct_policy | mart | Policy fact table |

## Quick Start
```bash
# 1. Clone the repo
git clone https://github.com/airdeniz/open-fabric.git
cd open-fabric

# 2. Start all services
docker-compose up -d

# 3. Generate data
python data_generator/generate.py

# 4. Ingest to MinIO
python ingestion/ingest.py

# 5. Run dbt models
cd dbt_project/insurance_dwh
dbt run
dbt test

# 6. Open dashboards
# Airflow:  http://localhost:8081 (admin/admin)
# Superset: http://localhost:8088 (admin/admin)
# MinIO:    http://localhost:9001 (minioadmin/minioadmin)
```

## Services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Spark | http://localhost:8080 | — |