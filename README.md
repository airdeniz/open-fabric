# Insurance DWH — Open Source Data Platform

Open source equivalent of Microsoft Fabric, built for insurance data warehouse use cases.

## Architecture

| Layer | Tool | Fabric Equivalent |
|---|---|---|
| Storage | MinIO + Parquet | OneLake |
| Ingestion | Python + Apache Kafka | Data Factory |
| Transformation | dbt Core + DuckDB | Fabric Notebooks |
| Orchestration | Apache Airflow | Data Factory Pipelines |
| BI / Dashboard | Apache Superset | Power BI |

## Stack

- **Data Generator**: Faker (Python) — 1000 synthetic insurance records
- **Apache Kafka**: Near real-time data streaming (new policy every 30 seconds)
- **MinIO**: S3-compatible object storage (Bronze layer)
- **dbt Core**: Staging and mart models with 11 tests
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

# 2. Start all services with one command
.\start.bat

# 3. For near real-time streaming (optional, two separate terminals)
python ingestion/kafka_producer.py
python ingestion/kafka_consumer.py
```

## Services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |