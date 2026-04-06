# Insurance DWH — Open Source Data Platform

Open source equivalent of Microsoft Fabric, built for insurance data warehouse use cases.

## Architecture
![Dashboard](docs/screenshot.png)

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

## Testing

The project includes two layers of testing:

**Python Unit Tests (pytest)** — 20 tests covering data generation and ingestion logic:

- `TestGeneratePolicy` — validates policy structure, ID format, product codes, statuses and date logic
- `TestGeneratePolicyVersion` — validates version structure, premium values and policy ID matching
- `TestGenerateInsured` — validates insured structure and TC number format
- `TestGenerateAll` — validates bulk generation counts and unique ID constraints
- `TestJsonToParquet` — validates Parquet conversion, column preservation and multi-record handling
- `TestKafkaProducer` — validates Kafka event structure and JSON serialization
- `TestDataValidation` — validates null checks and business rules (gross premium >= net premium)

Run unit tests:
```bash
pytest tests/ -v
```

**dbt Tests** — 11 data quality tests on staging models:

- `unique` and `not_null` on all primary keys
- `accepted_values` on policy status field

Run dbt tests:
```bash
cd dbt_project/insurance_dwh
dbt test
```

## Architecture Decision Records

All architectural decisions are documented in the [docs/adr](docs/adr) folder.

| ADR | Decision |
|---|---|
| [ADR-001](docs/adr/ADR-001-storage-minio.md) | Why MinIO for object storage |
| [ADR-002](docs/adr/ADR-002-transformation-dbt-duckdb.md) | Why dbt Core + DuckDB for transformations |
| [ADR-003](docs/adr/ADR-003-orchestration-airflow.md) | Why Apache Airflow for orchestration |
| [ADR-004](docs/adr/ADR-004-streaming-kafka.md) | Why Apache Kafka for streaming |
| [ADR-005](docs/adr/ADR-005-visualization-superset.md) | Why Apache Superset for visualization |

## Quick Start
```bash
# 1. Clone the repo
git clone https://github.com/airdeniz/open-fabric.git
cd open-fabric

# 2. Start all services with one command
.\start.bat

# 3. Run unit tests
pytest tests/ -v

# 4. For near real-time streaming (optional, two separate terminals)
python ingestion/kafka_producer.py
python ingestion/kafka_consumer.py
```

## Examples

The [examples](examples) folder contains ready-to-run scripts demonstrating key use cases:

| Example | Description |
|---|---|
| [01_basic_pipeline.py](examples/01_basic_pipeline.py) | End-to-end batch pipeline — generate, ingest, transform, query |
| [02_realtime_streaming.py](examples/02_realtime_streaming.py) | Kafka producer and consumer for near real-time streaming |
| [03_dbt_transformation.py](examples/03_dbt_transformation.py) | dbt transformation deep dive — raw data to Gold layer |

## Services

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8081 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
