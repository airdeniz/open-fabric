from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airdeniz",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="insurance_dwh_pipeline",
    description="Insurance DWH end-to-end pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["insurance", "dwh", "dbt"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="cd /opt/airflow && python data_generator/generate.py",
    )

    ingest_to_bronze = BashOperator(
        task_id="ingest_to_bronze",
        bash_command="cd /opt/airflow && python ingestion/ingest.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project/insurance_dwh && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project/insurance_dwh && dbt test --profiles-dir .",
    )

    generate_data >> ingest_to_bronze >> dbt_run >> dbt_test