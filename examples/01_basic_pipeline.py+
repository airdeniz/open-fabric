"""
Example 01: Basic Batch Pipeline
---------------------------------
This example demonstrates the simplest end-to-end pipeline:
1. Generate synthetic insurance data
2. Ingest to MinIO Bronze layer as Parquet
3. Run dbt transformations
4. Query the result

Run:
    python examples/01_basic_pipeline.py
"""

import sys
import os
import subprocess
import pandas as pd
from io import BytesIO

sys.path.insert(0, os.path.abspath("."))

from data_generator.generate import generate_all
from minio import Minio

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "bronze"


def step1_generate_data(n=100):
    print("\n[Step 1] Generating synthetic insurance data...")
    policies, versions, insureds = generate_all(n)
    print(f"  ✓ {len(policies)} policies generated")
    print(f"  ✓ {len(versions)} versions generated")
    print(f"  ✓ {len(insureds)} insureds generated")
    return policies, versions, insureds


def step2_ingest_to_minio(policies, versions, insureds):
    print("\n[Step 2] Ingesting data to MinIO Bronze layer...")

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    datasets = {
        "policies/policies.parquet": policies,
        "policy_versions/policy_versions.parquet": versions,
        "insureds/insureds.parquet": insureds,
    }

    for object_name, data in datasets.items():
        df = pd.DataFrame(data)
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        client.put_object(
            BUCKET_NAME, object_name, buffer,
            buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )
        print(f"  ✓ {object_name} uploaded ({len(df)} rows)")


def step3_run_dbt():
    print("\n[Step 3] Running dbt transformations...")
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd="dbt_project/insurance_dwh",
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        print("  ✓ dbt run completed successfully")
    else:
        print("  ✗ dbt run failed")
        print(result.stdout)


def step4_query_result():
    print("\n[Step 4] Querying fct_policy mart table...")
    import duckdb
    conn = duckdb.connect("dbt_project/insurance_dwh/insurance.duckdb")
    df = conn.execute("SELECT product_code, COUNT(*) as count, ROUND(AVG(gross_premium), 2) as avg_premium FROM fct_policy GROUP BY product_code ORDER BY count DESC").df()
    print("\n  fct_policy summary:")
    print(df.to_string(index=False))
    conn.close()


if __name__ == "__main__":
    print("=" * 50)
    print("  Open Fabric — Basic Batch Pipeline Example")
    print("=" * 50)

    policies, versions, insureds = step1_generate_data(100)
    step2_ingest_to_minio(policies, versions, insureds)
    step3_run_dbt()
    step4_query_result()

    print("\n✓ Pipeline completed successfully!")