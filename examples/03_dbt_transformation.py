"""
Example 03: dbt Transformation Deep Dive
------------------------------------------
This example demonstrates how dbt transforms raw Bronze data
into clean, business-ready Gold tables. It shows:

1. What raw data looks like before transformation
2. Running dbt models step by step
3. What the final fct_policy mart table looks like
4. Running dbt tests and showing results

Prerequisites:
    - data_generator/output/*.parquet files must exist
      Run: python data_generator/generate.py

Run:
    python examples/03_dbt_transformation.py
"""

import sys
import os
import subprocess

sys.path.insert(0, os.path.abspath("."))


def show_raw_data():
    import pandas as pd

    print("\n[Step 1] Raw Bronze data (before transformation)")
    print("-" * 50)

    policies = pd.read_parquet("data_generator/output/policies.parquet")
    print(f"\n  policies.parquet — {len(policies)} rows")
    print(policies[["policy_id", "product_code", "status", "start_date"]].head(3).to_string(index=False))

    versions = pd.read_parquet("data_generator/output/policy_versions.parquet")
    print(f"\n  policy_versions.parquet — {len(versions)} rows")
    print(versions[["version_id", "policy_id", "gross_premium", "net_premium"]].head(3).to_string(index=False))

    insureds = pd.read_parquet("data_generator/output/insureds.parquet")
    print(f"\n  insureds.parquet — {len(insureds)} rows")
    print(insureds[["insured_id", "policy_id", "full_name", "email"]].head(3).to_string(index=False))


def run_dbt_models():
    print("\n[Step 2] Running dbt models...")
    print("-" * 50)

    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "."],
        cwd="dbt_project/insurance_dwh",
        capture_output=True,
        text=True
    )

    for line in result.stdout.splitlines():
        if any(x in line for x in ["START", "OK", "ERROR", "PASS", "FAIL", "Done"]):
            print(f"  {line.strip()}")


def run_dbt_tests():
    print("\n[Step 3] Running dbt tests...")
    print("-" * 50)

    result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "."],
        cwd="dbt_project/insurance_dwh",
        capture_output=True,
        text=True
    )

    for line in result.stdout.splitlines():
        if any(x in line for x in ["PASS", "FAIL", "ERROR", "Done"]):
            print(f"  {line.strip()}")


def show_gold_data():
    import duckdb

    print("\n[Step 4] Gold layer — fct_policy mart table")
    print("-" * 50)

    conn = duckdb.connect("dbt_project/insurance_dwh/insurance.duckdb")

    total = conn.execute("SELECT COUNT(*) FROM fct_policy").fetchone()[0]
    print(f"\n  Total rows: {total}")

    print("\n  By product code:")
    df = conn.execute("""
        SELECT
            product_code,
            COUNT(*) as policy_count,
            ROUND(AVG(gross_premium), 2) as avg_gross_premium,
            ROUND(AVG(net_premium), 2) as avg_net_premium,
            ROUND(AVG(commission_amount), 2) as avg_commission
        FROM fct_policy
        GROUP BY product_code
        ORDER BY policy_count DESC
    """).df()
    print(df.to_string(index=False))

    print("\n  By status:")
    df2 = conn.execute("""
        SELECT
            status,
            COUNT(*) as count,
            ROUND(AVG(policy_duration_days), 0) as avg_duration_days
        FROM fct_policy
        GROUP BY status
        ORDER BY count DESC
    """).df()
    print(df2.to_string(index=False))

    conn.close()


if __name__ == "__main__":
    print("=" * 50)
    print("  Open Fabric — dbt Transformation Example")
    print("=" * 50)

    show_raw_data()
    run_dbt_models()
    run_dbt_tests()
    show_gold_data()

    print("\n✓ Example completed successfully!")