import pytest
import sys
import os
import json
import pandas as pd
from io import BytesIO
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath("."))


class TestJsonToParquet:
    def test_parquet_conversion(self):
        data = [
            {
                "policy_id": "POL-000001",
                "product_code": "KASKO",
                "status": "ACTIVE",
                "start_date": "2024-01-01",
                "end_date": "2025-01-01",
                "created_at": "2026-01-01T00:00:00"
            }
        ]
        df = pd.DataFrame(data)
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        df_read = pd.read_parquet(buffer)
        assert len(df_read) == 1
        assert df_read["policy_id"][0] == "POL-000001"
        assert df_read["product_code"][0] == "KASKO"

    def test_parquet_preserves_all_columns(self):
        data = [
            {
                "policy_id": "POL-000001",
                "product_code": "KASKO",
                "status": "ACTIVE",
                "start_date": "2024-01-01",
                "end_date": "2025-01-01",
                "created_at": "2026-01-01T00:00:00"
            }
        ]
        df = pd.DataFrame(data)
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        df_read = pd.read_parquet(buffer)
        assert set(df_read.columns) == set(df.columns)

    def test_parquet_multiple_records(self):
        data = [
            {"policy_id": f"POL-{i:06d}", "product_code": "KASKO"}
            for i in range(100)
        ]
        df = pd.DataFrame(data)
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        df_read = pd.read_parquet(buffer)
        assert len(df_read) == 100


class TestKafkaProducer:
    def test_policy_event_structure(self):
        from data_generator.generate import generate_policy
        policy = generate_policy(1)

        required_fields = [
            "policy_id", "policy_number", "product_code",
            "status", "start_date", "end_date", "created_at"
        ]
        for field in required_fields:
            assert field in policy

    def test_policy_event_serializable(self):
        from data_generator.generate import generate_policy
        policy = generate_policy(1)

        serialized = json.dumps(policy, ensure_ascii=False).encode("utf-8")
        deserialized = json.loads(serialized.decode("utf-8"))

        assert deserialized["policy_id"] == policy["policy_id"]
        assert deserialized["product_code"] == policy["product_code"]


class TestDataValidation:
    def test_no_null_policy_ids(self):
        from data_generator.generate import generate_all
        policies, _, _ = generate_all(100)
        for p in policies:
            assert p["policy_id"] is not None
            assert p["policy_id"] != ""

    def test_no_null_premiums(self):
        from data_generator.generate import generate_all
        _, versions, _ = generate_all(100)
        for v in versions:
            assert v["gross_premium"] is not None
            assert v["net_premium"] is not None

    def test_gross_premium_greater_than_net(self):
        from data_generator.generate import generate_all
        _, versions, _ = generate_all(100)
        for v in versions:
            assert v["gross_premium"] >= v["net_premium"]