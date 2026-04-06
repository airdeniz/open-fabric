import pytest
import sys
import os

sys.path.insert(0, os.path.abspath("."))
from data_generator.generate import (
    generate_policy,
    generate_policy_version,
    generate_insured,
    generate_all
)


class TestGeneratePolicy:
    def test_policy_has_required_fields(self):
        policy = generate_policy(1)
        assert "policy_id" in policy
        assert "policy_number" in policy
        assert "product_code" in policy
        assert "status" in policy
        assert "start_date" in policy
        assert "end_date" in policy
        assert "created_at" in policy

    def test_policy_id_format(self):
        policy = generate_policy(1)
        assert policy["policy_id"] == "POL-000001"

    def test_policy_product_code_valid(self):
        valid_codes = ["KASKO", "TRAFIK", "DASK", "SAGLIK"]
        for i in range(20):
            policy = generate_policy(i)
            assert policy["product_code"] in valid_codes

    def test_policy_status_valid(self):
        valid_statuses = ["ACTIVE", "CANCELLED", "EXPIRED"]
        for i in range(20):
            policy = generate_policy(i)
            assert policy["status"] in valid_statuses

    def test_policy_end_date_after_start_date(self):
        policy = generate_policy(1)
        assert policy["end_date"] > policy["start_date"]


class TestGeneratePolicyVersion:
    def test_version_has_required_fields(self):
        version = generate_policy_version(1, 1)
        assert "version_id" in version
        assert "policy_id" in version
        assert "gross_premium" in version
        assert "net_premium" in version
        assert "endorsement_type" in version

    def test_version_premium_positive(self):
        for i in range(20):
            version = generate_policy_version(i, i)
            assert version["gross_premium"] > 0
            assert version["net_premium"] > 0

    def test_version_policy_id_matches(self):
        version = generate_policy_version(5, 1)
        assert version["policy_id"] == "POL-000005"


class TestGenerateInsured:
    def test_insured_has_required_fields(self):
        insured = generate_insured(1, 1)
        assert "insured_id" in insured
        assert "policy_id" in insured
        assert "full_name" in insured
        assert "tc_no" in insured
        assert "birth_date" in insured
        assert "email" in insured

    def test_insured_tc_no_length(self):
        for i in range(20):
            insured = generate_insured(i, i)
            assert len(insured["tc_no"]) == 11


class TestGenerateAll:
    def test_generate_all_counts(self):
        policies, versions, insureds = generate_all(100)
        assert len(policies) == 100
        assert len(versions) == 100
        assert len(insureds) == 100

    def test_generate_all_unique_ids(self):
        policies, versions, insureds = generate_all(100)
        policy_ids = [p["policy_id"] for p in policies]
        assert len(set(policy_ids)) == 100