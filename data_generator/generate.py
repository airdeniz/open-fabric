import json
import random
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("tr_TR")

def random_date(start_year=2020, end_year=2025):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_policy(policy_id):
    start_date = random_date()
    end_date = start_date + timedelta(days=365)
    return {
        "policy_id": f"POL-{policy_id:06d}",
        "policy_number": fake.bothify(text="??-######"),
        "product_code": random.choice(["KASKO", "TRAFIK", "DASK", "SAGLIK"]),
        "status": random.choice(["ACTIVE", "CANCELLED", "EXPIRED"]),
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "created_at": datetime.now().isoformat()
    }

def generate_policy_version(policy_id, version_id):
    return {
        "version_id": f"VER-{version_id:06d}",
        "policy_id": f"POL-{policy_id:06d}",
        "version_number": random.randint(1, 5),
        "gross_premium": round(random.uniform(500, 15000), 2),
        "net_premium": round(random.uniform(400, 12000), 2),
        "endorsement_type": random.choice(["NEW", "RENEWAL", "CANCELLATION", "AMENDMENT"]),
        "created_at": datetime.now().isoformat()
    }

def generate_insured(policy_id, insured_id):
    return {
        "insured_id": f"INS-{insured_id:06d}",
        "policy_id": f"POL-{policy_id:06d}",
        "full_name": fake.name(),
        "tc_no": fake.bothify(text="###########"),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=80).strftime("%Y-%m-%d"),
        "phone": fake.phone_number(),
        "email": fake.email(),
        "created_at": datetime.now().isoformat()
    }

def generate_all(n=1000):
    policies = []
    versions = []
    insureds = []

    for i in range(1, n + 1):
        policies.append(generate_policy(i))
        versions.append(generate_policy_version(i, i))
        insureds.append(generate_insured(i, i))

    return policies, versions, insureds

if __name__ == "__main__":
    import os
    import pandas as pd

    os.makedirs("data_generator/output", exist_ok=True)

    policies, versions, insureds = generate_all(1000)

    # JSON kaydet
    with open("data_generator/output/policies.json", "w", encoding="utf-8") as f:
        json.dump(policies, f, ensure_ascii=False, indent=2)

    with open("data_generator/output/policy_versions.json", "w", encoding="utf-8") as f:
        json.dump(versions, f, ensure_ascii=False, indent=2)

    with open("data_generator/output/insureds.json", "w", encoding="utf-8") as f:
        json.dump(insureds, f, ensure_ascii=False, indent=2)

    # Parquet kaydet
    pd.DataFrame(policies).to_parquet("data_generator/output/policies.parquet", index=False)
    pd.DataFrame(versions).to_parquet("data_generator/output/policy_versions.parquet", index=False)
    pd.DataFrame(insureds).to_parquet("data_generator/output/insureds.parquet", index=False)

    print(f"✓ {len(policies)} poliçe üretildi")
    print(f"✓ {len(versions)} versiyon üretildi")
    print(f"✓ {len(insureds)} sigortalı üretildi")
    print("✓ JSON ve Parquet dosyaları oluşturuldu")