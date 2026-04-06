import json
import os
import pandas as pd
from minio import Minio
from minio.error import S3Error
from io import BytesIO

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "bronze"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def ensure_bucket():
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        print(f"✓ Bucket '{BUCKET_NAME}' oluşturuldu")
    else:
        print(f"✓ Bucket '{BUCKET_NAME}' zaten mevcut")

def json_to_parquet_and_upload(json_path, object_name):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    size = buffer.getbuffer().nbytes

    client.put_object(
        BUCKET_NAME,
        object_name,
        buffer,
        size,
        content_type="application/octet-stream"
    )
    print(f"✓ {object_name} yüklendi ({len(df)} kayıt, {size} byte)")

if __name__ == "__main__":
    ensure_bucket()

    files = [
        ("data_generator/output/policies.json",        "policies/policies.parquet"),
        ("data_generator/output/policy_versions.json", "policy_versions/policy_versions.parquet"),
        ("data_generator/output/insureds.json",        "insureds/insureds.parquet"),
    ]

    for json_path, object_name in files:
        json_to_parquet_and_upload(json_path, object_name)

    print("\n✓ Tüm veriler Bronze katmanına yüklendi!")