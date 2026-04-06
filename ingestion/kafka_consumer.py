import json
import os
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio
from io import BytesIO
import pandas as pd

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

consumer = KafkaConsumer(
    "insurance-policies",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    group_id="insurance-consumer-group"
)

def upload_to_minio(event):
    df = pd.DataFrame([event])
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    size = buffer.getbuffer().nbytes

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    object_name = f"realtime/policy_{event['policy_id']}_{timestamp}.parquet"

    client.put_object(
        BUCKET_NAME,
        object_name,
        buffer,
        size,
        content_type="application/octet-stream"
    )
    print(f"✓ MinIO'ya yüklendi: {object_name}")

if __name__ == "__main__":
    print("✓ Kafka Consumer başlatıldı — mesajlar bekleniyor...")

    for message in consumer:
        event = message.value
        print(f"→ Alındı: {event['policy_id']} | {event['product_code']} | {datetime.now().strftime('%H:%M:%S')}")
        upload_to_minio(event)