"""
Example 02: Near Real-Time Streaming
--------------------------------------
This example demonstrates the Kafka-based streaming pipeline:
1. Producer sends a policy event to Kafka every 5 seconds (faster than default for demo)
2. Consumer reads from Kafka and writes to MinIO in real-time
3. After 3 messages, shows what landed in MinIO

Prerequisites:
    - Kafka must be running (docker-compose up kafka -d)
    - MinIO must be running (docker-compose up minio -d)

Run in two separate terminals:
    Terminal 1: python examples/02_realtime_streaming.py producer
    Terminal 2: python examples/02_realtime_streaming.py consumer
"""

import sys
import os
import json
import random
import time
from datetime import datetime, timedelta
from io import BytesIO

sys.path.insert(0, os.path.abspath("."))


def run_producer(n=5, interval=5):
    from faker import Faker
    from kafka import KafkaProducer

    fake = Faker("tr_TR")

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )

    print(f"Producer started — sending {n} events every {interval} seconds...\n")

    for i in range(1, n + 1):
        start_date = datetime.now() - timedelta(days=random.randint(0, 365))
        event = {
            "policy_id": fake.bothify(text="POL-######"),
            "product_code": random.choice(["KASKO", "TRAFIK", "DASK", "SAGLIK"]),
            "status": random.choice(["ACTIVE", "CANCELLED", "EXPIRED"]),
            "gross_premium": round(random.uniform(500, 15000), 2),
            "full_name": fake.name(),
            "email": fake.email(),
            "created_at": datetime.now().isoformat()
        }

        producer.send("insurance-policies", value=event)
        producer.flush()
        print(f"[{i}/{n}] Sent: {event['policy_id']} | {event['product_code']} | {event['gross_premium']} TL")
        time.sleep(interval)

    print("\n✓ Producer finished.")


def run_consumer(max_messages=5):
    import pandas as pd
    from kafka import KafkaConsumer
    from minio import Minio

    client = Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    if not client.bucket_exists("bronze"):
        client.make_bucket("bronze")

    consumer = KafkaConsumer(
        "insurance-policies",
        bootstrap_servers="localhost:9092",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="example-consumer-group"
    )

    print(f"Consumer started — waiting for messages (max {max_messages})...\n")
    count = 0

    for message in consumer:
        event = message.value
        df = pd.DataFrame([event])
        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"realtime/example_{event['policy_id']}_{timestamp}.parquet"

        client.put_object(
            "bronze", object_name, buffer,
            buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )

        print(f"[{count + 1}] Received and stored: {event['policy_id']} | {event['product_code']}")
        count += 1

        if count >= max_messages:
            break

    print(f"\n✓ Consumer finished. {count} messages processed.")

    print("\nFiles in MinIO (realtime/):")
    objects = list(client.list_objects("bronze", prefix="realtime/", recursive=True))
    for obj in objects[-5:]:
        print(f"  {obj.object_name} — {obj.size} bytes")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python examples/02_realtime_streaming.py producer")
        print("  python examples/02_realtime_streaming.py consumer")
        sys.exit(1)

    mode = sys.argv[1]

    if mode == "producer":
        run_producer(n=5, interval=5)
    elif mode == "consumer":
        run_consumer(max_messages=5)
    else:
        print(f"Unknown mode: {mode}. Use 'producer' or 'consumer'.")