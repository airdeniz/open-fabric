import json
import time
import random
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer

fake = Faker("tr_TR")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

def random_date(start_year=2020, end_year=2026):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def generate_policy_event():
    policy_id = fake.bothify(text="POL-######")
    start_date = random_date()
    end_date = start_date + timedelta(days=365)

    policy = {
        "policy_id": policy_id,
        "policy_number": fake.bothify(text="??-######"),
        "product_code": random.choice(["KASKO", "TRAFIK", "DASK", "SAGLIK"]),
        "status": random.choice(["ACTIVE", "CANCELLED", "EXPIRED"]),
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "gross_premium": round(random.uniform(500, 15000), 2),
        "net_premium": round(random.uniform(400, 12000), 2),
        "full_name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "created_at": datetime.now().isoformat()
    }

    return policy

if __name__ == "__main__":
    print("✓ Kafka Producer başlatıldı — her 30 saniyede bir poliçe üretiliyor...")
    counter = 1

    while True:
        event = generate_policy_event()
        producer.send("insurance-policies", value=event)
        producer.flush()
        print(f"[{counter}] Gönderildi: {event['policy_id']} | {event['product_code']} | {event['gross_premium']} TL | {datetime.now().strftime('%H:%M:%S')}")
        counter += 1
        time.sleep(30)