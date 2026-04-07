import csv
import uuid
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

OUTPUT_FILE = "data/transactions.csv"
TOTAL_RECORDS = 1200  

MERCHANTS = [
    "STORE_23", "ONLINE_SHOP", "GAS_STATION",
    "SUPERMARKET", "RESTAURANT_A", "PHARMACY",
    "ELECTRONICS_HUB", "CAFE_CENTRAL", "BOOKSTORE"
]

CUSTOMERS = [f"C{str(i).zfill(5)}" for i in range(1, 51)]

def generate_record():
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMERS),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "timestamp": (datetime(2025, 1, 1) + timedelta(
            days=random.randint(0, 364),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "merchant": random.choice(MERCHANTS)
    }

def main():
    records = []

    for _ in range(1000):
        records.append(generate_record())

    duplicates = random.sample(records, 50)
    records.extend(duplicates)

    for _ in range(50):
        rec = generate_record()
        rec["amount"] = random.choice([0, -10.5, -25.0, -100.0])
        records.append(rec)

    for _ in range(10):
        rec = generate_record()
        rec["amount"] = 0.0
        records.append(rec)

    random.shuffle(records)

    with open(OUTPUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "transaction_id", "customer_id",
            "amount", "timestamp", "merchant"
        ])
        writer.writeheader()
        writer.writerows(records)

    print(f"✅ Generated {len(records)} records → {OUTPUT_FILE}")
    print(f"   - Normal records   : 1000")
    print(f"   - Duplicates added : 50")
    print(f"   - Negative amounts : 50")
    print(f"   - Zero amounts     : 10")

if __name__ == "__main__":
    main()