import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "ecommerce-events"


products = [
    {"product_id": 1, "name": "Laptop", "price": 999.99},
    {"product_id": 2, "name": "Headphones", "price": 149.99},
    {"product_id": 3, "name": "Keyboard", "price": 89.99},
    {"product_id": 4, "name": "Mouse", "price": 49.99},
    {"product_id": 5, "name": "Monitor", "price": 299.99},
]


event_types = [
    "view_product",
    "add_to_cart",
    "purchase",
    "refund",
]


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def generate_event():
    product = random.choice(products)
    event_type = random.choice(event_types)

    amount = product["price"] if event_type in ["purchase", "refund"] else 0.0

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "product_id": product["product_id"],
        "event_type": event_type,
        "amount": amount,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    return event


print("Starting E-Commerce Event Generator...")

try:
    while True:
        event = generate_event()

        producer.send(
            KAFKA_TOPIC,
            value=event,
        )

        producer.flush()

        print(f"Sent: {event}")

        time.sleep(random.uniform(0.2, 1.0))

except KeyboardInterrupt:
    print("\nStopping producer...")

finally:
    producer.close()
