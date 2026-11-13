"""
Order Simulator — Kafka Producer

Generates fake food delivery orders every few seconds
and publishes them to a Kafka topic.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

# ─── Configuration ───────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "food_orders")

# ─── Seed Data (mirrors the Postgres seed) ───────────────────
ITEMS = [
    {"item_id": 1,  "name": "Margherita Pizza",    "category": "Pizza",   "price": 8.50},
    {"item_id": 2,  "name": "Pepperoni Pizza",      "category": "Pizza",   "price": 10.00},
    {"item_id": 3,  "name": "Quattro Formaggi",     "category": "Pizza",   "price": 11.50},
    {"item_id": 4,  "name": "Caesar Salad",          "category": "Salad",   "price": 7.00},
    {"item_id": 5,  "name": "Greek Salad",           "category": "Salad",   "price": 7.50},
    {"item_id": 6,  "name": "Spaghetti Carbonara",  "category": "Pasta",   "price": 9.50},
    {"item_id": 7,  "name": "Penne Arrabbiata",     "category": "Pasta",   "price": 8.00},
    {"item_id": 8,  "name": "Tiramisu",              "category": "Dessert", "price": 5.50},
    {"item_id": 9,  "name": "Coca-Cola",             "category": "Drink",   "price": 2.50},
    {"item_id": 10, "name": "Water",                 "category": "Drink",   "price": 1.50},
    {"item_id": 11, "name": "Bruschetta",            "category": "Starter", "price": 6.00},
    {"item_id": 12, "name": "Garlic Bread",          "category": "Starter", "price": 4.00},
]

RESTAURANTS = [
    {"restaurant_id": 1, "name": "Bella Napoli",       "city": "Milan"},
    {"restaurant_id": 2, "name": "Sakura Sushi",        "city": "Milan"},
    {"restaurant_id": 3, "name": "La Piazzetta",        "city": "Rome"},
    {"restaurant_id": 4, "name": "Burger Factory",      "city": "Turin"},
    {"restaurant_id": 5, "name": "Trattoria da Mario",  "city": "Florence"},
]

CUSTOMERS = [
    {"customer_id": 1, "name": "Marco Rossi",     "city": "Milan"},
    {"customer_id": 2, "name": "Giulia Bianchi",  "city": "Rome"},
    {"customer_id": 3, "name": "Luca Verdi",      "city": "Turin"},
    {"customer_id": 4, "name": "Sara Russo",      "city": "Florence"},
    {"customer_id": 5, "name": "Andrea Moretti",  "city": "Milan"},
    {"customer_id": 6, "name": "Elena Conti",     "city": "Rome"},
    {"customer_id": 7, "name": "Davide Romano",   "city": "Milan"},
    {"customer_id": 8, "name": "Francesca Ricci", "city": "Turin"},
]

fake = Faker("it_IT")


def generate_order() -> dict:
    """Generate a single random food order."""
    customer = random.choice(CUSTOMERS)
    restaurant = random.choice(RESTAURANTS)
    item = random.choice(ITEMS)
    quantity = random.randint(1, 4)

    return {
        "customer_id": customer["customer_id"],
        "customer_name": customer["name"],
        "restaurant_id": restaurant["restaurant_id"],
        "restaurant_name": restaurant["name"],
        "item_id": item["item_id"],
        "item_name": item["name"],
        "item_category": item["category"],
        "unit_price": item["price"],
        "quantity": quantity,
        "total_price": round(item["price"] * quantity, 2),
        "delivery_city": customer["city"],
        "delivery_address": fake.address().replace("\n", ", "),
        "status": "pending",
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
    }


def create_producer() -> KafkaProducer:
    """Create Kafka producer with retry logic for startup."""
    max_retries = 30
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            print(f"✅ Connected to Kafka at {KAFKA_SERVERS}")
            return producer
        except Exception as e:
            print(f"⏳ Waiting for Kafka (attempt {attempt}/{max_retries}): {e}")
            time.sleep(2)

    raise ConnectionError(f"Could not connect to Kafka after {max_retries} attempts")


def main():
    """Main loop: generate and publish orders continuously."""
    producer = create_producer()
    order_count = 0

    print(f"🚀 Order simulator started — publishing to topic '{KAFKA_TOPIC}'")

    try:
        while True:
            order = generate_order()
            order_count += 1

            # Use customer_id as the Kafka key for partitioning
            key = str(order["customer_id"])

            producer.send(KAFKA_TOPIC, key=key, value=order)
            producer.flush()

            print(
                f"📦 Order #{order_count}: "
                f"{order['customer_name']} ordered {order['quantity']}x "
                f"{order['item_name']} from {order['restaurant_name']} "
                f"(€{order['total_price']:.2f})"
            )

            # Random delay between 2-8 seconds to simulate real traffic
            delay = random.uniform(2, 8)
            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\n🛑 Stopped after {order_count} orders")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
