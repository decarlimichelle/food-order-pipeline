"""
Kafka Consumer — writes to both PostgreSQL and MongoDB

Reads order events from Kafka and:
  1. Saves the raw JSON to MongoDB (data lake / audit log)
  2. Inserts structured rows into PostgreSQL (data warehouse)
"""

import json
import os
import time

import psycopg2
from kafka import KafkaConsumer
from pymongo import MongoClient

# ─── Configuration ───────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "food_orders")

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline123"),
    "dbname": os.getenv("POSTGRES_DB", "food_orders"),
}

MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST", "localhost"),
    "port": int(os.getenv("MONGO_PORT", 27017)),
    "user": os.getenv("MONGO_USER", "pipeline"),
    "password": os.getenv("MONGO_PASSWORD", "pipeline123"),
}


def connect_postgres(max_retries: int = 30) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**POSTGRES_CONFIG)
            conn.autocommit = True
            print("✅ Connected to PostgreSQL")
            return conn
        except Exception as e:
            print(f"⏳ Waiting for PostgreSQL (attempt {attempt}): {e}")
            time.sleep(2)
    raise ConnectionError("Could not connect to PostgreSQL")


def connect_mongo(max_retries: int = 30) -> MongoClient:
    """Connect to MongoDB with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            uri = (
                f"mongodb://{MONGO_CONFIG['user']}:{MONGO_CONFIG['password']}"
                f"@{MONGO_CONFIG['host']}:{MONGO_CONFIG['port']}"
            )
            client = MongoClient(uri)
            client.admin.command("ping")
            print("✅ Connected to MongoDB")
            return client
        except Exception as e:
            print(f"⏳ Waiting for MongoDB (attempt {attempt}): {e}")
            time.sleep(2)
    raise ConnectionError("Could not connect to MongoDB")


def create_consumer(max_retries: int = 30) -> KafkaConsumer:
    """Create Kafka consumer with retry logic."""
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="order-consumer-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            print(f"✅ Connected to Kafka, subscribed to '{KAFKA_TOPIC}'")
            return consumer
        except Exception as e:
            print(f"⏳ Waiting for Kafka (attempt {attempt}): {e}")
            time.sleep(2)
    raise ConnectionError("Could not connect to Kafka")


def save_to_mongo(collection, order: dict) -> None:
    """Save the raw order JSON to MongoDB."""
    collection.insert_one(order.copy())


def save_to_postgres(cursor, order: dict) -> None:
    """Insert the raw JSON into the raw_orders staging table,
    then insert a structured row into the fact_orders table."""

    # 1) Raw staging table — store the full JSON payload
    cursor.execute(
        "INSERT INTO raw_orders (payload) VALUES (%s)",
        (json.dumps(order),),
    )

    # 2) Structured fact table — insert the parsed fields
    cursor.execute(
        """
        INSERT INTO fact_orders
            (customer_id, restaurant_id, item_id, quantity,
             total_price, status, order_timestamp, delivery_city)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            order["customer_id"],
            order["restaurant_id"],
            order["item_id"],
            order["quantity"],
            order["total_price"],
            order["status"],
            order["order_timestamp"],
            order["delivery_city"],
        ),
    )


def main():
    """Main loop: consume from Kafka and write to both databases."""
    pg_conn = connect_postgres()
    pg_cursor = pg_conn.cursor()
    mongo_client = connect_mongo()
    mongo_db = mongo_client["food_orders"]
    mongo_collection = mongo_db["raw_events"]
    consumer = create_consumer()

    order_count = 0

    print("🚀 Consumer started — waiting for orders...")

    try:
        for message in consumer:
            order = message.value
            order_count += 1

            # Write to both data stores
            save_to_mongo(mongo_collection, order)
            save_to_postgres(pg_cursor, order)

            print(
                f"💾 Order #{order_count} saved: "
                f"{order['customer_name']} — {order['item_name']} "
                f"→ Postgres ✓ | Mongo ✓"
            )

    except KeyboardInterrupt:
        print(f"\n🛑 Stopped after processing {order_count} orders")
    finally:
        pg_cursor.close()
        pg_conn.close()
        mongo_client.close()
        consumer.close()


if __name__ == "__main__":
    main()
