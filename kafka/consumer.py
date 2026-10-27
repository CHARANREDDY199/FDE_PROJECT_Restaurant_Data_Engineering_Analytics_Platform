"""
Consumes restaurant order events from Kafka and inserts them into PostgreSQL.
Measures database insertion and commit time for each event.
"""

import json
import os
import time

import psycopg2
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9093"
TOPIC = "restaurant_orders"

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DB", "restaurant_analytics"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def insert_event(conn, event):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO orders
                (order_id, customer_id, order_time, order_type, status, total_amount)
            VALUES (%s, %s, %s, %s, 'placed', 0)
            ON CONFLICT (order_id) DO NOTHING
            """,
            (
                event["order_id"],
                event["customer_id"],
                event["order_time"],
                event["order_type"],
            ),
        )

        cur.execute(
            """
            INSERT INTO order_items
                (order_id, menu_item_id, quantity, unit_price)
            SELECT %s, %s, %s, price
            FROM menu
            WHERE menu_item_id = %s
            """,
            (
                event["order_id"],
                event["menu_item_id"],
                event["quantity"],
                event["menu_item_id"],
            ),
        )

    conn.commit()


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="restaurant-etl-consumer",
    )

    conn = get_connection()

    print("Listening for order events...")

    try:
        for message in consumer:
            event = message.value
            start_time = time.perf_counter()

            try:
                insert_event(conn, event)

                elapsed_ms = (
                    time.perf_counter() - start_time
                ) * 1000

                print(
                    f"Order ID: {event['order_id']} | "
                    f"DB insertion and commit time: {elapsed_ms:.2f} ms"
                )

            except Exception as exc:
                conn.rollback()
                print(
                    f"Failed to insert order "
                    f"{event.get('order_id')}: {exc}"
                )

    except KeyboardInterrupt:
        print("\nConsumer stopped.")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()