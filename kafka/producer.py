"""
Simulates real-time restaurant order events and publishes them to a
Kafka topic ('restaurant_orders'). Run alongside consumer.py to
demonstrate the real-time ingestion layer.
"""

import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9093"
TOPIC = "restaurant_orders"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

ORDER_TYPES = ["dine_in", "takeaway", "delivery"]


def make_event(order_id):
    return {
        "order_id": order_id,
        "customer_id": random.randint(1, 500),
        "menu_item_id": random.randint(1, 40),
        "quantity": random.randint(1, 3),
        "order_type": random.choice(ORDER_TYPES),
        "order_time": datetime.utcnow().isoformat(),
    }


def main(n_events=1000, delay_seconds=1.0):
    order_id = 100000
    for _ in range(n_events):
        event = make_event(order_id)
        producer.send(TOPIC, value=event)
        print("Produced:", event)
        order_id += 1
        time.sleep(delay_seconds)
    producer.flush()


if __name__ == "__main__":
    main()
