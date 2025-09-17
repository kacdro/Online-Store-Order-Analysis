# producer_fixed.py
import json
import random
import time
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "orders"
BOOTSTRAP = "localhost:9092"  # jeśli uruchamiasz w kontenerze: "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
    acks="all",
)

PRODUCTS = [1001, 1002, 1003, 1004, 1005]

# Losujemy raz cenę dla każdego produktu i trzymamy w słowniku
PRODUCT_PRICES = {pid: round(random.uniform(5.0, 200.0), 2) for pid in PRODUCTS}

def make_order():
    product = random.choice(PRODUCTS)
    return {
        "order_id": str(uuid.uuid4()),
        "product_id": product,
        "price": PRODUCT_PRICES[product],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

if __name__ == "__main__":
    print("Product prices:", PRODUCT_PRICES)
    print("Sending orders to Kafka topic:", TOPIC)
    try:
        while True:
            order = make_order()
            # używamy key = product_id żeby zamapować wiadomości z tym samym produktem do tej samej partycji
            producer.send(TOPIC, key=order["product_id"], value=order)
            # opcjonalnie block until ack (synchronous) -> avoid for high throughput
            # producer.flush()
            print(order)
            time.sleep(random.uniform(0.2, 1.0))
    except KeyboardInterrupt:
        print("Stopping producer, flushing...")
        producer.flush()
        producer.close()

