import json
import os
import time
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "order-events"
GROUP = "delivery-group"


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])
    print(f"[delivery] started, group={GROUP}, topic={TOPIC}", flush=True)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[delivery] kafka error: {msg.error()}", flush=True)
                continue

            ev = json.loads(msg.value().decode("utf-8"))
            key = msg.key().decode("utf-8") if msg.key() else None

            print(
                f"[delivery] part={msg.partition()} offset={msg.offset()} "
                f"key={key} → готовим доставку заказа {ev['order_id']} ({ev['item']})",
                flush=True,
            )
            time.sleep(1)
            print(f"[delivery] order {ev['order_id']} SHIPPED", flush=True)

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()