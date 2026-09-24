import json
import os
import time
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "order-events"
GROUP = "payment-group"


def build_consumer():
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",   # при первом запуске — читать с начала
        "enable.auto.commit": True,        # Kafka сама коммитит offset
    })


def main():
    consumer = build_consumer()
    consumer.subscribe([TOPIC])
    print(f"[payment] started, group={GROUP}, topic={TOPIC}", flush=True)

    try:
        while True:
            # poll ждёт сообщение до 1 секунды. Если ничего — вернёт None,
            # и мы просто пойдём на следующую итерацию.
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                # Часть ошибок — не фатальные (например, EOF партиции).
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[payment] kafka error: {msg.error()}", flush=True)
                continue

            # msg.value() и msg.key() — МЕТОДЫ, возвращают bytes.
            ev = json.loads(msg.value().decode("utf-8"))
            key = msg.key().decode("utf-8") if msg.key() else None

            print(
                f"[payment] part={msg.partition()} offset={msg.offset()} "
                f"key={key} order_id={ev['order_id']} amount={ev['amount']}",
                flush=True,
            )

            time.sleep(1)   # имитация обработки оплаты
            print(f"[payment] order {ev['order_id']} PAID", flush=True)

    except KeyboardInterrupt:
        # Ctrl+C — вежливо выходим
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()