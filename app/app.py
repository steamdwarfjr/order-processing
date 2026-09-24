import json
import os
import time
import pymysql
import pika
from flask import Flask, request, jsonify
from confluent_kafka import Producer

app = Flask(__name__)

MYSQL_HOST = os.getenv("MYSQL_HOST", "mysql")
MYSQL_USER = os.getenv("MYSQL_USER", "app")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "apppass")
MYSQL_DB   = os.getenv("MYSQL_DATABASE", "orders_db")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = "order-events"

RABBIT_HOST  = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_USER  = os.getenv("RABBIT_USER", "guest")
RABBIT_PASS  = os.getenv("RABBIT_PASS", "guest")
RABBIT_QUEUE = "order-notifications"

_producer = None


def get_producer():
    """Ленивая инициализация продюсера: подключаемся в момент первого запроса,
    а не при импорте модуля — иначе контейнер упадёт, если Kafka ещё не готова."""
    global _producer
    if _producer is None:
        _producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",       # ждать подтверждения от всех реплик
            "linger.ms": 5,      # копить сообщения 5 мс перед отправкой пачкой
        })
    return _producer


def delivery_report(err, msg):
    """Callback: Kafka вызовет эту функцию, когда сообщение реально записано
    или когда произошла ошибка. Без неё мы не узнаем о сбое доставки."""
    if err is not None:
        print(f"[kafka] delivery failed: {err}", flush=True)
    else:
        print(
            f"[kafka] delivered to {msg.topic()} "
            f"partition={msg.partition()} offset={msg.offset()}",
            flush=True,
        )


def db_connect():
    return pymysql.connect(
        host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASS,
        database=MYSQL_DB, autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def rabbit_publish(message: dict):
    creds = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    conn = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBIT_HOST, credentials=creds)
    )
    try:
        ch = conn.channel()
        ch.queue_declare(queue=RABBIT_QUEUE, durable=True)
        ch.basic_publish(
            exchange="",
            routing_key=RABBIT_QUEUE,
            body=json.dumps(message).encode(),
            properties=pika.BasicProperties(delivery_mode=2),
        )
    finally:
        conn.close()


@app.route("/api/orders", methods=["POST"])
def create_order():
    data = request.get_json(force=True, silent=True) or {}
    customer = (data.get("customer") or "").strip()
    item     = (data.get("item") or "").strip()
    amount   = data.get("amount")

    if not customer or not item or amount is None:
        return jsonify({"error": "customer, item и amount обязательны"}), 400
    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return jsonify({"error": "amount должен быть числом"}), 400

    # 1) MySQL
    conn = db_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (customer, item, amount, status) VALUES (%s,%s,%s,%s)",
                (customer, item, amount, "NEW"),
            )
            order_id = cur.lastrowid
    finally:
        conn.close()

    event = {
        "order_id": order_id,
        "customer": customer,
        "item":     item,
        "amount":   amount,
        "ts":       time.time(),
    }

    # 2) Kafka через confluent-kafka
    producer = get_producer()
    producer.produce(
        topic=KAFKA_TOPIC,
        key=str(order_id).encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )
    # produce() асинхронный: сообщение кладётся во внутренний буфер.
    # flush() блокируется, пока всё из буфера не уйдёт брокеру (или не истечёт таймаут).
    remaining = producer.flush(timeout=10)
    if remaining != 0:
        return jsonify({"error": "Kafka: сообщение не доставлено"}), 500

    # 3) RabbitMQ
    rabbit_publish({
        "order_id": order_id,
        "message": f"Новый заказ #{order_id} от {customer} на {amount} ₽",
    })

    return jsonify({"order_id": order_id, "status": "accepted"}), 201


@app.route("/api/health")
def health():
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)