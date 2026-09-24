import json
import os
import time
import pika

HOST  = os.getenv("RABBIT_HOST", "rabbitmq")
USER  = os.getenv("RABBIT_USER", "guest")
PASS  = os.getenv("RABBIT_PASS", "guest")
QUEUE = "order-notifications"

def connect_with_retry():
    creds = pika.PlainCredentials(USER, PASS)
    while True:
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host=HOST, credentials=creds, heartbeat=30)
            )
        except Exception as e:
            print(f"[notify] waiting for rabbitmq... {e}", flush=True)
            time.sleep(3)

def main():
    conn = connect_with_retry()
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE, durable=True)
    ch.basic_qos(prefetch_count=1)

    def on_message(ch, method, props, body):
        try:
            payload = json.loads(body.decode())
            print(f"[notify] {payload['message']}", flush=True)
            time.sleep(1)   # имитация отправки уведомления
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"[notify] error: {e}", flush=True)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    ch.basic_consume(queue=QUEUE, on_message_callback=on_message)
    print(f"[notify] waiting for messages in '{QUEUE}'", flush=True)
    ch.start_consuming()

if __name__ == "__main__":
    main()