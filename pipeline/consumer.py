import json
import logging

import mysql.connector
from confluent_kafka import Consumer

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [consumer] %(levelname)s %(message)s",
)
log = logging.getLogger("consumer")

UPSERT_SQL = """
INSERT INTO orders (order_id, customer_name, product, amount, status, created_at, updated_at)
VALUES (%(order_id)s, %(customer_name)s, %(product)s, %(amount)s, %(status)s, %(created_at)s, %(updated_at)s)
ON DUPLICATE KEY UPDATE
    customer_name = VALUES(customer_name),
    product       = VALUES(product),
    amount        = VALUES(amount),
    status        = VALUES(status),
    updated_at    = VALUES(updated_at)
"""


def main():
    consumer_config = dict(config.KAFKA_CONFIG)
    consumer_config.update({
        "group.id": config.CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer = Consumer(consumer_config)
    consumer.subscribe([config.TOPIC])

    conn = mysql.connector.connect(**config.TARGET_DB)
    cursor = conn.cursor()

    log.info("Listening on topic '%s' as group '%s'...", config.TOPIC, config.CONSUMER_GROUP)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("Kafka error: %s", msg.error())
                continue

            event = json.loads(msg.value())
            try:
                cursor.execute(UPSERT_SQL, event)
                conn.commit()
                consumer.commit(msg)
                log.info("Upserted order_id=%s (status=%s) into target_db.orders",
                         event["order_id"], event["status"])
            except mysql.connector.Error as db_err:
                conn.rollback()
                log.error("DB write failed for order_id=%s, will retry: %s",
                          event.get("order_id"), db_err)

    except KeyboardInterrupt:
        log.info("Shutting down consumer.")
    finally:
        cursor.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
