import json
import time
import logging
from datetime import datetime
from decimal import Decimal

import mysql.connector
from confluent_kafka import Producer

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
)
log = logging.getLogger("producer")


def load_watermark() -> str:
    try:
        with open(config.WATERMARK_FILE) as f:
            return f.read().strip()
    except FileNotFoundError:
        return "1970-01-01 00:00:00"


def save_watermark(value) -> None:
    with open(config.WATERMARK_FILE, "w") as f:
        f.write(str(value))


def row_to_event(row: dict) -> dict:
    event = dict(row)
    for key in ("created_at", "updated_at"):
        if isinstance(event.get(key), datetime):
            event[key] = event[key].isoformat()
    if isinstance(event.get("amount"), Decimal):
        event["amount"] = float(event["amount"])
    return event


def delivery_report(err, msg):
    if err is not None:
        log.error("Delivery failed for order_id key=%s: %s", msg.key(), err)
    else:
        log.info(
            "Delivered order_id=%s to %s[partition %d @ offset %d]",
            msg.key().decode(), msg.topic(), msg.partition(), msg.offset(),
        )


def main():
    producer = Producer(config.KAFKA_CONFIG)

    watermark = load_watermark()
    log.info("Starting from watermark updated_at > %s", watermark)

    conn = mysql.connector.connect(**config.SOURCE_DB)
    conn.autocommit = True

    try:
        while True:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(
                """
                SELECT order_id, customer_name, product, amount, status,
                       created_at, updated_at
                FROM orders
                WHERE updated_at > %s
                ORDER BY updated_at ASC, order_id ASC
                """,
                (watermark,),
            )
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                log.info("No new/changed rows.")
            else:
                for row in rows:
                    event = row_to_event(row)
                    producer.produce(
                        topic=config.TOPIC,
                        key=str(event["order_id"]),
                        value=json.dumps(event),
                        callback=delivery_report,
                    )
                    watermark = row["updated_at"]

                producer.flush()
                save_watermark(watermark)
                log.info("Published %d event(s). Watermark now %s", len(rows), watermark)

            time.sleep(config.POLL_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        log.info("Shutting down producer.")
    finally:
        producer.flush()
        conn.close()


if __name__ == "__main__":
    main()
