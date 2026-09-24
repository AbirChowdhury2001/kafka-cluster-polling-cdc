import random
import time
import logging

import mysql.connector

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [generator] %(levelname)s %(message)s",
)
log = logging.getLogger("generator")

CUSTOMERS = ["Abir", "The Weeknd", "Tame Impala", "Tory Lanez", "Ariana Grande", "Travis Scott", "Daft Punk", "Spiderman"]
PRODUCTS = ["Laptop", "Headphones", "Keyboard", "Monitor", "Webcam", "Mouse", "SSD"]
STATUS_FLOW = {"PENDING": "SHIPPED", "SHIPPED": "DELIVERED"}


def insert_order(cursor):
    customer = random.choice(CUSTOMERS)
    product = random.choice(PRODUCTS)
    amount = round(random.uniform(15, 900), 2)
    cursor.execute(
        "INSERT INTO orders (customer_name, product, amount, status) VALUES (%s, %s, %s, 'PENDING')",
        (customer, product, amount),
    )
    log.info("Inserted new order: %s bought %s ($%.2f)", customer, product, amount)


def advance_a_random_order(cursor):
    cursor.execute(
        "SELECT order_id, status FROM orders WHERE status IN ('PENDING','SHIPPED') ORDER BY RAND() LIMIT 1"
    )
    row = cursor.fetchone()
    if not row:
        return
    order_id, status = row
    next_status = STATUS_FLOW.get(status)
    if not next_status:
        return
    cursor.execute("UPDATE orders SET status = %s WHERE order_id = %s", (next_status, order_id))
    log.info("Order %s: %s -> %s", order_id, status, next_status)


def main():
    conn = mysql.connector.connect(**config.GENERATOR_DB)
    conn.autocommit = True
    cursor = conn.cursor()

    log.info("Generating simulated order activity every %ds. Ctrl+C to stop.",
              config.GENERATE_INTERVAL_SECONDS)

    try:
        while True:
            if random.random() < 0.6:
                insert_order(cursor)
            else:
                advance_a_random_order(cursor)
            time.sleep(config.GENERATE_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        log.info("Stopped.")
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
