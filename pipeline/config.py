SOURCE_DB = {
    "host": "127.0.0.1",
    "user": "pipeline_reader",
    "password": "*******",
    "database": "source_db",
}

GENERATOR_DB = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "*******",
    "database": "source_db",
}

TARGET_DB = {
    "host": "127.0.0.1",
    "user": "pipeline_writer",
    "password": "*******",
    "database": "target_db",
}

KAFKA_CONFIG = {
    "bootstrap.servers": "192.168.68.111:9094,192.168.68.112:9094,192.168.68.113:9094",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "orders-pipeline-user",
    "sasl.password": "*******",
    "ssl.ca.location": "./ca.crt",
    "ssl.endpoint.identification.algorithm": "none",
}

TOPIC = "orders-cdc"
CONSUMER_GROUP = "orders-consumer-group"
POLL_INTERVAL_SECONDS = 5
GENERATE_INTERVAL_SECONDS = 4
WATERMARK_FILE = "producer_watermark.txt"
