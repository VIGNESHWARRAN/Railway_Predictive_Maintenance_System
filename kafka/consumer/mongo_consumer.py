"""
mongo_consumer.py
Consumes from Kafka topic railway_sensor_stream and writes raw records to MongoDB.
Collection: railway_db.sensor_data
"""

import json
import logging
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BROKER   = "localhost:9092"
TOPIC          = "railway_sensor_stream"
GROUP_ID       = "metropt_mongo_writer"

MONGO_URI      = "mongodb://localhost:27017"
MONGO_DB       = "railway_db"
COLLECTION     = "sensor_data"

BATCH_SIZE     = 500   # Insert in bulk for efficiency


def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]
    col    = db[COLLECTION]
    # Index on timestamp for fast range queries
    col.create_index([("timestamp", ASCENDING)], background=True)
    return col


def run():
    collection = get_mongo_collection()
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    logger.info(f"Consuming from topic '{TOPIC}' → MongoDB '{COLLECTION}'")
    batch = []
    total = 0

    for message in consumer:
        record = message.value
        record["ingested_at"] = datetime.utcnow().isoformat()
        batch.append(record)

        if len(batch) >= BATCH_SIZE:
            try:
                collection.insert_many(batch, ordered=False)
                total += len(batch)
                logger.info(f"  Inserted batch — total: {total:,}")
            except BulkWriteError as bwe:
                logger.warning(f"Bulk write partial error: {bwe.details}")
            batch = []


if __name__ == "__main__":
    run()
