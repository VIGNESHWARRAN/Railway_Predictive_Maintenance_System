"""
sensor_streamer.py
Reads MetroPT-3 CSV and streams rows into Kafka topic: railway_sensor_stream
Simulates real-time IIoT sensor data at configurable replay speed.
"""

import csv
import json
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BROKER     = "localhost:9092"
TOPIC            = "railway_sensor_stream"
DEFAULT_CSV_PATH = "../../dataset/MetroPT3(AirCompressor).csv"

# Analogue sensor columns (float)
ANALOGUE_COLS = ["TP2", "TP3", "H1", "DV_pressure",
                 "Reservoirs", "Oil_temperature", "Motor_current"]

# Digital sensor columns (int 0/1)
DIGITAL_COLS  = ["COMP", "DV_eletric", "Towers", "MPG",
                 "LPS", "Pressure_switch", "Oil_level", "Caudal_impulses"]


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )


def parse_row(row: dict) -> dict:
    """Cast CSV string values to correct numeric types."""
    record = {}
    record["timestamp"] = row.get("timestamp", datetime.utcnow().isoformat())
    for col in ANALOGUE_COLS:
        try:
            record[col] = float(row[col])
        except (KeyError, ValueError):
            record[col] = None
    for col in DIGITAL_COLS:
        try:
            record[col] = int(float(row[col]))
        except (KeyError, ValueError):
            record[col] = None
    return record


def stream(csv_path: str, delay_seconds: float = 0.1, limit: int = None):
    producer = build_producer()
    csv_file = Path(csv_path)

    if not csv_file.exists():
        logger.error(f"CSV not found: {csv_path}")
        return

    logger.info(f"Streaming {csv_path} → topic '{TOPIC}' at {1/delay_seconds:.1f} rows/sec")
    sent = 0

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = parse_row(row)
            producer.send(TOPIC, value=record)
            sent += 1

            if sent % 1000 == 0:
                logger.info(f"  Sent {sent:,} records...")

            if limit and sent >= limit:
                break

            time.sleep(delay_seconds)

    producer.flush()
    logger.info(f"Done. Total records sent: {sent:,}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MetroPT-3 Kafka Producer")
    parser.add_argument("--csv",   default=DEFAULT_CSV_PATH, help="Path to MetroPT-3 CSV")
    parser.add_argument("--delay", type=float, default=0.1,  help="Seconds between records (default 0.1)")
    parser.add_argument("--limit", type=int,   default=None, help="Max rows to send (default: all)")
    args = parser.parse_args()

    stream(args.csv, args.delay, args.limit)
