"""
load_dataset_to_mongo.py

Loads MetroPT-3 CSV directly into MongoDB → railway_db.raw_sensor_data
Run this ONCE before starting the pipeline.

Usage:
    python scripts/load_dataset_to_mongo.py
    python scripts/load_dataset_to_mongo.py --csv dataset/MetroPT3.csv --batch 2000
"""

import csv
import logging
import argparse
from datetime import datetime, timezone
from pathlib import Path
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [LOADER] %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

MONGO_URI      = "mongodb://localhost:27017"
DB_NAME        = "railway_db"
COLLECTION     = "raw_sensor_data"
DEFAULT_CSV    = "dataset/MetroPT3(AirCompressor).csv"
DEFAULT_BATCH  = 5000

ANALOGUE_COLS = ["TP2", "TP3", "H1", "DV_pressure",
                 "Reservoirs", "Oil_temperature", "Motor_current"]
DIGITAL_COLS  = ["COMP", "DV_eletric", "Towers", "MPG",
                 "LPS", "Pressure_switch", "Oil_level", "Caudal_impulses"]


def parse_row(row: dict, loaded_at: str) -> dict | None:
    record = {}
    try:
        record["timestamp"] = row["timestamp"].strip()
    except KeyError:
        record["timestamp"] = row.get("Timestamp", "").strip()

    if not record["timestamp"]:
        return None

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

    record["_loaded_at"] = loaded_at
    return record


def load(csv_path: str, batch_size: int = DEFAULT_BATCH, drop_existing: bool = False):
    client     = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db         = client[DB_NAME]
    collection = db[COLLECTION]

    client.admin.command("ping")
    logger.info(f"Connected to MongoDB → {DB_NAME}.{COLLECTION}")

    if drop_existing:
        count = collection.count_documents({})
        logger.warning(f"Dropping {count:,} existing records from {COLLECTION}...")
        collection.drop()

    # Skip if already loaded
    existing = collection.estimated_document_count()
    if existing > 0 and not drop_existing:
        logger.info(f"Collection already has {existing:,} records. Use --drop to reload.")
        return

    # NOTE: indexes already created by mongo-init.js — no need to create them here

    csv_file = Path(csv_path)
    if not csv_file.exists():
        logger.error(f"CSV not found: {csv_path}")
        logger.error("Run: python scripts/download_dataset.py")
        return

    loaded_at = datetime.now(timezone.utc).isoformat()
    batch     = []
    total     = 0
    skipped   = 0

    logger.info(f"Loading {csv_file.name} → MongoDB...")

    with open(csv_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = parse_row(row, loaded_at)
            if record is None:
                skipped += 1
                continue

            batch.append(record)

            if len(batch) >= batch_size:
                try:
                    collection.insert_many(batch, ordered=False)
                    total += len(batch)
                    logger.info(f"  Inserted {total:,} records...")
                except BulkWriteError as bwe:
                    inserted = bwe.details.get("nInserted", 0)
                    total += inserted
                    logger.warning(f"  Partial insert: {inserted} of {len(batch)}")
                batch = []

    # Insert remaining
    if batch:
        try:
            collection.insert_many(batch, ordered=False)
            total += len(batch)
        except BulkWriteError as bwe:
            total += bwe.details.get("nInserted", 0)

    logger.info(f"\n{'='*50}")
    logger.info(f"✅ Load complete!")
    logger.info(f"   Total inserted : {total:,}")
    logger.info(f"   Skipped rows   : {skipped}")
    logger.info(f"   Collection     : {DB_NAME}.{COLLECTION}")
    logger.info(f"   Verify with    : python scripts/verify_mongo.py")
    logger.info(f"{'='*50}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load MetroPT-3 CSV into MongoDB")
    parser.add_argument("--csv",   default=DEFAULT_CSV, help="Path to MetroPT-3 CSV file")
    parser.add_argument("--batch", type=int, default=DEFAULT_BATCH, help="Batch insert size")
    parser.add_argument("--drop",  action="store_true", help="Drop existing collection before loading")
    args = parser.parse_args()

    load(args.csv, args.batch, args.drop)