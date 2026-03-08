"""
verify_mongo.py
Quick check to confirm both collections have data and show sample records.

Usage:
    python scripts/verify_mongo.py
"""

from pymongo import MongoClient
from datetime import datetime

MONGO_URI = "mongodb://localhost:27017"
DB_NAME   = "railway_db"

def verify():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db     = client[DB_NAME]

    print(f"\n{'='*60}")
    print(f"  MongoDB Verification — {DB_NAME}")
    print(f"{'='*60}\n")

    collections = {
        "raw_sensor_data":       "📥 RAW DATASET (input)",
        "processed_predictions": "📤 ML PREDICTIONS (output)",
        "spark_run_log":         "📋 SPARK RUN LOG",
    }

    for col_name, label in collections.items():
        col   = db[col_name]
        count = col.estimated_document_count()
        print(f"{label}")
        print(f"  Collection : {col_name}")
        print(f"  Records    : {count:,}")

        if count > 0:
            # Show one sample
            sample = col.find_one({}, {"_id": 0})
            print(f"  Sample keys: {list(sample.keys())}")

            if col_name == "raw_sensor_data":
                first = col.find_one({}, {"timestamp": 1, "_id": 0}, sort=[("timestamp", 1)])
                last  = col.find_one({}, {"timestamp": 1, "_id": 0}, sort=[("timestamp", -1)])
                print(f"  Date range : {first.get('timestamp')} → {last.get('timestamp')}")

            if col_name == "processed_predictions":
                anomaly_count = col.count_documents({"is_anomaly": True})
                print(f"  Anomalies  : {anomaly_count:,} ({100*anomaly_count/count:.2f}%)")

                # Fault breakdown
                pipeline = [
                    {"$match": {"is_anomaly": True}},
                    {"$group": {"_id": "$fault_hint", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
                faults = list(col.aggregate(pipeline))
                if faults:
                    print(f"  Fault types:")
                    for f in faults:
                        print(f"    {f['_id']:35s} : {f['count']:,}")
        print()

    print(f"{'='*60}\n")

if __name__ == "__main__":
    verify()
