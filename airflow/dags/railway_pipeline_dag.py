"""
railway_pipeline_dag.py
Orchestrates the full MetroPT-3 ingestion + ML pipeline every 15 minutes.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import subprocess
import logging

logger = logging.getLogger(__name__)

# ── DAG defaults ───────────────────────────────────────────────────────────────
default_args = {
    "owner":            "railway-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
}

dag = DAG(
    dag_id="railway_metropt3_pipeline",
    default_args=default_args,
    description="MetroPT-3 sensor ingestion + anomaly detection pipeline",
    schedule_interval=timedelta(minutes=15),
    start_date=days_ago(1),
    catchup=False,
    tags=["railway", "anomaly-detection", "metropt3"],
)

# ── Task 1: Start Kafka producer (streams 500 rows per run as a batch) ─────────
start_stream = BashOperator(
    task_id="start_sensor_stream",
    bash_command=(
        "python /opt/railway/kafka/producer/sensor_streamer.py "
        "--delay 0.01 --limit 500"
    ),
    dag=dag,
)

# ── Task 2: Verify new records landed in MongoDB ───────────────────────────────
def verify_ingestion(**context):
    client = MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=5000)
    db = client["railway_db"]
    count = db["sensor_data"].estimated_document_count()
    logger.info(f"sensor_data total records: {count:,}")
    if count == 0:
        raise ValueError("No records found in sensor_data — ingestion may have failed.")
    context["ti"].xcom_push(key="record_count", value=count)

verify = PythonOperator(
    task_id="verify_ingestion",
    python_callable=verify_ingestion,
    dag=dag,
)

# ── Task 3: Submit Spark anomaly detection job ─────────────────────────────────
spark_job = BashOperator(
    task_id="spark_ml_task",
    bash_command=(
        "spark-submit "
        "--class railway.RailwayAnomalyDetector "
        "--master spark://localhost:7077 "
        "--packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 "
        "/opt/railway/spark-ml/target/scala-2.12/railway-anomaly-detector-assembly-1.0.jar"
    ),
    dag=dag,
    execution_timeout=timedelta(minutes=10),
)

# ── Task 4: Alert on new anomalies ────────────────────────────────────────────
def alert_on_anomaly(**context):
    from datetime import timezone
    client = MongoClient("mongodb://localhost:27017")
    db = client["railway_db"]

    fifteen_mins_ago = (
        datetime.now(timezone.utc) - timedelta(minutes=15)
    ).isoformat()

    recent_anomalies = list(db["predictions"].find({
        "is_anomaly": True,
        "processed_at": {"$gte": fifteen_mins_ago}
    }))

    if recent_anomalies:
        logger.warning(f"⚠️  {len(recent_anomalies)} ANOMALIES detected in last 15 minutes!")
        for a in recent_anomalies[:5]:
            logger.warning(
                f"  → ts={a.get('timestamp')} | score={a.get('anomaly_score', 0):.3f} | hint={a.get('fault_hint')}"
            )
        # TODO: integrate Slack/Twilio/SMS here
    else:
        logger.info("✅ No anomalies in last 15 minutes.")

    return len(recent_anomalies)

alert = PythonOperator(
    task_id="alert_on_fault",
    python_callable=alert_on_anomaly,
    dag=dag,
)

# ── DAG dependency chain ───────────────────────────────────────────────────────
start_stream >> verify >> spark_job >> alert
