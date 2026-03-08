# 🚆 Railway APU Fault Detection Pipeline
### MetroPT-3 Dataset · Metro do Porto · Real-world IIoT Sensor Data

---

## 📖 What This Project Does

This project builds a **complete end-to-end data engineering and machine learning pipeline** that monitors the **Air Production Unit (APU)** of metro trains in real time. The APU is a critical compressor system — if it fails, the train's pneumatic braking system is at risk.

The pipeline does the following automatically:

1. **Streams** real sensor readings (pressure, temperature, motor current) from a CSV into Kafka — simulating live IIoT data
2. **Ingests** those readings into MongoDB (`raw_sensor_data` collection) for storage
3. **Processes** the raw data with Apache Spark — engineers features and runs an anomaly detection ML model
4. **Writes** predictions back to MongoDB (`processed_predictions` collection) with anomaly scores and fault labels
5. **Displays** everything on a live Streamlit dashboard — alerts, trends, fault breakdowns
6. **Orchestrates** the full workflow automatically every 15 minutes via Apache Airflow

---

## 🏗️ System Architecture

```
MetroPT-3 CSV
     │
     ▼
[Kafka Producer] ──► [Kafka Topic: railway_sensor_stream]
                                    │
                                    ▼
                           [Kafka Consumer]
                                    │
                                    ▼
                    MongoDB: railway_db.raw_sensor_data     ◄── INPUT
                                    │
                                    ▼
                          [Apache Spark ML Job]
                        ┌───────────────────────┐
                        │  Feature Engineering  │
                        │  KMeans Clustering    │
                        │  Anomaly Scoring      │
                        │  Rule-based Labeling  │
                        └───────────────────────┘
                                    │
                                    ▼
                 MongoDB: railway_db.processed_predictions  ◄── OUTPUT
                                    │
                                    ▼
                       [Streamlit Dashboard :8501]

[Airflow DAG] ──── orchestrates everything every 15 min
```

---

## 📦 Dataset: MetroPT-3

Real sensor data from Metro do Porto, Portugal. Collected from the APU compressor onboard metro trains.

| Property | Details |
|----------|---------|
| Source | Metro do Porto, Portugal |
| UCI Repository | https://archive.ics.uci.edu/dataset/791/metropt+3+dataset |
| Kaggle | https://www.kaggle.com/datasets/anshtanwar/metro-train-dataset |
| Size | 1,516,948 rows |
| Sampling Rate | 0.1 Hz — one reading every 10 seconds |
| Features | 15 sensors (7 analogue + 8 digital) |
| Labels | Unlabeled — 6 failure events from maintenance logs |
| License | CC BY 4.0 |

### Analogue Sensors (continuous values)

| Column | What it measures |
|--------|-----------------|
| `TP2` | Compressor pressure (bar) |
| `TP3` | Pneumatic panel pressure (bar) |
| `H1` | Discharge valve pressure (bar) |
| `DV_pressure` | Pressure differential at towers valve (bar) |
| `Reservoirs` | Air tank pressure (bar) |
| `Oil_temperature` | Compressor oil temperature (°C) |
| `Motor_current` | Motor current draw (A) |

### Digital Sensors (binary 0/1)

| Column | What it measures |
|--------|-----------------|
| `COMP` | Compressor ON/OFF |
| `DV_eletric` | Electrical discharge valve |
| `Towers` | Air drying tower status |
| `MPG` | Manual pressure gauge |
| `LPS` | Low pressure switch |
| `Pressure_switch` | Pressure switch |
| `Oil_level` | Oil level indicator |
| `Caudal_impulses` | Flow impulse counter |

---

## 🧠 Machine Learning: Why Unsupervised?

The dataset has **no row-level fault labels** — the only fault information is 6 timestamps from maintenance reports. This makes supervised models like GBT or Random Forest impossible to train reliably. Instead we use **unsupervised anomaly detection**:

### How it works (3 stages):

**Stage 1 — Feature Engineering (Spark)**
- Computes pressure differentials: `TP2 - TP3`, `H1 - DV_pressure`
- Computes `motor_oil_ratio` — current vs temperature
- Rolling 30-minute mean and standard deviation for `TP2` and `Motor_current`

**Stage 2 — KMeans Clustering (Anomaly Scoring)**
- Trains KMeans (k=8) on normal operating windows
- For every reading, computes **Euclidean distance from its nearest cluster centroid**
- Distance = anomaly score — readings far from any cluster are anomalous
- Threshold: `mean + 3σ` of all scores — anything above is flagged

**Stage 3 — Rule-based Fault Labeling**
- Assigns a human-readable fault hint based on which sensor crossed its threshold:

| Fault Label | Condition |
|-------------|-----------|
| `LOW_COMPRESSOR_PRESSURE` | TP2 < 8.0 bar |
| `HIGH_MOTOR_CURRENT` | Motor current > 25A |
| `OVERHEATING` | Oil temperature > 90°C |
| `VALVE_PRESSURE_DROP` | DV_pressure < 0.5 bar |
| `GENERAL_ANOMALY` | Score high but no specific threshold crossed |

---

## 🗄️ MongoDB Schema

**Database:** `railway_db`

### `raw_sensor_data` — INPUT collection
Stores every row from the MetroPT-3 CSV exactly as-is.
```
timestamp, TP2, TP3, H1, DV_pressure, Reservoirs,
Oil_temperature, Motor_current,
COMP, DV_eletric, Towers, MPG, LPS,
Pressure_switch, Oil_level, Caudal_impulses,
_loaded_at
```

### `processed_predictions` — OUTPUT collection
Spark ML writes one prediction record per input row.
```
source_timestamp, anomaly_score, is_anomaly, fault_hint,
cluster_id, threshold_used,
TP2, Motor_current, Oil_temperature, DV_pressure,
pressure_diff_TP2_TP3, motor_oil_ratio,
rolling_mean_TP2, rolling_mean_motor,
processed_at, spark_run_id
```

### `spark_run_log` — AUDIT collection
Tracks every Spark job execution for the Airflow audit trail.

---

## 🛠️ Technology Stack

| Technology | Version | Role |
|------------|---------|------|
| Apache Kafka | 7.4.0 (Confluent) | Real-time sensor event streaming |
| Apache Zookeeper | 7.4.0 | Kafka coordination |
| MongoDB | 6.0 | Raw data + predictions storage |
| Apache Spark | 3.5.1 | Distributed ML + feature engineering |
| Scala | 2.12 | Spark ML implementation |
| Python | 3.9+ | Kafka scripts, Airflow DAGs, Streamlit |
| Streamlit | 1.32 | Live monitoring dashboard |
| Apache Airflow | 2.7.0 | Pipeline orchestration |
| Docker | Latest | Containerized infrastructure |

---

## 📁 Project Structure

```
railway-failure-platform/
├── docker/
│   ├── docker-compose.yml            # All 6 service definitions
│   └── mongo-init.js                 # Auto-creates MongoDB collections + indexes
├── dataset/
│   └── MetroPT3(AirCompressor).csv   # Downloaded from Kaggle
├── kafka/
│   ├── producer/
│   │   └── sensor_streamer.py        # Reads CSV → streams to Kafka
│   └── consumer/
│       └── mongo_consumer.py         # Kafka → MongoDB raw_sensor_data
├── spark-ml/
│   ├── build.sbt                     # Scala dependencies
│   └── src/main/scala/railway/
│       └── RailwayAnomalyDetector.scala   # Feature eng + KMeans + scoring
├── streamlit-ui/
│   ├── app.py                        # Dashboard UI
│   └── requirements.txt
├── airflow/
│   └── dags/
│       └── railway_pipeline_dag.py   # 15-min orchestration DAG
└── scripts/
    ├── download_dataset.py           # Kaggle download helper
    ├── load_dataset_to_mongo.py      # CSV → MongoDB loader (run once)
    └── verify_mongo.py               # Check both collections have data
```

---

## 🚀 Complete Setup Guide (WSL2 + Docker Desktop on Windows)

### Prerequisites

Install these on your Windows machine before starting:

- **Docker Desktop** — https://www.docker.com/products/docker-desktop
  - After install: Settings → Resources → WSL Integration → Enable Ubuntu → Apply & Restart
- **WSL2 Ubuntu** — run `wsl --install` in PowerShell as Admin

Create `C:\Users\<YourName>\.wslconfig` to give WSL enough memory:
```ini
[wsl2]
memory=6GB
processors=4
swap=2GB
```
Then restart WSL: run `wsl --shutdown` in PowerShell, then reopen Ubuntu.

---

### Inside WSL Ubuntu — install dependencies

```bash
# System packages
sudo apt update && sudo apt upgrade -y
sudo apt install -y openjdk-11-jdk python3 python3-pip python3-venv curl

# Set JAVA_HOME
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

# sbt (Scala build tool)
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add -
sudo apt update && sudo apt install -y sbt

# Kaggle CLI
pip3 install kaggle
echo 'export PATH=$PATH:~/.local/bin' >> ~/.bashrc
source ~/.bashrc
```

---

### STEP 1 — Start Docker infrastructure

```bash
cd ~/railway-failure-platform/docker
docker compose up -d

# Verify all 6 containers are running
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected output:
```
NAMES           STATUS
airflow         Up
spark-worker    Up
spark-master    Up
kafka           Up
zookeeper       Up
mongodb         Up
```

Service URLs:

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | — |
| Spark Master UI | http://localhost:8083 | — |
| Spark Worker UI | http://localhost:8082 | — |
| Airflow | http://localhost:8081 | admin / admin |
| MongoDB | localhost:27017 | — |
| Kafka | localhost:9092 | — |

---

### STEP 2 — Set up Python environment

```bash
cd ~/railway-failure-platform
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r streamlit-ui/requirements.txt
```

---

### STEP 3 — Download the dataset

```bash
# Place your Kaggle API key first
mkdir -p ~/.kaggle
cp /mnt/c/Users/<YourWindowsUsername>/Downloads/kaggle.json ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json

# Download dataset
python scripts/download_dataset.py

# Verify
ls dataset/
# Should show: MetroPT3(AirCompressor).csv
```

---

### STEP 4 — Load CSV into MongoDB (run once)

```bash
python scripts/load_dataset_to_mongo.py
# Loads all 1,516,948 rows into railway_db.raw_sensor_data
# Takes ~3 minutes — you will see batch progress logs
```

Verify both collections:
```bash
python scripts/verify_mongo.py
```

---

### STEP 5 — Start the pipeline (open 3 terminals)

**Terminal 1 — Kafka Consumer** (writes Kafka stream → MongoDB):
```bash
cd ~/railway-failure-platform && source venv/bin/activate
python kafka/consumer/mongo_consumer.py
```

**Terminal 2 — Kafka Producer** (streams CSV → Kafka topic):
```bash
cd ~/railway-failure-platform && source venv/bin/activate
python kafka/producer/sensor_streamer.py \
  --csv "dataset/MetroPT3(AirCompressor).csv" \
  --delay 0.05
```

**Terminal 3 — Streamlit Dashboard**:
```bash
cd ~/railway-failure-platform && source venv/bin/activate
cd streamlit-ui && streamlit run app.py
```

Open **http://localhost:8501** in your Windows browser.
Uncheck **"Use demo data"** in the sidebar once the producer is sending records.

---

### STEP 6 — Build and run the Spark ML job

```bash
# Build JAR (one-time only, takes ~5 mins — downloads Scala dependencies)
cd ~/railway-failure-platform/spark-ml
sbt assembly

# Copy JAR into the Spark master container
docker cp \
  target/scala-2.12/railway-anomaly-detector-assembly-1.0.jar \
  spark-master:/opt/railway-detector.jar

# Submit the Spark job
docker exec -it spark-master \
  /opt/spark/bin/spark-submit \
    --class railway.RailwayAnomalyDetector \
    --master spark://spark-master:7077 \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
    /opt/railway-detector.jar
```

After this completes, `processed_predictions` is populated.
Run `python scripts/verify_mongo.py` to confirm.

---

### STEP 7 — Enable Airflow for automated runs

Open **http://localhost:8081** → login: `admin` / `admin`

- Find DAG: `railway_metropt3_pipeline`
- Toggle it **ON**
- Runs every 15 minutes automatically: producer → verify → Spark → alert

---

### Final state check

```bash
python scripts/verify_mongo.py
```

```
📥 RAW DATASET      → raw_sensor_data       : 1,516,948 records
📤 ML PREDICTIONS   → processed_predictions : 1,516,948 records
📋 SPARK RUN LOG    → spark_run_log         : 1 record
```

---

## 🔧 Useful Commands

```bash
# Restart a single container
docker compose restart kafka

# View container logs
docker logs mongodb --tail 50
docker logs spark-master --tail 50

# Stop everything
docker compose down

# Stop and wipe all data (fresh start)
docker compose down -v

# MongoDB shell — query data directly
docker exec -it mongodb mongosh
use railway_db
db.raw_sensor_data.countDocuments()
db.processed_predictions.find({ is_anomaly: true }).limit(5)
db.processed_predictions.countDocuments({ is_anomaly: true })

# Reactivate venv after opening a new terminal
source ~/railway-failure-platform/venv/bin/activate
```

---

## 🔥 Troubleshooting

| Problem | Fix |
|---------|-----|
| `docker: command not found` | Enable WSL Integration in Docker Desktop → Settings → Resources → WSL Integration |
| `permission denied /var/run/docker.sock` | Run `sudo usermod -aG docker $USER` then `wsl --shutdown` in PowerShell |
| Port already in use (e.g. 8080) | Change host port in docker-compose.yml e.g. `"8084:8080"` |
| `bitnami/spark` image not found | Use `apache/spark:3.5.1` — Bitnami removed all images from Docker Hub in Sept 2025 |
| Kafka connection refused | Wait 30 seconds after `docker compose up` before running producer/consumer |
| CSV not found error | Run `ls dataset/` and pass the exact filename using `--csv` flag |
| Index already exists error | `mongo-init.js` already created indexes — do not call `create_index` in Python scripts |
| WSL running out of memory | Add `.wslconfig` with `memory=6GB` at `C:\Users\<YourName>\.wslconfig` |
| Spark worker not connecting | Ensure `spark-master` container is fully started before worker tries to register |

---

## 📈 Future Improvements

- **LSTM Autoencoder** — temporal anomaly detection on pressure time-series sequences
- **Spark Structured Streaming** — sub-second latency replacing batch Spark jobs
- **Remaining Useful Life (RUL) Estimation** — regression model predicting time-to-failure
- **Slack / Twilio Alerts** — push notifications for on-call maintenance teams
- **GraphX Integration** — model compressor topology and fault propagation across sensors

---

## 📚 Citation

```
Veloso, B., Gama, J., Ribeiro, R.P., & Pereira, P.M. (2022).
MetroPT: A Benchmark Dataset for Predictive Maintenance.
UCI Machine Learning Repository.
https://doi.org/10.24432/C5058W
```

---

> Built for railway safety & predictive maintenance simulation using real-world Metro do Porto telemetry data.