// mongo-init.js
// Run automatically when MongoDB container starts for the first time
// Creates railway_db with proper collections and indexes

db = db.getSiblingDB("railway_db");

// ── 1. RAW DATASET COLLECTION ──────────────────────────────────────────────────
// Stores MetroPT-3 CSV rows exactly as-is, loaded once at startup
db.createCollection("raw_sensor_data", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["timestamp"],
      properties: {
        timestamp:       { bsonType: "string",  description: "ISO timestamp from CSV" },
        TP2:             { bsonType: "double",  description: "Compressor pressure (bar)" },
        TP3:             { bsonType: "double",  description: "Pneumatic panel pressure (bar)" },
        H1:              { bsonType: "double",  description: "Discharge valve pressure (bar)" },
        DV_pressure:     { bsonType: "double",  description: "Differential pressure at towers valve (bar)" },
        Reservoirs:      { bsonType: "double",  description: "Air tank pressure (bar)" },
        Oil_temperature: { bsonType: "double",  description: "Compressor oil temperature (C)" },
        Motor_current:   { bsonType: "double",  description: "Compressor motor current (A)" },
        COMP:            { bsonType: "int",     description: "Compressor ON/OFF" },
        DV_eletric:      { bsonType: "int",     description: "Electrical discharge valve status" },
        Towers:          { bsonType: "int",     description: "Air drying tower status" },
        MPG:             { bsonType: "int",     description: "Manual pressure gauge" },
        LPS:             { bsonType: "int",     description: "Low pressure switch" },
        Pressure_switch: { bsonType: "int",     description: "Pressure switch activation" },
        Oil_level:       { bsonType: "int",     description: "Oil level indicator" },
        Caudal_impulses: { bsonType: "int",     description: "Flow impulse counter" },
        _loaded_at:      { bsonType: "string",  description: "When this record was loaded into MongoDB" }
      }
    }
  },
  validationAction: "warn"   // warn instead of error so bad rows don't block ingestion
});

// Indexes on raw_sensor_data
db.raw_sensor_data.createIndex({ "timestamp": 1 },             { name: "idx_timestamp" });
db.raw_sensor_data.createIndex({ "TP2": 1 },                   { name: "idx_tp2" });
db.raw_sensor_data.createIndex({ "Motor_current": 1 },         { name: "idx_motor" });
db.raw_sensor_data.createIndex({ "Oil_temperature": 1 },       { name: "idx_oil_temp" });

print("✅ Collection created: raw_sensor_data");

// ── 2. PROCESSED PREDICTIONS COLLECTION ───────────────────────────────────────
// Stores Spark ML output — anomaly scores and fault labels per record
db.createCollection("processed_predictions", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["source_timestamp", "anomaly_score", "is_anomaly", "processed_at"],
      properties: {
        source_timestamp: { bsonType: "string",  description: "Original timestamp from raw_sensor_data" },
        anomaly_score:    { bsonType: "double",  description: "KMeans centroid distance — higher = more anomalous" },
        is_anomaly:       { bsonType: "bool",    description: "True if score exceeds threshold (mean + 3sigma)" },
        fault_hint:       { bsonType: "string",  description: "Rule-based fault label" },
        cluster_id:       { bsonType: "int",     description: "KMeans cluster assignment" },
        threshold_used:   { bsonType: "double",  description: "Threshold value applied in this run" },
        // Raw sensor values carried forward for dashboard display
        TP2:              { bsonType: "double" },
        Motor_current:    { bsonType: "double" },
        Oil_temperature:  { bsonType: "double" },
        DV_pressure:      { bsonType: "double" },
        // Engineered features
        pressure_diff_TP2_TP3: { bsonType: "double" },
        motor_oil_ratio:       { bsonType: "double" },
        rolling_mean_TP2:      { bsonType: "double" },
        rolling_mean_motor:    { bsonType: "double" },
        // Metadata
        processed_at:     { bsonType: "string",  description: "When Spark wrote this prediction" },
        spark_run_id:     { bsonType: "string",  description: "Unique ID per Spark job run" }
      }
    }
  },
  validationAction: "warn"
});

// Indexes on processed_predictions
db.processed_predictions.createIndex({ "source_timestamp": 1 },   { name: "idx_src_timestamp" });
db.processed_predictions.createIndex({ "is_anomaly": 1 },          { name: "idx_is_anomaly" });
db.processed_predictions.createIndex({ "anomaly_score": -1 },      { name: "idx_anomaly_score_desc" });
db.processed_predictions.createIndex({ "fault_hint": 1 },          { name: "idx_fault_hint" });
db.processed_predictions.createIndex({ "processed_at": -1 },       { name: "idx_processed_at" });
db.processed_predictions.createIndex(
  { "is_anomaly": 1, "source_timestamp": 1 },
  { name: "idx_anomaly_timestamp_compound" }
);

print("✅ Collection created: processed_predictions");

// ── 3. SPARK RUN METADATA COLLECTION ──────────────────────────────────────────
// Tracks each Spark ML job run — useful for Airflow audit trail
db.createCollection("spark_run_log");
db.spark_run_log.createIndex({ "run_id": 1 },      { unique: true, name: "idx_run_id" });
db.spark_run_log.createIndex({ "started_at": -1 }, { name: "idx_started_at" });

print("✅ Collection created: spark_run_log");

// ── Summary ────────────────────────────────────────────────────────────────────
print("\n=== railway_db initialized ===");
print("Collections:");
db.getCollectionNames().forEach(c => print("  - " + c));
