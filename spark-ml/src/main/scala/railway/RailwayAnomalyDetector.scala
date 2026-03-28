package railway

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.PipelineModel
import java.io.{File, PrintWriter}

object RailwayAnomalyDetector {

  val MONGO_URI  = "mongodb://172.20.0.4:27017"
  val DATABASE   = "railway_db"
  val INPUT_COL  = "raw_sensor_data"
  val OUTPUT_COL = "processed_predictions"
  val LOG_COL    = "spark_run_log"

  // ── Model persistence paths ──────────────────────────────────────────────
  val PIPELINE_MODEL_PATH = "/opt/railway-model/scaler-pipeline"
  val KMEANS_MODEL_PATH   = "/opt/railway-model/kmeans"
  val THRESHOLD_PATH      = "/opt/railway-model/threshold.txt"
  val EVAL_REPORT_PATH    = "/opt/railway-model/evaluation_report.txt"

  // ── Data split ratios ────────────────────────────────────────────────────
  // 70% TRAIN  → KMeans fitting + threshold computation (no leakage)
  // 20% TEST   → held-out evaluation, NOT written to MongoDB
  // 10% DEMO   → reserved for live dashboard demo, written to MongoDB
  val TRAIN_RATIO = 0.70
  val TEST_RATIO  = 0.20

  val FEATURE_COLS: Array[String] = Array(
    "TP2", "TP3", "H1", "DV_pressure",
    "Reservoirs", "Oil_temperature", "Motor_current"
  )

  val EXTENDED_COLS: Array[String] = FEATURE_COLS ++ Array(
    "pressure_diff_TP2_TP3",
    "pressure_diff_H1_DV",
    "motor_oil_ratio",
    "rolling_mean_TP2",
    "rolling_std_TP2",
    "rolling_mean_motor",
    "rolling_std_motor"
  )

  // ── Entry point ──────────────────────────────────────────────────────────
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RailwayAnomalyDetector-MetroPT3")
      .config("spark.mongodb.read.connection.uri",
              s"mongodb://172.20.0.4:27017/$DATABASE.$INPUT_COL")
      .config("spark.mongodb.write.connection.uri",
              s"mongodb://172.20.0.4:27017/$DATABASE.$OUTPUT_COL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // ── 1. Load raw data ───────────────────────────────────────────────────
    println("[STEP 1] Loading raw sensor data from MongoDB...")
    val rawDf = spark.read
      .format("mongodb")
      .option("spark.mongodb.read.connection.uri",
              s"mongodb://172.20.0.4:27017/$DATABASE.$INPUT_COL")
      .option("database",   DATABASE)
      .option("collection", INPUT_COL)
      .load()

    println(s"  Loaded ${rawDf.count()} records.")
    val cleanDf = rawDf.na.drop("any", FEATURE_COLS)
    .limit(200000)  // ← add this line
    .cache()
    println(s"  After dropping nulls: ${cleanDf.count()} records.")

    // ── 2. Feature Engineering ─────────────────────────────────────────────
    println("[STEP 2] Engineering features...")
    val featuredDf = engineerFeatures(spark, cleanDf).cache()
    val totalCount = featuredDf.count()

    // ── 3. Temporal 70 / 20 / 10 split ────────────────────────────────────
    // MetroPT-3 is a time-series — we split temporally (not randomly) so the
    // model never sees future data during training.
    println("[STEP 3] Splitting data (70% train / 20% test / 10% demo)...")

    val trainEnd = (totalCount * TRAIN_RATIO).toLong
    val testEnd  = (totalCount * (TRAIN_RATIO + TEST_RATIO)).toLong

    val indexedDf   = featuredDf.withColumn("_row_idx", monotonically_increasing_id())
    val sortedIdx   = indexedDf.select("_row_idx").orderBy("_row_idx")
    val trainMaxIdx = sortedIdx.limit(trainEnd.toInt).agg(max("_row_idx")).first().getLong(0)
    val testMaxIdx  = sortedIdx.limit(testEnd.toInt).agg(max("_row_idx")).first().getLong(0)

    val trainDf = indexedDf.filter(col("_row_idx") <= trainMaxIdx).drop("_row_idx").cache()
    val testDf  = indexedDf.filter(col("_row_idx") > trainMaxIdx && col("_row_idx") <= testMaxIdx).drop("_row_idx").cache()
    val demoDf  = indexedDf.filter(col("_row_idx") > testMaxIdx).drop("_row_idx").cache()

    val trainCount = trainDf.count()
    val testCount  = testDf.count()
    val demoCount  = demoDf.count()

    println(s"  Train : ${"%,d".format(trainCount)} rows  (70%)")
    println(s"  Test  : ${"%,d".format(testCount)} rows  (20%) — evaluation only")
    println(s"  Demo  : ${"%,d".format(demoCount)} rows  (10%) — written to MongoDB")

    // ── 4. Load or train model on TRAIN split ─────────────────────────────
    println("[STEP 4] Checking for saved model...")

    val modelExists =
      new File(PIPELINE_MODEL_PATH).exists() &&
      new File(KMEANS_MODEL_PATH).exists()   &&
      new File(THRESHOLD_PATH).exists()

    val (scalerModel, kmeansModel, threshold) = if (modelExists) {
      println("  [CACHE HIT] Loading saved models from disk...")
      val sm  = PipelineModel.load(PIPELINE_MODEL_PATH)
      val km  = KMeansModel.load(KMEANS_MODEL_PATH)
      val thr = scala.io.Source.fromFile(THRESHOLD_PATH).mkString.trim.toDouble
      println(f"  Loaded threshold: $thr%.4f")
      (sm, km, thr)
    } else {
      println("  [TRAINING] No saved model — training on 70% split...")
      trainAndSave(spark, trainDf)
    }

    // ── 5. Evaluate on held-out TEST set ──────────────────────────────────
    println("[STEP 5] Evaluating on held-out 20% test set...")
    val testScaled    = scalerModel.transform(testDf)
    val testCentroids = kmeansModel.transform(testScaled)
    val testScored    = computeAnomalyScores(spark, testCentroids, kmeansModel)
    val testLabeled   = testScored
      .withColumn("is_anomaly", col("anomaly_score") > lit(threshold))
      .withColumn("fault_hint",
        when(col("is_anomaly") === true,
          deriveFaultHint(col("TP2"), col("Motor_current"),
                          col("Oil_temperature"), col("DV_pressure")))
        .otherwise(lit("NORMAL")))

    val evalReport = evaluateModel(
      spark, testLabeled, kmeansModel, threshold, testDf, "TEST SET (20%)")
    println(evalReport)
    saveTextFile(evalReport, EVAL_REPORT_PATH)

    // ── 6. Score DEMO split and write to MongoDB for dashboard ────────────
    println("[STEP 6] Scoring demo split and writing to MongoDB...")

    val runId = java.util.UUID.randomUUID().toString
    val now   = java.time.Instant.now().toString

    val demoScaled    = scalerModel.transform(demoDf)
    val demoCentroids = kmeansModel.transform(demoScaled)
    val demoScored    = computeAnomalyScores(spark, demoCentroids, kmeansModel)
    val demoLabeled   = demoScored
      .withColumn("is_anomaly", col("anomaly_score") > lit(threshold))
      .withColumn("fault_hint",
        when(col("is_anomaly") === true,
          deriveFaultHint(col("TP2"), col("Motor_current"),
                          col("Oil_temperature"), col("DV_pressure")))
        .otherwise(lit("NORMAL")))
      .withColumn("threshold_used", lit(threshold))
      .withColumn("processed_at",   lit(now))
      .withColumn("spark_run_id",   lit(runId))
      .withColumn("data_split",     lit("demo"))

    val outputDf = demoLabeled.select(
      col("timestamp").as("source_timestamp"),
      col("anomaly_score"),
      col("is_anomaly"),
      col("fault_hint"),
      col("cluster").as("cluster_id"),
      col("threshold_used"),
      col("TP2"),
      col("Motor_current"),
      col("Oil_temperature"),
      col("DV_pressure"),
      col("pressure_diff_TP2_TP3"),
      col("motor_oil_ratio"),
      col("rolling_mean_TP2"),
      col("rolling_mean_motor"),
      col("processed_at"),
      col("spark_run_id"),
      col("data_split")
    )

    outputDf.write
      .format("mongodb")
      .option("spark.mongodb.write.connection.uri",
              s"mongodb://172.20.0.4:27017/$DATABASE.$OUTPUT_COL")
      .option("database",   DATABASE)
      .option("collection", OUTPUT_COL)
      .mode("append")
      .save()

    val demoAnomal = demoLabeled.filter(col("is_anomaly") === true).count()

    // ── 7. Summary ─────────────────────────────────────────────────────────
    println(s"\n=== FINAL SUMMARY ===")
    println(s"  Train rows     : ${"%,d".format(trainCount)}  (model training)")
    println(s"  Test rows      : ${"%,d".format(testCount)}  (evaluation only — NOT in DB)")
    println(s"  Demo rows      : ${"%,d".format(demoCount)}  (written to MongoDB)")
    println(f"  Demo anomalies : ${"%,d".format(demoAnomal)} (${100.0 * demoAnomal / demoCount}%.2f%%)")
    println(f"  Threshold      : $threshold%.4f")
    println(s"  Spark run ID   : $runId")
    println(s"  Eval report    : $EVAL_REPORT_PATH")

    // ── 8. Write spark_run_log ─────────────────────────────────────────────
    val logDf = Seq((
      runId, now, demoCount, demoAnomal, threshold, modelExists, "demo"
    )).toDF(
      "spark_run_id", "processed_at", "records_processed",
      "anomalies_found", "threshold_used", "model_loaded_from_cache", "data_split"
    )

    logDf.write
      .format("mongodb")
      .option("database",   DATABASE)
      .option("collection", LOG_COL)
      .mode("append")
      .save()

    println("  Run log written.")
    spark.stop()
  }

  // ── Train, compute threshold, and save models ────────────────────────────
  def trainAndSave(
    spark: SparkSession,
    trainDf: DataFrame
  ): (PipelineModel, KMeansModel, Double) = {

    val assembler = new VectorAssembler()
      .setInputCols(EXTENDED_COLS)
      .setOutputCol("raw_features")
      .setHandleInvalid("skip")

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithMean(true)
      .setWithStd(true)

    val pipeline    = new Pipeline().setStages(Array(assembler, scaler))
    val scalerModel = pipeline.fit(trainDf)
    val scaledDf    = scalerModel.transform(trainDf)

    println(s"  Fitting KMeans (k=8) on ${"%,d".format(trainDf.count())} rows...")
    val kmeans = new KMeans()
      .setK(8)
      .setSeed(42L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val kmeansModel    = kmeans.fit(scaledDf)
    val trainCentroids = kmeansModel.transform(scaledDf)
    val trainScored    = computeAnomalyScores(spark, trainCentroids, kmeansModel)

    val stats = trainScored.select(
      mean("anomaly_score").as("mu"),
      stddev("anomaly_score").as("sigma")
    ).first()

    val threshold = stats.getDouble(0) + 3.0 * stats.getDouble(1)
    println(f"  Threshold (train mean + 3 sigma): $threshold%.4f")

    new File(PIPELINE_MODEL_PATH).getParentFile.mkdirs()
    scalerModel.write.overwrite().save(PIPELINE_MODEL_PATH)
    kmeansModel.write.overwrite().save(KMEANS_MODEL_PATH)
    saveTextFile(threshold.toString, THRESHOLD_PATH)
    println("  Models saved to /opt/railway-model/")

    (scalerModel, kmeansModel, threshold)
  }

  // ── Evaluation on held-out test set ──────────────────────────────────────
  def evaluateModel(
    spark: SparkSession,
    labeledDf: DataFrame,
    kmeansModel: KMeansModel,
    threshold: Double,
    rawDf: DataFrame,
    splitLabel: String
  ): String = {

    val sb = new StringBuilder

    sb.append("\n" + "=" * 62 + "\n")
    sb.append(s"  RAILWAY ANOMALY DETECTOR — EVALUATION REPORT\n")
    sb.append(s"  Split: $splitLabel\n")
    sb.append("=" * 62 + "\n\n")

    // 1. Counts
    val total        = labeledDf.count()
    val anomalyCount = labeledDf.filter(col("is_anomaly") === true).count()
    val normalCount  = total - anomalyCount
    val anomalyPct   = 100.0 * anomalyCount / total

    sb.append("[1] DATASET OVERVIEW\n")
    sb.append(s"    Total records    : ${"%,d".format(total)}\n")
    sb.append(s"    Normal records   : ${"%,d".format(normalCount)} (${f"${100.0 - anomalyPct}%.2f"}%%)\n")
    sb.append(s"    Anomaly records  : ${"%,d".format(anomalyCount)} (${f"$anomalyPct%.2f"}%%)\n")
    sb.append(f"    Threshold        : $threshold%.4f\n\n")

    // 2. Score distribution
    val ss = labeledDf.select(
      mean("anomaly_score"),
      stddev("anomaly_score"),
      min("anomaly_score"),
      max("anomaly_score"),
      expr("percentile_approx(anomaly_score, 0.50)"),
      expr("percentile_approx(anomaly_score, 0.90)"),
      expr("percentile_approx(anomaly_score, 0.95)"),
      expr("percentile_approx(anomaly_score, 0.99)")
    ).first()

    sb.append("[2] ANOMALY SCORE DISTRIBUTION\n")
    sb.append(f"    Mean   : ${ss.getDouble(0)}%.4f\n")
    sb.append(f"    Std    : ${ss.getDouble(1)}%.4f\n")
    sb.append(f"    Min    : ${ss.getDouble(2)}%.4f\n")
    sb.append(f"    Max    : ${ss.getDouble(3)}%.4f\n")
    sb.append(f"    p50    : ${ss.getDouble(4)}%.4f\n")
    sb.append(f"    p90    : ${ss.getDouble(5)}%.4f\n")
    sb.append(f"    p95    : ${ss.getDouble(6)}%.4f\n")
    sb.append(f"    p99    : ${ss.getDouble(7)}%.4f\n\n")

    // 3. Class separation
    val normalMu = labeledDf.filter(col("is_anomaly") === false)
                            .select(mean("anomaly_score")).first().getDouble(0)
    val anomMu   = labeledDf.filter(col("is_anomaly") === true)
                            .select(mean("anomaly_score")).first().getDouble(0)
    val sep      = anomMu - normalMu

    sb.append("[3] CLASS SEPARATION\n")
    sb.append(f"    Normal class mean  : $normalMu%.4f\n")
    sb.append(f"    Anomaly class mean : $anomMu%.4f\n")
    sb.append(f"    Gap (anomaly-normal): $sep%.4f")
    if (sep > threshold * 0.5) sb.append("  GOOD\n\n")
    else                        sb.append("  LOW - classes overlap\n\n")

    // 4. Cluster breakdown
    sb.append("[4] CLUSTER QUALITY\n")
    sb.append(s"    ${"Cluster"}%-10s ${"Size"}%12s ${"Mean Dist"}%12s ${"Std Dist"}%12s\n")
    sb.append("    " + "-" * 50 + "\n")

    var wcss = 0.0
    labeledDf
      .groupBy("cluster")
      .agg(count("*").as("sz"), mean("anomaly_score").as("md"), stddev("anomaly_score").as("sd"))
      .orderBy("cluster")
      .collect()
      .foreach { row =>
        val cid = row.getInt(0)
        val sz  = row.getLong(1)
        val md  = row.getDouble(2)
        val sd  = if (row.isNullAt(3)) 0.0 else row.getDouble(3)
        wcss   += md * sz
        sb.append(s"    ${cid.toString.padTo(10, ' ')} ${"%,d".format(sz).reverse.padTo(12, ' ').reverse} ${f"$md%12.4f"} ${f"$sd%12.4f"}\n")
      }
    sb.append(f"\n    Weighted WCSS proxy : $wcss%.2f  (lower = tighter clusters)\n\n")

    // 5. Fault label breakdown
    sb.append("[5] FAULT LABEL BREAKDOWN (anomalies only)\n")
    labeledDf.filter(col("is_anomaly") === true)
      .groupBy("fault_hint").count().orderBy(desc("count")).collect()
      .foreach { row =>
        val label = row.getString(0)
        val cnt   = row.getLong(1)
        val pct   = 100.0 * cnt / anomalyCount
        sb.append(s"    ${label.padTo(35, ' ')} ${"%,d".format(cnt).reverse.padTo(10, ' ').reverse}  (${f"$pct%.1f"}%%)\n")
      }
    sb.append("\n")

    // 6. Sensor breach counts
    sb.append("[6] SENSOR THRESHOLD BREACHES\n")
    sb.append(s"    TP2 < 8.0 bar          : ${"%,d".format(rawDf.filter(col("TP2") < 8.0).count())}\n")
    sb.append(s"    Motor_current > 25A    : ${"%,d".format(rawDf.filter(col("Motor_current") > 25.0).count())}\n")
    sb.append(s"    Oil_temperature > 90C  : ${"%,d".format(rawDf.filter(col("Oil_temperature") > 90.0).count())}\n")
    sb.append(s"    DV_pressure < 0.5 bar  : ${"%,d".format(rawDf.filter(col("DV_pressure") < 0.5).count())}\n\n")

    // 7. Auto quality checks
    sb.append("[7] AUTOMATED QUALITY CHECKS\n")

    if (anomalyPct >= 1.0 && anomalyPct <= 5.0)
      sb.append("    [PASS] Anomaly rate is in the healthy 1-5% range\n")
    else if (anomalyPct < 1.0)
      sb.append(s"    [WARN] Anomaly rate ${f"$anomalyPct%.2f"}%% < 1%% — threshold may be too strict\n")
    else
      sb.append(s"    [WARN] Anomaly rate ${f"$anomalyPct%.2f"}%% > 5%% — threshold may be too lenient\n")

    if (sep > 1.0)
      sb.append("    [PASS] Strong class separation between normal and anomaly scores\n")
    else
      sb.append("    [WARN] Weak class separation — model may not distinguish faults well\n")

    sb.append("\n[8] DATA SPLIT USED\n")
    sb.append("    70%% TRAIN  -> KMeans + threshold (no data leakage)\n")
    sb.append("    20%% TEST   -> held-out evaluation (this report)\n")
    sb.append("    10%% DEMO   -> written to MongoDB for live dashboard\n\n")

    sb.append("[9] TO FORCE RETRAIN\n")
    sb.append("    docker exec -it spark-master rm -rf /opt/railway-model\n")
    sb.append("    Then re-run spark-submit.\n\n")

    sb.append("=" * 62 + "\n")
    sb.toString()
  }

  // ── Feature Engineering ───────────────────────────────────────────────────
  def engineerFeatures(spark: SparkSession, df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val w30 = Window.orderBy("timestamp").rowsBetween(-180, 0)

    df
      .withColumn("pressure_diff_TP2_TP3", col("TP2") - col("TP3"))
      .withColumn("pressure_diff_H1_DV",   col("H1")  - col("DV_pressure"))
      .withColumn("motor_oil_ratio",        col("Motor_current") / (col("Oil_temperature") + lit(1.0)))
      .withColumn("rolling_mean_TP2",       avg("TP2").over(w30))
      .withColumn("rolling_std_TP2",        stddev("TP2").over(w30))
      .withColumn("rolling_mean_motor",     avg("Motor_current").over(w30))
      .withColumn("rolling_std_motor",      stddev("Motor_current").over(w30))
      .na.fill(0.0)
  }

  // ── Euclidean distance to nearest centroid ────────────────────────────────
  def computeAnomalyScores(
    spark: SparkSession,
    df: DataFrame,
    model: KMeansModel
  ): DataFrame = {
    import org.apache.spark.ml.linalg.Vector
    val centroids = model.clusterCenters
    val distUDF   = udf((features: Vector, cluster: Int) => {
      val c = centroids(cluster)
      math.sqrt(features.toArray.zip(c.toArray).map { case (a, b) => math.pow(a - b, 2) }.sum)
    })
    df.withColumn("anomaly_score", distUDF(col("features"), col("cluster")))
  }

  // ── Rule-based fault label ────────────────────────────────────────────────
  def deriveFaultHint(
    tp2: org.apache.spark.sql.Column,
    motor: org.apache.spark.sql.Column,
    oil: org.apache.spark.sql.Column,
    dvPressure: org.apache.spark.sql.Column
  ): org.apache.spark.sql.Column =
    when(tp2 < lit(8.0),         lit("LOW_COMPRESSOR_PRESSURE"))
    .when(motor > lit(25.0),     lit("HIGH_MOTOR_CURRENT"))
    .when(oil > lit(90.0),       lit("OVERHEATING"))
    .when(dvPressure < lit(0.5), lit("VALVE_PRESSURE_DROP"))
    .otherwise(                  lit("GENERAL_ANOMALY"))

  // ── Write string to file ──────────────────────────────────────────────────
  def saveTextFile(content: String, path: String): Unit = {
    try {
      new File(path).getParentFile.mkdirs()
      val pw = new PrintWriter(new File(path))
      pw.write(content)
      pw.close()
    } catch {
      case e: Exception => println(s"  [WARN] Could not write $path: ${e.getMessage}")
    }
  }
}