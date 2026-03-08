package railway

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans

/**
 * RailwayAnomalyDetector
 *
 * Reads raw MetroPT-3 sensor data from MongoDB (railway_db.raw_sensor_data),
 * engineers features, runs KMeans-based anomaly scoring,
 * and writes predictions to MongoDB (railway_db.processed_predictions).
 *
 * Uses mongo-spark-connector 3.0.2 which still supports the classic
 * MongoSpark.load / MongoSpark.save API compatible with Spark 3.x
 *
 * Run with:
 *   spark-submit --class railway.RailwayAnomalyDetector \
 *     --master spark://spark-master:7077 \
 *     --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.2 \
 *     railway-detector.jar
 */
object RailwayAnomalyDetector {

  val MONGO_URI  = "mongodb://mongodb:27017"
  val DATABASE   = "railway_db"
  val INPUT_COL  = "raw_sensor_data"
  val OUTPUT_COL = "processed_predictions"

  val FEATURE_COLS: Array[String] = Array(
    "TP2", "TP3", "H1", "DV_pressure",
    "Reservoirs", "Oil_temperature", "Motor_current"
  )

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("RailwayAnomalyDetector-MetroPT3")
      .master("spark://spark-master:7077")
      .config("spark.mongodb.input.uri",  s"$MONGO_URI/$DATABASE.$INPUT_COL")
      .config("spark.mongodb.output.uri", s"$MONGO_URI/$DATABASE.$OUTPUT_COL")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    // ── 1. Load raw data from MongoDB ──────────────────────────────────────
    println("[STEP 1] Loading raw sensor data from MongoDB...")
    val rawDf = spark.read
      .format("com.mongodb.spark.sql.DefaultSource")
      .load()
      .limit(200000)

    val totalRaw = rawDf.count()
    println(s"  Loaded $totalRaw records.")

    val cleanDf = rawDf.na.drop("any", FEATURE_COLS)
    println(s"  After dropping nulls: ${cleanDf.count()} records.")

    // ── 2. Feature Engineering ─────────────────────────────────────────────
    println("[STEP 2] Engineering features...")
    val featuredDf = engineerFeatures(spark, cleanDf)

    // ── 3. Assemble + Scale features ───────────────────────────────────────
    val extendedCols = FEATURE_COLS ++ Array(
      "pressure_diff_TP2_TP3",
      "pressure_diff_H1_DV",
      "motor_oil_ratio",
      "rolling_mean_TP2",
      "rolling_std_TP2",
      "rolling_mean_motor",
      "rolling_std_motor"
    )

    val assembler = new VectorAssembler()
      .setInputCols(extendedCols)
      .setOutputCol("raw_features")
      .setHandleInvalid("skip")

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithMean(true)
      .setWithStd(true)

    val pipeline     = new Pipeline().setStages(Array(assembler, scaler))
    val scalerModel  = pipeline.fit(featuredDf)
    val scaledDf     = scalerModel.transform(featuredDf)

    // ── 4. KMeans anomaly scoring ──────────────────────────────────────────
    println("[STEP 3] Running KMeans anomaly scoring...")

    val sampleSize = math.min((scaledDf.count() * 0.8).toLong, 500000L)
    val normalDf   = scaledDf.limit(sampleSize.toInt)

    val kmeans = new KMeans()
      .setK(8)
      .setSeed(42L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val kmeansModel  = kmeans.fit(normalDf)
    val centroidsDf  = kmeansModel.transform(scaledDf)
    val anomalyDf    = computeAnomalyScores(spark, centroidsDf, kmeansModel)

    // ── 5. Threshold = mean + 3σ ───────────────────────────────────────────
    val stats = anomalyDf.select(
      mean("anomaly_score").as("mean_score"),
      stddev("anomaly_score").as("std_score")
    ).first()

    val threshold = stats.getDouble(0) + 3.0 * stats.getDouble(1)
    println(f"  Anomaly threshold (mean + 3σ): $threshold%.4f")

    val runId    = java.util.UUID.randomUUID().toString
    val now      = java.time.Instant.now().toString

    val labeledDf = anomalyDf
      .withColumn("is_anomaly",       col("anomaly_score") > lit(threshold))
      .withColumn("fault_hint",       deriveFaultHint(col("TP2"), col("Motor_current"), col("Oil_temperature"), col("DV_pressure")))
      .withColumn("threshold_used",   lit(threshold))
      .withColumn("processed_at",     lit(now))
      .withColumn("spark_run_id",     lit(runId))

    // ── 6. Write predictions to MongoDB ────────────────────────────────────
    println("[STEP 4] Writing predictions to MongoDB...")

    val outputDf = labeledDf.select(
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
      col("spark_run_id")
    )

    outputDf.write
      .format("com.mongodb.spark.sql.DefaultSource")
      .mode("append")
      .save()

    // ── 7. Summary ─────────────────────────────────────────────────────────
    val total        = outputDf.count()
    val anomalyCount = labeledDf.filter(col("is_anomaly") === true).count()

    println(s"\n=== DETECTION SUMMARY ===")
    println(f"  Total records    : $total%,d")
    println(f"  Anomalies flagged: $anomalyCount%,d (${100.0 * anomalyCount / total}%.2f%%)")
    println(f"  Threshold used   : $threshold%.4f")
    println(f"  Spark run ID     : $runId")

    spark.stop()
  }

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

  def computeAnomalyScores(
    spark: SparkSession,
    df: DataFrame,
    model: org.apache.spark.ml.clustering.KMeansModel
  ): DataFrame = {
    import org.apache.spark.ml.linalg.Vector

    val centroids = model.clusterCenters

    val distanceUDF = udf((features: Vector, cluster: Int) => {
      val centroid = centroids(cluster)
      math.sqrt(
        features.toArray.zip(centroid.toArray)
          .map { case (a, b) => math.pow(a - b, 2) }
          .sum
      )
    })

    df.withColumn("anomaly_score", distanceUDF(col("features"), col("cluster")))
  }

  def deriveFaultHint(
    tp2: org.apache.spark.sql.Column,
    motor: org.apache.spark.sql.Column,
    oil: org.apache.spark.sql.Column,
    dvPressure: org.apache.spark.sql.Column
  ): org.apache.spark.sql.Column = {
    when(tp2 < lit(8.0),        lit("LOW_COMPRESSOR_PRESSURE"))
    .when(motor > lit(25.0),    lit("HIGH_MOTOR_CURRENT"))
    .when(oil > lit(90.0),      lit("OVERHEATING"))
    .when(dvPressure < lit(0.5),lit("VALVE_PRESSURE_DROP"))
    .otherwise(                 lit("GENERAL_ANOMALY"))
  }
}
