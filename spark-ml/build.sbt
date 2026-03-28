name := "railway-anomaly-detector"
version := "1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"   % "3.5.1" % "provided",
  "org.apache.spark"  %% "spark-sql"    % "3.5.1" % "provided",
  "org.apache.spark"  %% "spark-mllib"  % "3.5.1" % "provided",
  "org.mongodb.spark"  % "mongo-spark-connector_2.12" % "10.4.0"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}