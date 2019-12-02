name := "spark-streeaming"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"
val kafkaVersion = "2.4.3"
val sparkAvroVersion = "4.0.0"
val sparkStreamingVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % kafkaVersion,
  "org.apache.spark" %% "spark-streaming" % sparkStreamingVersion,
  "com.github.mrpowers" % "spark-fast-tests" % "v0.16.0" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.3.0_0.12.0" % "test"
)