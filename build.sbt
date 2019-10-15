name := "word-frequency-spark"

organization := "es.eriktorr.katas"

version := "0.1"

scalaVersion := "2.11.12"

val kafkaVersion = "2.3.0"
val sparkTestingBaseVersion = "2.4.3_0.12.0"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "com.dimafeng" %% "testcontainers-scala" % "0.33.0" % Test,
  "org.testcontainers" % "kafka" % "1.12.2" % Test,
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "com.holdenkarau" %% "spark-testing-base" % sparkTestingBaseVersion % Test,
  "com.holdenkarau" %% "spark-testing-kafka-0_8" % sparkTestingBaseVersion % Test
)

logBuffered in Test := false
parallelExecution in Test := false

// Minimum Memory Requirements for spark-testing-base
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
