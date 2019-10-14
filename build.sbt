name := "word-frequency-spark"

organization := "es.eriktorr.katas"

version := "0.1"

scalaVersion := "2.11.12"

val sparkTestingBase = "2.4.3_0.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.4",
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.3.0" % Test,
  "com.holdenkarau" %% "spark-testing-base" % sparkTestingBase % Test,
  "com.holdenkarau" %% "spark-testing-kafka-0_8" % sparkTestingBase % Test
)

logBuffered in Test := false
parallelExecution in Test := false

// Minimum Memory Requirements for spark-testing-base
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
