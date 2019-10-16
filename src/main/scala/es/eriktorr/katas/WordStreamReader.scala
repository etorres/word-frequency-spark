package es.eriktorr.katas

import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions

class WordStreamReader(private val bootstrapServers: String, private val topic: String) {

  private val sparkSession = SparkSession.builder
    .appName("Word-Frequency-Counter")
    .getOrCreate
  import sparkSession.implicits._

  private val dataFrame = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", topic)
    .option("startingOffsets", "latest")
    .option("group.id", "kafka-sandbox-consumer-group")
    .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    .option("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer")
    .load()

  def wordFrequency(): Unit = {
    dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
//      .flatMap()

    dataFrame.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate","false")
      .start()
      .awaitTermination(1000L)
  }

}
