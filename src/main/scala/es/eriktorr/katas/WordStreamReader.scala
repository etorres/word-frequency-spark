package es.eriktorr.katas

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StringType

import scala.concurrent.duration.Duration
import scala.language.implicitConversions

class WordStreamReader(private val bootstrapServers: String, private val topic: String, private val checkpointLocation: String) {

  private val sparkSession = SparkSession.builder.getOrCreate
  import sparkSession.implicits._

  private val dataFrame = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("group.id", "word-stream-reader-consumer-group")
    .load()

  def wordFrequency(): Unit = {
    val words = dataFrame
      .withColumn("keyCasted", 'key.cast(StringType))
      .withColumn("valueCasted", 'value.cast(StringType))
      .withColumn("words", split('valueCasted, "\\s+"))
      .withColumn("word", explode('words))
      .withColumn("word", lower(trim('word)))
      .where(length('word) > 0)
      .select('word, 'timestamp)
      .as[Word]

    val windowedCounts = words
      .withWatermark("timestamp", "4 seconds")
      .groupBy("word")
      .count()
      .orderBy('count.desc)
      .limit(5)
      .as[WordFrequency]

    windowedCounts.writeStream
      .queryName("Word-Frequency-Query")
      .outputMode(OutputMode.Complete)
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(Duration.create(2, TimeUnit.SECONDS)))
      .start()
      .awaitTermination(30000L)
  }

}
