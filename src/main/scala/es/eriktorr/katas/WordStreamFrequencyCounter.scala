package es.eriktorr.katas

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StringType

import scala.concurrent.duration.Duration
import scala.language.implicitConversions

class WordStreamFrequencyCounter(private val bootstrapServers: String,
                                 private val inTopics: String,
                                 private val outTopics: String,
                                 private val checkpointLocation: String) {

  private val sparkSession = SparkSession.builder.getOrCreate
  import sparkSession.implicits._

  private val dataFrame = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", inTopics)
    .option("startingOffsets", "earliest")
    .option("group.id", "word-stream-frequency-counter")
    .load()

  def topTenWordFrequency(): Unit = {
    val words = wordsFrom(dataFrame)

    val windowedCounts = words
      .withWatermark("timestamp", "4 seconds")
      .groupBy("word")
      .count()
      .orderBy('count.desc)
      .limit(10)
      .as[WordFrequency]

    windowedCounts
      .withColumn("key", 'word)
      .withColumn("value", 'count.cast(StringType))
      .writeStream
      .queryName("word-frequency-query")
      .outputMode(OutputMode.Complete)
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("topic", outTopics)
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(Duration.create(2, TimeUnit.SECONDS)))
      .start()
      .awaitTermination(30000L)

    //    windowedCounts.writeStream
    //      .queryName("word-frequency-query")
    //      .outputMode(OutputMode.Complete)
    //      .format("console")
    //      .option("truncate", "false")
    //      .option("checkpointLocation", checkpointLocation)
    //      .trigger(Trigger.ProcessingTime(Duration.create(2, TimeUnit.SECONDS)))
    //      .start()
    //      .awaitTermination(30000L)
  }

  def wordsFrom(input: DataFrame): Dataset[Word] = {
    input
      .withColumn("keyCasted", 'key.cast(StringType))
      .withColumn("valueCasted", 'value.cast(StringType))
      .withColumn("words", split('valueCasted, "\\s+"))
      .withColumn("word", explode('words))
      .withColumn("word", lower(trim('word)))
      .where(length('word) > 0)
      .select('word, 'timestamp)
      .as[Word]
  }

}
