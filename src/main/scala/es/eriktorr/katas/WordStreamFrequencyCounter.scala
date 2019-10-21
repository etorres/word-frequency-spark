package es.eriktorr.katas

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.language.implicitConversions

class WordStreamFrequencyCounter(private val bootstrapServers: String,
                                 private val inTopics: String,
                                 private val outTopics: String,
                                 private val checkpointLocation: String) {

  private val sparkSession = SparkSession.builder.getOrCreate
  import sparkSession.implicits._

  private val lines = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", inTopics)
    .option("startingOffsets", "earliest")
    .option("group.id", "word-stream-frequency-counter")
    .load()

  def topTenWordFrequency(): Unit = {
    val words = wordsFrom(lines)
    val wordFrequencies = topTenMostCommon(words, 4)
    processNext(wordFrequencies, 2, 30)
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

  def topTenMostCommon(words: Dataset[Word], allowedLateArrivalsInSeconds: Int): Dataset[WordFrequency] = {
    words
      .withWatermark("timestamp", s"$allowedLateArrivalsInSeconds seconds")
      .groupBy("word")
      .count()
      .orderBy('count.desc)
      .limit(10)
      .as[WordFrequency]
  }

  private def processNext(windowedCounts: Dataset[WordFrequency], microBatchesIntervalInSeconds: Int, processingWindowInSeconds: Int): Unit = {
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
      .trigger(Trigger.ProcessingTime(Duration.create(microBatchesIntervalInSeconds, TimeUnit.SECONDS)))
      .start()
      .awaitTermination(TimeUnit.SECONDS.toMillis(processingWindowInSeconds))
  }

  private def debugNext(windowedCounts: Dataset[WordFrequency], microBatchesIntervalInSeconds: Int, processingWindowInSeconds: Int): Unit = {
    windowedCounts.writeStream
      .queryName("word-frequency-query")
      .outputMode(OutputMode.Complete)
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(Duration.create(microBatchesIntervalInSeconds, TimeUnit.SECONDS)))
      .start()
      .awaitTermination(TimeUnit.SECONDS.toMillis(processingWindowInSeconds))
  }

}
