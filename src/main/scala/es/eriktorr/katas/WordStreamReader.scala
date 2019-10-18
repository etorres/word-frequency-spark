package es.eriktorr.katas

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StringType

import scala.language.implicitConversions

class WordStreamReader(private val bootstrapServers: String, private val topic: String) {

  private val sparkSession = SparkSession.builder.getOrCreate
  import sparkSession.implicits._

  private val nameNodeAddress = hadoopNameNodeAddress apply sparkSession.sparkContext.hadoopConfiguration

  private val dataFrame = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .option("group.id", "kafka-sandbox-consumer-group")
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

    val windowedCounts = words
      .withWatermark("timestamp", "10 seconds")
      .groupBy("word")
      .count()

    windowedCounts.writeStream
      .queryName("Word-Frequency-Query")
      .outputMode(OutputMode.Update)
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", s"hdfs://$nameNodeAddress/user/erik_torres/checkpoints/word-frequency-query")
      .start()
      //      .awaitTermination()
      .awaitTermination(10000L)
  }

  private def hadoopNameNodeAddress: Configuration => String =
    _.getStrings("dfs.namenode.servicerpc-address", "localhost:8020").head

}
