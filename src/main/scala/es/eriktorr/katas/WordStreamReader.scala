package es.eriktorr.katas

import java.sql.Timestamp

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.functions.{col, explode, split}

import scala.language.implicitConversions

class WordStreamReader(private val bootstrapServers: String, private val topic: String) {

  private val sparkSession = SparkSession.builder
    .appName("Word-Frequency-Counter")
    .master("local[*]")
    .getOrCreate
  import sparkSession.implicits._

  private val dataFrame = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .option("group.id", "kafka-sandbox-consumer-group")
      .load()

  def wordFrequency(): Unit = {
    val words = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp").as[(String, String, Long)]
      .select(explode(split(col("value"),"\\s+")).alias("word"), col("timestamp"))
      .map(row => (row.getAs[String]("word").trim.toLowerCase, row.getAs[Timestamp]("timestamp")))
      .filter(_._1.nonEmpty)
      .toDF("word", "timestamp")

    val windowedCounts = words
      .withWatermark("timestamp", "2 seconds")
      .groupBy("word")
      .count()


//      .orderBy($"count".desc)
//      .limit(25)


//    val dataSet = dataFrame.selectExpr("CAST(value AS STRING)").as[String]
//      .flatMap(_.split("\\s+"))
//      .map(_.trim.toLowerCase())
//      .filter(_.nonEmpty)
//      .map(word => (word, 1))
//      .groupByKey(_._1)
//      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(updateFrequencyAcrossEvents)
//      .orderBy($"value._2".desc)

//      .orderBy($"_2".desc)

    //      .groupByKey(_._1)
    //      .reduceGroups((a, b) => (a._1, a._2 + b._2))
    //      .map(_._2)
    //      .orderBy($"_2".desc)
    //      .take(25)

    val nameNodeAddress = hadoopNameNodeAddress apply sparkSession.sparkContext.hadoopConfiguration

    windowedCounts.writeStream
      .queryName("Word-Frequency-Query")
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .option("checkpointLocation", s"hdfs://${nameNodeAddress}/user/erik_torres/checkpoints/word-frequency-query")
      .start()
//      .awaitTermination()
      .awaitTermination(120000L)
  }

  private val updateFrequencyAcrossEvents: (String, Iterator[(String, Int)], GroupState[Map[String, Int]]) => Map[String, Int] = (key, values, state) => {
    val currentState = state.getOption.getOrElse(Map[String, Int]())
    val count = values.map(_._2).sum + currentState.getOrElse(key, 0)
    currentState + (key -> count)
  }

  private def hadoopNameNodeAddress: Configuration =>  String =
    _.getStrings("dfs.namenode.servicerpc-address", "localhost:8020").head

}
