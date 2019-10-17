package es.eriktorr.katas

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

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

  def mappingFunc(key: String, values: Iterator[(String, Int)], state: GroupState[UserState]): UserState = {

    for (tuple <- values) println(s"x->${tuple._1}, y->${tuple._2}")

    UserState()
  }

  def wordFrequency(): Unit = {
    val hadoopConfiguration = sparkSession.sparkContext.hadoopConfiguration
    val nameNodeHttpAddress = hadoopConfiguration.getStrings("dfs.namenode.servicerpc-address", "localhost:8020").head

    val dataSet = dataFrame.selectExpr("CAST(value AS STRING)").as[String]
      .flatMap(_.split("\\s+"))
      .map(_.trim.toLowerCase())
      .filter(_.nonEmpty)
      .map(word => (word, 1))
//      .groupByKey(_._1)
//      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(mappingFunc)

//      .groupByKey(_._1)
//      .reduceGroups((a, b) => (a._1, a._2 + b._2))
//      .map(_._2)
//      .orderBy($"_2".desc)
//      .take(25)

    dataSet.writeStream
      .queryName("Word-Frequency-Query")
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .option("checkpointLocation", s"hdfs://${nameNodeHttpAddress}/user/erik_torres/checkpoints/word-frequency-query")
      .start()
      .awaitTermination(1000L)
  }

}
