package es.eriktorr.katas

import org.apache.spark.sql.SparkSession

object WordFrequencyApplication {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("Word-Frequency-Counter")
      .master("local[*]")
      .getOrCreate()

    doRun(args)

    sparkSession.stop()
  }

  def doRun(args: Array[String]): Unit = {
    val bootstrapServers = args(0)
    val topics = listFrom(args(1))

    val wordStreamReader = new WordStreamReader(bootstrapServers, args(1))
    wordStreamReader.wordFrequency()

    //    val topicConsumer = new TopicConsumer(bootstrapServers, topics)
    //    val value = topicConsumer.consume
    //    println(s"\n\n >> HERE: $value\n") // TODO
  }

  private def listFrom: String => List[String] = _.split(",").toList

}
