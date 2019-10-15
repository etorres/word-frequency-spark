package es.eriktorr.katas

import org.apache.spark.SparkContext

object WordFrequencyApplication {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkContext.getOrCreate()

    doRun(sparkContext, args)

    sparkContext.stop()
  }

  def doRun(sparkContext: SparkContext, args: Array[String]): Unit = {
    val bootstrapServers = args(0)
    val topics = listFrom(args(1))

    val topicConsumer = new TopicConsumer(bootstrapServers, topics)
    val value = topicConsumer.consume
    println(s"\n\n >> HERE: $value\n") // TODO
  }

  private def listFrom: String => List[String] = _.split(",").toList

}
