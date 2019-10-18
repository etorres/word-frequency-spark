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
    val topics = args(1)

    val wordStreamReader = new WordStreamReader(bootstrapServers, topics)
    wordStreamReader.wordFrequency()
  }

  private def listFrom: String => List[String] = _.split(",").toList

}
