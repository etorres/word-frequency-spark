package es.eriktorr.katas

import org.apache.spark.sql.SparkSession

object WordFrequencyApplication {

  val ApplicationName = "word-frequency-counter"

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName(ApplicationName)
      .master("local[*]")
      .getOrCreate()

    doRun(args)

    sparkSession.stop()
  }

  def doRun(args: Array[String]): Unit = {
    val bootstrapServers = args(0)
    val topics = args(1)
    val checkpointLocation = args(2)

    val wordStreamReader = new WordStreamReader(bootstrapServers, topics, checkpointLocation)
    wordStreamReader.wordFrequency()
  }

}
