package es.eriktorr.katas

import org.apache.spark.SparkContext

object WordFrequencyApplication {

  def main(args: Array[String]): Unit = {
    val sparkContext = SparkContext.getOrCreate()

    doRun(sparkContext, args)

    sparkContext.stop()
  }

  def doRun(sparkContext: SparkContext, args: Array[String]): Unit = {
    // TODO
  }

}
