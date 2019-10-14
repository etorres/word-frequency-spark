package es.eriktorr.katas

import com.holdenkarau.spark.testing.SharedSparkContext
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

class WordFrequencyApplicationAcceptance extends FlatSpec with Matchers with EmbeddedKafka with SharedSparkContext with BeforeAndAfter {

  /*
  a) Read text from kafka topic.
  b) Find word frequency in text.
  c) Write frequency to HDFS.
   */

  before {
    val conf = sc.hadoopConfiguration
    println(s"BEFORE: config = $conf")
    println(s"BEFORE: dataDir = ${conf.getStrings("dfs.datanode.data.dir", "none").head}")
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    println(s"BEFORE: home = ${fs.getHomeDirectory.toString}")
  }

  "Word frequency counter" should "find the top 25 most used words in a file" in {
    val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 0, zooKeeperPort = 0)
    withRunningKafkaOnFoundPort(kafkaConfig) { implicit actualKafkaConfig =>
      // now a kafka broker is listening on actualKafkaConfig.kafkaPort
      publishStringMessageToKafka("topic", "message")




      val conf = sc.hadoopConfiguration
      println(s"INSIDE: config = $conf")
      println(s"INSIDE: dataDir = ${conf.getStrings("dfs.datanode.data.dir", "none").head}")
      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
      println(s"INSIDE: home = ${fs.getHomeDirectory.toString}")


      consumeFirstStringMessageFrom("topic") shouldBe "message"
    }





//    WordFrequencyApplication.doRun(sc, Array(
//
//    ))
  }

}
