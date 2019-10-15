package es.eriktorr.katas

import java.util.concurrent.TimeUnit

import com.holdenkarau.spark.testing.SharedSparkContext
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

class WordFrequencyApplicationAcceptance extends FlatSpec
  with Matchers with BeforeAndAfter
  with EmbeddedKafka with SharedSparkContext {

  /*
  a) Read text from kafka topic.
  b) Find word frequency in text.
  c) Write frequency to HDFS.
   */

  private val topic = "word-frequency"

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
      val bootstrapServers = s":${actualKafkaConfig.kafkaPort}"

      val producer = topicProducer(bootstrapServers)
      val recordMetadata = producer.send(new ProducerRecord[String, String](topic, "this is a message"))
        .get(1000L, TimeUnit.MILLISECONDS)
      println(s"\n\n >> HERE[producer]: topic=${recordMetadata.topic()}, partition=${recordMetadata.partition()}, offset=${recordMetadata.offset()}\n")

      WordFrequencyApplication.doRun(sc, Array(
        bootstrapServers,
        topic))

      //
      //
      //
      //      val conf = sc.hadoopConfiguration
      //      println(s"INSIDE: config = $conf")
      //      println(s"INSIDE: dataDir = ${conf.getStrings("dfs.datanode.data.dir", "none").head}")
      //      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
      //      println(s"INSIDE: home = ${fs.getHomeDirectory.toString}")
      //
      //
      //      consumeFirstStringMessageFrom("topic") shouldBe "message"
    }

  }

  private def topicProducer(bootstrapServers: String): KafkaProducer[String, String] = {
    val kafkaConfiguration = Map[String, AnyRef](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> "kafka-sandbox"
    ).asJava

    val keySerializer = new StringSerializer
    val valueSerializer = new StringSerializer

    new KafkaProducer[String, String](kafkaConfiguration, keySerializer, valueSerializer)
  }

}
