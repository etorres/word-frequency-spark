package es.eriktorr.katas

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

class WordFrequencyApplicationAcceptance extends FlatSpec
  with Matchers with BeforeAndAfter
  with ForAllTestContainer with SharedSparkContext {

  /*
  a) Read text from kafka topic.
  b) Find word frequency in text.
  c) Write frequency to HDFS.
   */

  override val container = KafkaContainer()
  private val topic = "word-frequency"

  before {
    val conf = sc.hadoopConfiguration
    println(s"BEFORE: config = $conf")
    println(s"BEFORE: dataDir = ${conf.getStrings("dfs.datanode.data.dir", "none").head}")
    val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    println(s"BEFORE: home = ${fs.getHomeDirectory.toString}")
  }

  "Word frequency counter" should "find the top 25 most used words in a message" in {
    val bootstrapServers = container.kafkaContainer.getBootstrapServers

    val producer = topicProducer(bootstrapServers)
    producer.send(new ProducerRecord[String, String](topic, "this is a message"))
      .get(1000L, TimeUnit.MILLISECONDS)
    producer.close(Duration.ofMillis(1000L))

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

  private def topicProducer(bootstrapServers: String): KafkaProducer[String, String] = {
    val kafkaConfiguration = Map[String, AnyRef](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers
    ).asJava

    val keySerializer = new StringSerializer
    val valueSerializer = new StringSerializer

    new KafkaProducer[String, String](kafkaConfiguration, keySerializer, valueSerializer)
  }

}
