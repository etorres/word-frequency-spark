package es.eriktorr.katas

import java.time.Duration
import java.util.concurrent.TimeUnit

import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.language.implicitConversions

class WordFrequencyApplicationAcceptance extends FlatSpec with Matchers with ForAllTestContainer with SharedSparkContext {

  /*
  a) Read text from kafka topic.
  b) Find word frequency in text.
  c) Write frequency to HDFS.
   */

  override val container = KafkaContainer()
  private val topic = "word-frequency"

  "Word frequency counter" should "find the top 25 most used words a text read from Kafka" in {
    sendTextToKafka("data/the-fall-of-the-house-of-usher.txt", container.kafkaContainer.getBootstrapServers)

    WordFrequencyApplication.doRun(sc, Array(
      container.kafkaContainer.getBootstrapServers,
      topic))


    //      val conf = sc.hadoopConfiguration
    //      println(s"INSIDE: config = $conf")
    //      println(s"INSIDE: dataDir = ${conf.getStrings("dfs.datanode.data.dir", "none").head}")
    //      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    //      println(s"INSIDE: home = ${fs.getHomeDirectory.toString}")
    //
    //      consumeFirstStringMessageFrom("topic") shouldBe "message"


  }

  private def sendTextToKafka(fileName: String, bootstrapServers: String): Unit = {
    val producer = topicProducer(bootstrapServers)
    for (line <- pathToFile andThen readFile apply fileName) {
      producer.send(new ProducerRecord[String, String](topic, line))
        .get(1000L, TimeUnit.MILLISECONDS)
    }
    producer.close(Duration.ofMillis(1000L))
  }

  private def topicProducer(bootstrapServers: String): KafkaProducer[String, String] = {
    val kafkaConfiguration = Map[String, AnyRef](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers
    ).asJava

    val keySerializer = new StringSerializer
    val valueSerializer = new StringSerializer

    new KafkaProducer[String, String](kafkaConfiguration, keySerializer, valueSerializer)
  }

  private val pathToFile: String => String = {
    getClass.getClassLoader.getResource(_).getPath
  }

  private val readFile: String => List[String] = (fileName: String) => {
    val source = Source.fromFile(fileName)
    val lines = source.getLines
      .flatMap((line: String) => line.split(","))
      .filter(_.nonEmpty)
      .toList
    source.close()
    lines
  }

}
