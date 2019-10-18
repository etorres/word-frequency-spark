package es.eriktorr.katas

import java.net.URI
import java.time.Duration
import java.util.concurrent.TimeUnit

import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.io.Source
import scala.language.implicitConversions

class WordFrequencyApplicationAcceptance extends FlatSpec
  with Matchers with BeforeAndAfter
  with ForAllTestContainer with SharedSparkContext {

  private val FileName = "data/the-fall-of-the-house-of-usher.txt"
  private val WordFrequency = Map(
    "the" -> 565,
    "of" -> 427,
    "and" -> 237,
    "i" -> 160,
    "a" -> 156,
    "in" -> 140,
    "to" -> 118,
    "which" -> 92,
    "his" -> 86,
    "was" -> 78
  )

  override val container = KafkaContainer()
  private val inTopics = "word-frequency-input"
  private val outTopics = "word-frequency-output"
  private var checkpointLocation: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    checkpointLocation = makeCheckpointDirectory apply WordFrequencyApplication.ApplicationName
  }

  "Word frequency counter" should "find the top 10 most used words a text read from Kafka" in {
    sendTextToKafka(FileName, container.kafkaContainer.getBootstrapServers)

    WordFrequencyApplication.doRun(Array(
      container.kafkaContainer.getBootstrapServers,
      inTopics,
      outTopics,
      checkpointLocation))

    consumeNumberMessagesFrom(container.kafkaContainer.getBootstrapServers, outTopics, 10) shouldBe WordFrequency
  }

  private def sendTextToKafka(fileName: String, bootstrapServers: String): Unit = {
    val producer = topicProducer(bootstrapServers)
    for (line <- pathToFile andThen readFile apply fileName) {
      producer.send(new ProducerRecord[String, String](inTopics, line))
        .get(1000L, TimeUnit.MILLISECONDS)
    }
    producer.close(1000L, TimeUnit.MILLISECONDS)
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

  /*
   * https://github.com/manub/scalatest-embedded-kafka/blob/master/embedded-kafka/src/main/scala/net/manub/embeddedkafka/EmbeddedKafka.scala
   */
  private def consumeNumberMessagesFrom(bootstrapServers: String, topic: String, number: Int): Map[String, Long] = {
    val consumer = topicConsumer(bootstrapServers)
    consumer.subscribe(listFrom(topic))

    val timeoutNs = System.nanoTime + 30.seconds.toNanos
    var messagesRead = 0

    var messages = Map[String, Long]()

    while (messagesRead < number && System.nanoTime < timeoutNs) {
      val records = consumer.poll(Duration.ofMillis(1000L)).asScala

      val pendingToRead = number - messagesRead
      for (record <- records.iterator.take(pendingToRead)) {
        messages += (record.key() -> record.value().toLong)
        messagesRead += 1
      }
    }

    consumer.close()
    messages
  }

  private def topicConsumer(bootstrapServers: String): KafkaConsumer[String, String] = {
    val kafkaConfiguration = Map[String, AnyRef](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.GROUP_ID_CONFIG -> "word-frequency-application-acceptance",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "1000"
    ).asJava

    val keyDeserializer = new StringDeserializer
    val valueDeserializer = new StringDeserializer

    new KafkaConsumer[String, String](kafkaConfiguration, keyDeserializer, valueDeserializer)
  }

  private def listFrom: String => java.util.List[String] = List(_).asJava

  private val makeCheckpointDirectory: String => String = (checkpointName: String) => {
    val hadoopFileSystem = FileSystem.get(
      URI.create(sc.getCheckpointDir.get),
      sc.hadoopConfiguration)
    val checkpointPath = new Path(sc.getCheckpointDir.get, checkpointName)
    hadoopFileSystem.mkdirs(checkpointPath)
    checkpointPath.toString
  }

}
