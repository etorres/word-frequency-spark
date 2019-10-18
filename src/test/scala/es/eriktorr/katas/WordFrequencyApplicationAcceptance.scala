package es.eriktorr.katas

import java.net.URI
import java.util.concurrent.TimeUnit

import com.dimafeng.testcontainers.{ForAllTestContainer, KafkaContainer}
import com.holdenkarau.spark.testing.{HDFSCluster, SharedSparkContext}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.io.Source
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

  private var hdfsCluster: HDFSCluster = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    hdfsCluster = new HDFSCluster
    hdfsCluster.startHDFS()

    checkpointDirectory apply "word-frequency-query"
  }

  override def afterAll() {
    hdfsCluster.shutdownHDFS()
    super.afterAll()
  }

  "Word frequency counter" should "find the top 25 most used words a text read from Kafka" in {
    sendTextToKafka("data/the-fall-of-the-house-of-usher.txt", container.kafkaContainer.getBootstrapServers)

    WordFrequencyApplication.doRun(Array(
        container.kafkaContainer.getBootstrapServers,
        topic))


    //      val conf = sc.hadoopConfiguration
    //      println(s"INSIDE: config = $conf")
    //      println(s"INSIDE: dataDir = ${conf.getStrings("dfs.datanode.data.dir", "none").head}")
    //      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
    //      println(s"INSIDE: home = ${fs.getHomeDirectory.toString}")
    //
    //      consumeFirstStringMessageFrom("topic") shouldBe "message"


    //    Thread.sleep(60000)
  }

  private def sendTextToKafka(fileName: String, bootstrapServers: String): Unit = {
    val producer = topicProducer(bootstrapServers)
    for (line <- pathToFile andThen readFile apply fileName) {
      producer.send(new ProducerRecord[String, String](topic, line))
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

  private val checkpointDirectory: String => Boolean = (checkpointName: String) => {
    val hadoopConfiguration = sc.hadoopConfiguration
    val nameNodeAddress = hadoopConfiguration.getStrings("dfs.namenode.servicerpc-address", "localhost:8020").head

    val hadoopFileSystem = FileSystem.get(URI.create(s"hdfs://$nameNodeAddress/user/erik_torres"), hadoopConfiguration)
    hadoopFileSystem.mkdirs(new Path(hadoopFileSystem.getWorkingDirectory, s"/checkpoints/$checkpointName"))
  }

}
