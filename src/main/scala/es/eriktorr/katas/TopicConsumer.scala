package es.eriktorr.katas

import java.time.Duration

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.language.implicitConversions

class TopicConsumer(private val bootstrapServers: String, private val topics: List[String]) {

  private val kafkaConfiguration = Map[String, AnyRef](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
    ConsumerConfig.GROUP_ID_CONFIG -> "kafka-sandbox",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> true.toString,
    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> 1000.toString
  ).asJava

  private val keyDeserializer = new StringDeserializer
  private val valueDeserializer = new StringDeserializer

  private val kafkaConsumer = new KafkaConsumer[String, String](kafkaConfiguration, keyDeserializer, valueDeserializer)

  kafkaConsumer.subscribe(topics.asJava)

  def consume: String = {

    // TODO
    for ((k,v) <- kafkaConsumer.listTopics().asScala) println(s"\n\n >> HERE[consumer]: topic=[key: $k, value: $v]\n")
    // TODO

    val records = kafkaConsumer.poll(Duration.ofMillis(1000L))
    records.asScala.map(_.value()).head
  }

}
