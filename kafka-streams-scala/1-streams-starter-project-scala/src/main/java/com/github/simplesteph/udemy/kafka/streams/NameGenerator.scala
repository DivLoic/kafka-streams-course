package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import com.github.simplesteph.udemy.kafka.streams.Dataset.StreetFighterCast
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random.shuffle


// ADD THE JAVA VERISON OF THIS
sealed trait NameCount {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val properties = new Properties()

  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.put(ProducerConfig.RETRIES_CONFIG, "0")
  properties.put(ProducerConfig.ACKS_CONFIG, "0")

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

  def targetTopic: String

  def generate: ProducerRecord[String, String] = new ProducerRecord(
    targetTopic,
    shuffle(StreetFighterCast).take(2).map(_.name).reduce((a, b) => s"$a vs $b")
  )

  def run(): Unit = {
    sys.addShutdownHook {
      logger warn s"Shunting down the generator: ${getClass.getSimpleName.stripSuffix("$")}"
      producer.flush()
      producer.close()
    }

    logger info s"Starting the generator: ${getClass.getSimpleName.stripSuffix("$")}"
    while (true) {
      producer.send(generate)
      Thread.sleep((1 second) toMillis)
    }
  }
}


// move in a 0 module
object NameCountOne extends App with NameCount {

  def targetTopic = "streams-plaintext-input"

  run()
}

object NameCountTwo extends App with NameCount {

  def targetTopic = "word-count-input"

  run()
}