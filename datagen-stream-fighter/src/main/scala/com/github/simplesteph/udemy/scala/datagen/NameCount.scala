package com.github.simplesteph.udemy.scala.datagen

import com.github.simplesteph.udemy.scala.datagen.Dataset.StreetFighterCast
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random.shuffle

sealed trait NameCount {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val config: Map[String, AnyRef] = Map[String, AnyRef](
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ProducerConfig.RETRIES_CONFIG -> "0",
    ProducerConfig.ACKS_CONFIG -> "0"
  )

  val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](config asJava)

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

object NameCountOne extends App with NameCount {

  def targetTopic = "streams-plaintext-input"

  run()
}

object NameCountTwo extends App with NameCount {

  def targetTopic = "word-count-input"

  run()
}