package com.github.simplesteph.udemy.scala.datagen

import com.github.simplesteph.udemy.scala.datagen.Dataset.StreetFighterCast
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random.shuffle

object FavouritePlayer extends App {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val config: Map[String, AnyRef] = Map[String, AnyRef](
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    ProducerConfig.RETRIES_CONFIG -> "0",
    ProducerConfig.ACKS_CONFIG -> "0"
  )

  val producer = new KafkaProducer[String, String](config asJava)

  def targetTopic = "favourite-player-input"

  def generate(host: String): ProducerRecord[String, String] = new ProducerRecord[String, String](
    targetTopic,
    s"$host,${shuffle(StreetFighterCast).head.name}"
  )

  sys.addShutdownHook {
    logger warn s"Shunting down the generator: ${getClass.getSimpleName.stripSuffix("$")}"
    producer.flush()
    producer.close()
  }

  logger info s"Starting the generator: ${getClass.getSimpleName.stripSuffix("$")}"

  while(true) {
    (1 to 3).toVector.map { id =>

      producer.send(generate(s"terminal-$id"))

    }
    Thread.sleep((3 second) toMillis)
  }
}
