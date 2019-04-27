package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import com.github.simplesteph.udemy.kafka.streams.internal.Generator
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random.shuffle

object GameGenerator extends App with Generator {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val properties = new Properties()

  properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
  properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.put(ProducerConfig.RETRIES_CONFIG, "0")
  properties.put(ProducerConfig.ACKS_CONFIG, "0")

  val producer = new KafkaProducer[String, String](properties)

  sys.addShutdownHook {
    logger warn s"Shunting down the generator: ${getClass.getSimpleName.stripSuffix("$")}"
    producer.flush()
    producer.close()
  }

  logger info s"Starting the generator: ${getClass.getSimpleName.stripSuffix("$")}"

  while(true) {
    Thread.sleep((3 second) toMillis)

    (1 to 3).toVector.map { id =>
      val record = new ProducerRecord[String, String](
        "favourite-player-input", s"terminal-$id,${shuffle(StreetFighterCast).head.name}"
      )

      producer.send(record)
    }
  }
}
