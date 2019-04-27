package com.github.simplesteph.udemy.kafka.streams

import java.time.Instant
import java.util.Properties

import com.github.simplesteph.udemy.kafka.streams.internal.Generator
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import io.circe.generic.auto._
import io.circe.syntax._

import scala.util.Random

object GameGenerator extends App with Generator {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  case object KEN extends Character("Ken", "US")
  case object RYU extends Character("Ryu", "Japan")
  case object GEKI extends Character("Geki", "Japan")
  case object CHUNLI extends Character("ChunLi", "China")

  private val config: Map[String, AnyRef] = Map(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> "127.0.0.1:9092",

    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer], // producer acks
    ProducerConfig.ACKS_CONFIG -> "all", // strongest producing guarantee
    ProducerConfig.RETRIES_CONFIG -> "3",
    ProducerConfig.LINGER_MS_CONFIG -> "1", // leverage idempotent producer from Kafka 0.11 !
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "true", // ensure we don't push duplicates
  )

  val propreties = new Properties()
  propreties.putAll(config.asJava)

  private val producer = new KafkaProducer[String, String](propreties)

  def randomKey: String = Random.shuffle(Vector("X", "O")).head

  def newRandomImpact(challenger: Challenger): ProducerRecord[String, String] = {
      // { "damage" : 16 } (16 is a random number between 5 and 25 excluded)
    val damage = Random.nextInt(20) + 5

    // Instant.now() is to get the current time using Java 8
    val now = Instant.now()

    val hit = Hit(randomKey, challenger, damage, now)

    // format the hit as json {"key": "X", challenger: {"name": "Leo", ...}, ...}
    val jsonHit = hit.asJson

    new ProducerRecord[String, String]("arcade-contest", challenger.login, jsonHit.noSpaces)
  }

  sys.addShutdownHook {
    producer.flush()
    producer.close()
  }

  val challengers = Vector(
    Challenger("john", RYU),
    Challenger("steph", CHUNLI),
    Challenger("alice", KEN)
  )

  var i = 0
  while (true) {
    if (i % 10 == 0) logger info s"Producing batch: $i"

    challengers foreach { challenger =>
      producer.send(newRandomImpact(challenger))
      Thread.sleep(Random.nextInt(2000) + 500)
    }
    i += 1
  }
}
