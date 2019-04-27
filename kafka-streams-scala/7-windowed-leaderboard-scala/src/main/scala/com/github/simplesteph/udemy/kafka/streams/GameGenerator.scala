package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import com.github.simplesteph.udemy.kafka.streams.internal.Generator
import com.github.simplesteph.{MachineId, Victory}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object GameGenerator extends App with Generator {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val properties: Properties = new Properties()

  val machineIdSerde: SpecificAvroSerde[MachineId] = new SpecificAvroSerde()
  val victorySerde: SpecificAvroSerde[Victory] = new SpecificAvroSerde()

  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")

  properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3")
  properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
  properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

  val registryConf = Map(SCHEMA_REGISTRY_URL_CONFIG -> "http://127.0.0.1:8081")

  machineIdSerde.configure(registryConf.asJava, true)
  victorySerde.configure(registryConf.asJava, false)

  val victoryProducer: Producer[MachineId, Victory] = new KafkaProducer(
    properties,
    machineIdSerde.serializer(),
    victorySerde.serializer()
  )

  val termNumber: Int = 10

  var i: Int = 0
  while (true) {
    Thread.sleep(100L * nextDelay.getOrElse(2))

    nextVictory.foreach { victory =>

      val record: ProducerRecord[MachineId, Victory] =
        new ProducerRecord("windowed-victories-scala", new MachineId(victory.terminal), victory)

      victoryProducer.send(record)

      if (i % 10 == 0) logger.info(s"Sending the ${i}th victory from the game terminals")
    }

    i = i + 1
  }
}
