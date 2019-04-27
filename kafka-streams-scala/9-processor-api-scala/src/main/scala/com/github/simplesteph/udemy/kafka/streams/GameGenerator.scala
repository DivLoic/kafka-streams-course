package com.github.simplesteph.udemy.kafka.streams

import java.nio.charset.StandardCharsets
import java.util.Properties

import com.github.simplesteph.udemy.kafka.streams.internal.Generator
import com.github.simplesteph.{MachineId, Reward, Victory}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object GameGenerator extends App with Generator {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val properties: Properties = new Properties()

  val machineIdSerde: SpecificAvroSerde[MachineId] = new SpecificAvroSerde()
  val victorySerde: SpecificAvroSerde[Victory] = new SpecificAvroSerde()
  val rewardSerde: SpecificAvroSerde[Reward] = new SpecificAvroSerde()

  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")

  properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3")
  properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
  properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

  val registryConf = Map(SCHEMA_REGISTRY_URL_CONFIG -> "http://127.0.0.1:8081")

  machineIdSerde.configure(registryConf.asJava, true)
  victorySerde.configure(registryConf.asJava, false)
  rewardSerde.configure(registryConf.asJava, false)

  val victoryProducer: Producer[MachineId, Victory] = new KafkaProducer(
    properties,
    machineIdSerde.serializer(),
    victorySerde.serializer()
  )

  val rewardProducer = new KafkaProducer(
    properties,
    machineIdSerde.serializer(),
    rewardSerde.serializer()
  )

  val termNumber: Int = 10

  val rewards: Vector[Reward] = nextRewards(termNumber)

  rewards
    .map(reward => new ProducerRecord("processor-rewards-scala", new MachineId(reward.idTerm), reward))
    .foreach(rewardProducer.send)

  rewardProducer.flush()

  var i: Int = 0

  while (true) {

    Thread.sleep(1000L)

    nextVictory.foreach { victory =>

      val record: ProducerRecord[MachineId, Victory] =
        new ProducerRecord("processor-victories", new MachineId(victory.terminal), victory)

      if (i % 3 == 0) victoryProducer.send(addRandomValidationStatus(record))
      else victoryProducer.send(record)

      if (i % 10 == 0) logger.info(s"Sending the ${i}th victory from the game terminals")
    }

    i = i + 1
  }

  private def addRandomValidationStatus(record: ProducerRecord[MachineId, Victory]) = {
    if (!nextValidStatus) {
      val newRecord = new ProducerRecord[MachineId, Victory](
        record.topic(),
        record.key(),
        record.value().copy(terminal = "XXX")
      )
      newRecord.headers().add(new RecordHeader("status", "invalid".getBytes(StandardCharsets.UTF_8)))
      newRecord

    } else {
      record.headers().add(new RecordHeader("status", "valid".getBytes(StandardCharsets.UTF_8)))
      record
    }
  }
}
