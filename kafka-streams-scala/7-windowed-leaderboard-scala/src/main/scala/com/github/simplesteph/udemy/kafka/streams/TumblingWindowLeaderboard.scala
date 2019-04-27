package com.github.simplesteph.udemy.kafka.streams

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneId}
import java.util.Properties

import com.github.simplesteph._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.{GenericAvroSerde, SpecificAvroSerde}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

object TumblingWindowLeaderboard extends App {

  private val logger = LoggerFactory.getLogger(getClass)

  private val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault)

  private val initializer = Victories(0L)

  private val aggregator = (_: CharacterKey, victory: Victory, victories: Victories) =>
    Victories(victories.count + 1, latest = Try(fmt.format(victory.timestamp)).toOption)

  val properties = new Properties
  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "tumbling-window-scala-app")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

  properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081")

  val registryConf: Map[String, AnyRef] = Map(
    AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://127.0.0.1:8081")

  val victorySerde = new SpecificAvroSerde[Victory]
  val victoriesSerde = new SpecificAvroSerde[Victories]
  val machineIdSerde = new SpecificAvroSerde[MachineId]
  val characterKeySerde = new SpecificAvroSerde[CharacterKey]

  victorySerde.configure(registryConf.asJava, false)

  victoriesSerde.configure(registryConf.asJava, false)

  machineIdSerde.configure(registryConf.asJava, true)

  characterKeySerde.configure(registryConf.asJava, true)

  val consumed = Consumed.`with`(machineIdSerde, victorySerde)
    .withTimestampExtractor(new VictoryTimestampKeyExtractor)

  val windowSize = Duration.ofSeconds(10L)

  // /!\ here the step of the window is different than it's size
  val windowStep = Duration.ofSeconds(10L)

  val windows = TimeWindows.of(windowSize).advanceBy(windowStep)

  val windowedStringSerde =
    new WindowedSerdes.TimeWindowedSerde[CharacterKey](characterKeySerde, windowSize.toMillis);

  val builder = new StreamsBuilder

  val victories = builder.stream("windowed-victories-scala")(consumed)

  victories

    .filter((_: MachineId, value: Victory) => value.game == Game.StreetFighter)

    .selectKey((_: MachineId, value: Victory) => CharacterKey(value.character.name))

    .groupByKey(Grouped.`with`(characterKeySerde, victorySerde))

    .windowedBy(windows)

    .aggregate(initializer)(aggregator)(Materialized.`with`(characterKeySerde, victoriesSerde))

    .toStream

    .mapValues(moveWindowStartToValue(_, _))

    .to("tumbling-window-victories-scala")(Produced.`with`(windowedStringSerde, victoriesSerde))

  val streams = new KafkaStreams(builder.build, properties)
  streams.cleanUp() // only do this in dev - not in prod
  streams.start()

  // print the topology
  logger.info(builder.build.describe.toString)
  // shutdown hook to correctly close the streams application

  sys.addShutdownHook {
    streams.close()
  }

  private def moveWindowStartToValue(key: Windowed[CharacterKey], value: Victories): Victories = {
    val statingBound = key.window.startTime
    val startingTime = statingBound.atZone(ZoneId.systemDefault)
    value.copy(window_start = Some(fmt.format(startingTime)))
  }
}
