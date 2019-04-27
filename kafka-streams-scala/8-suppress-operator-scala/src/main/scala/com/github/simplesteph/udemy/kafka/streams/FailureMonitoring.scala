package com.github.simplesteph.udemy.kafka.streams

import java.time.format.DateTimeFormatter
import java.time.{Duration, ZoneId}
import java.util.Collections.singletonMap
import java.util.Properties

import com.github.simplesteph.{ErrorEvents, Game, MachineId, Victory}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.avro.{GenericAvroSerde, SpecificAvroSerde}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

object FailureMonitoring extends App {

  private val logger = LoggerFactory.getLogger(getClass)

  private val initializer = ErrorEvents(0L, None)

  private val aggregator = (_: MachineId, _: Victory, victories: ErrorEvents) => ErrorEvents(victories.count + 1, None)

  private val fmt = DateTimeFormatter.ofPattern("HH:mm:ss")

  val properties = new Properties

  val victorySerde = new SpecificAvroSerde[Victory]
  val machineIdSerde = new SpecificAvroSerde[MachineId]
  val errorEventSerde = new SpecificAvroSerde[ErrorEvents]

  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "warning-signal-monitoring-scala-app")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

  properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081")

  val registryConf = singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081")

  machineIdSerde.configure(registryConf, true)

  victorySerde.configure(registryConf, false)

  errorEventSerde.configure(registryConf, false)

  val builder = new StreamsBuilder

  val consumed = Consumed.`with`(machineIdSerde, victorySerde)
    .withTimestampExtractor(new VictoryTimestampKeyExtractor)

  val victories = builder.stream("suppress-victories")(consumed)

  val windowSize = Duration.ofSeconds(10L)

  val windowStep = Duration.ofSeconds(10L)

  val gracePeriod = Duration.ofSeconds(10L)

  val windows = TimeWindows.of(windowSize).advanceBy(windowStep).grace(gracePeriod)

  val windowedMachineIdSerde = new WindowedSerdes.TimeWindowedSerde[MachineId](machineIdSerde, windowSize.toMillis)

  victories
    // victory without a correct game name is an error
    .filter((_: MachineId, value: Victory) => value.game == Game.None)

    // we count ...
    .groupByKey(Grouped.`with`(machineIdSerde, victorySerde))

    // over a window ...
    .windowedBy(windows)

    // the number of errors
    .aggregate(initializer)(aggregator)(Materialized.`with`(machineIdSerde, errorEventSerde))

    // we suppress old versions of the windows
    .suppress(Suppressed.untilWindowCloses(unbounded))

    // we add the starting bound of the window in the value
    .toStream

    .map((key: Windowed[MachineId], value: ErrorEvents) => {
      val statingBound = key.window.startTime
      val startingTime = statingBound.atZone(ZoneId.systemDefault)
      (key, value.copy(window_start = Some(startingTime.format(fmt))))
    })

    .to("suppress-errors-count-scala")(Produced.`with`(windowedMachineIdSerde, errorEventSerde))

  val streams = new KafkaStreams(builder.build, properties)
  streams.cleanUp() // only do this in dev - not in prod
  streams.start()

  // print the topology
  logger.info(builder.build.describe.toString)

  // shutdown hook to correctly close the streams application
  sys.addShutdownHook {
    streams.close()
  }
}
