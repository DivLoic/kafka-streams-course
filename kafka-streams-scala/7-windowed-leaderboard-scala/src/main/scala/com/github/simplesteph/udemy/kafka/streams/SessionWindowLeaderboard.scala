package com.github.simplesteph.udemy.kafka.streams

import java.time.{Duration, Instant}
import java.util.Properties

import com.github.simplesteph.{Victories, Victory}
import io.circe.syntax._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.{GenericAvroSerde, SpecificAvroSerde}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

class SessionWindowLeaderboard extends App {

  private val logger = LoggerFactory.getLogger(classOf[SessionWindowLeaderboard])

  private val initializer = new Victories(0L, Instant.ofEpochMilli(-1L))

  private val aggregator = (_: String, victory: Victory, victories: Victories) =>
    new Victories(victories.count + 1, victory.datetime)

  private val merger = (_: String, aggOne: Victories, aggTwo: Victories) => new Victories(
    aggOne.count + aggTwo.count,
    if (aggOne.latest.isAfter(aggTwo.latest)) aggOne.latest else aggTwo.latest
  )

  val properties = new Properties

  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-window-app")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0)

  properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081")

  val builder = new StreamsBuilder

  val victorySerde = new SpecificAvroSerde[Victory]
  val victoriesSerde = new SpecificAvroSerde[Victories]
  val consumed = Consumed.`with`(Serdes.String, victorySerde)

  val victories = builder.stream("victories")(consumed)

  val inactivityGap = Duration.ofSeconds(30L)

  val windows = SessionWindows.`with`(inactivityGap)

  val windowedStringSerde = WindowedSerdes.timeWindowedSerdeFrom(classOf[String])

  victories
    .selectKey((key: String, value: Victory) =>
      // {"id-terminal": 101, "character": "ryu"}
      Map(key -> value.character.name).asJson.toString()
    )

    .groupByKey(Grouped.`with`(Serdes.String, victorySerde))

    .windowedBy(windows)

    .aggregate(initializer)(aggregator, merger)(Materialized.`with`(Serdes.String, victoriesSerde))

    .toStream.to("tumbling-window-victories")(Produced.`with`(windowedStringSerde, victoriesSerde))

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
