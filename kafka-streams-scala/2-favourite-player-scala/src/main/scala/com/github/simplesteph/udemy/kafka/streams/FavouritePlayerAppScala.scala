package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, KTable, Materialized, Produced}
import org.slf4j.LoggerFactory

object FavouritePlayerAppScala extends App {

    val logger = LoggerFactory.getLogger(getClass)

    val config: Properties = new Properties
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-player-scala")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

    // for a immediate feed back when using the generator
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")

    // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

    val builder: StreamsBuilder = new StreamsBuilder

    // Step 1: We create the topic of users keys to players
    val textLines: KStream[String, String] = builder.stream[String, String]("favourite-player-input")(
      Consumed.`with`(Serdes.String, Serdes.String)
    )

    val usersAndColours: KStream[String, String] = textLines
      // 1 - we ensure that a comma is here as we will split on it
      .filter((_: String, value: String) => value.contains(","))
      // 2 - we select a key that will be the user id (lowercase for safety)
      .selectKey[String]((_: String, value: String) => value.split(",")(0).toLowerCase)
      // 3 - we get the player from the value (lowercase for safety)
      .mapValues[String]((value: String) => value.split(",")(1).toLowerCase)
      // 4 - we filter undesired players (could be a data sanitization step)
      .filter((user: String, player: String) => List("ryu", "ken", "chunli").contains(player))

    val intermediaryTopic = "terminal-keys-and-players-scala"
    usersAndColours.to(intermediaryTopic)(Produced.`with`(Serdes.String, Serdes.String))

    // step 2 - we read that topic as a KTable so that updates are read correctly
    val usersAndColoursTable: KTable[String, String] = builder.table(intermediaryTopic)(
      Consumed.`with`(Serdes.String, Serdes.String)
    )

    // step 3 - we count the occurrences of players
    val favouriteColours: KTable[String, Long] = usersAndColoursTable
      // 5 - we group by player within the KTable
      .groupBy((user: String, player: String) => (player, player))(Grouped.`with`(Serdes.String, Serdes.String))
      .count()(Materialized.as("CountsByPlayersScala")(Serdes.String, Serdes.Long))

    // 6 - we output the results to a Kafka Topic - don't forget the serializers
    favouriteColours.toStream.to("favourite-player-output-scala")(Produced.`with`(Serdes.String, Serdes.Long))

    val streams: KafkaStreams = new KafkaStreams(builder.build, config)
    streams.cleanUp()
    streams.start()

    // print the topology
    logger info streams.toString

    // shutdown hook to correctly close the streams application
    sys.addShutdownHook {
      streams.close()
    }
}
