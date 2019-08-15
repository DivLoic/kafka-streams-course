package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.{Logger, LoggerFactory}

object UserEventEnricherAppScala extends App {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val consumed: Consumed[String, String] = Consumed.`with`(Serdes.String(), Serdes.String())
  implicit val produces: Produced[String, String] = Produced.`with`(Serdes.String(), Serdes.String())

  val config: Properties = new Properties
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-event-enricher-app-scala")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[StringSerde])
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[StringSerde])

  val builder = new StreamsBuilder

  // we get a global table out of Kafka. This table will be replicated on each Kafka Streams application
  // the key of our globalKTable is the user ID
  val usersGlobalTable = builder.globalTable("user-table")

  // we get a stream of user purchases
  val userPurchases = builder.stream("user-purchases")

  // we want to enrich that stream
  val userPurchasesEnrichedJoin: KStream[String, String] = userPurchases.join(usersGlobalTable)(
    (key: String, value: String) => key, /* map from the (key, value) of this stream to the key of the GlobalKTable */
    (userPurchase: String, userInfo: String) => s"""{"purchase":$userPurchase,"user-info":$userInfo}"""
  )

  userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join-scala")

  // we want to enrich that stream using a Left Join
  val userPurchasesEnrichedLeftJoin =
    userPurchases.leftJoin(usersGlobalTable)(
      (key, _: String) => key, /* map from the (key, value) of this stream to the key of the GlobalKTable */
      (userPurchase: String, userInfo: String) => {
        // as this is a left join, userInfo can be null
        if (userInfo != null)
          s"""{"purchase":$userPurchase,"user-info":$userInfo}"""
        else
          s"""{"purchase":$userPurchase,"user-info":null}"""
      }
    )

  userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join-scala")

  val streams = new KafkaStreams(builder.build(), config)

  streams.cleanUp() // only do this in dev - not in prod
  streams.start()

  // print the topology
  logger info builder.build().describe().toString

  // shutdown hook to correctly close the streams application
  sys.addShutdownHook {
    streams.close()
  }
}
