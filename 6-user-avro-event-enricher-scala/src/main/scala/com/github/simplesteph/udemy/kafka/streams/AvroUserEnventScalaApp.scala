package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object AvroUserEnventScalaApp extends App {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val valueAvroSerde = new GenericAvroSerde
  val keyAvroSerde = new GenericAvroSerde

  valueAvroSerde.configure(Map("schema.registry.url" -> "http://localhost:8081").asJava, false)
  keyAvroSerde.configure(Map("schema.registry.url" -> "http://localhost:8081").asJava, true)

  implicit val consumed = Consumed.`with`(valueAvroSerde, keyAvroSerde)
  implicit val produces = Produced.`with`(valueAvroSerde, keyAvroSerde)

  val config: Properties = new Properties
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-avro-event-enricher-app-scala")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[GenericRecord])
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericRecord])

  val builder = new StreamsBuilder

  // we get a global table out of Kafka. This table will be replicated on each Kafka Streams application
  // the key of our globalKTable is the user ID
  val usersGlobalTable: GlobalKTable[GenericRecord, GenericRecord] = builder.globalTable("user-avro-table")

  // we get a stream of user purchases
  val userPurchases = builder.stream("user-avro-purchases")

  // we want to enrich that stream
  val userPurchasesEnrichedJoin: KStream[GenericRecord, GenericRecord] = userPurchases.join(usersGlobalTable)(
    // map from the (key, value) of this stream to the key of the GlobalKTable
    //
    (key: GenericRecord, value: GenericRecord) => key.get("user-key").asInstanceOf[GenericRecord],

    (userPurchase: GenericRecord, userInfo: GenericRecord) => {
      ???
    }
  )

  userPurchasesEnrichedJoin.to("user-avro-purchases-enriched-scala")

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
