package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object GenericAvroEventScalaApp extends App {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val avroKeySerde = new GenericAvroSerde()
  val avroValueSerde = new GenericAvroSerde()

  val SalesDescriptionSchema: Schema = new Parser().parse(getClass.getResource("avro/sales-description.avsc").getPath)

  avroKeySerde
    .configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081").asJava, true)

  avroValueSerde
    .configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081").asJava, false)

  implicit val consumed: Consumed[GenericRecord, GenericRecord] = Consumed.`with`(avroKeySerde, avroValueSerde)
  implicit val produced: Produced[GenericRecord, GenericRecord] = Produced.`with`(avroKeySerde, avroValueSerde)

  val config: Properties = new Properties
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "generic-avro-scala-app")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[StringSerde])
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[StringSerde])
  config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")

  val builder = new StreamsBuilder()

  // we get a global table out of Kafka. This table will be replicated on each Kafka Streams application
  // the key of our globalKTable is the user ID
  val usersGlobalTable: GlobalKTable[GenericRecord, GenericRecord] = builder.globalTable("user-table-avro")

  // we get a stream of user purchases
  val userPurchases: KStream[GenericRecord, GenericRecord] = builder.stream("user-purchases-avro")

  // we want to enrich that stream
  val userPurchasesEnrichedJoin: KStream[GenericRecord, GenericRecord] = userPurchases.join(usersGlobalTable)(
    /* map from the (key, value) of this stream to the key of the GlobalKTable */
    (key: GenericRecord, _: GenericRecord) => key.get("client_id").asInstanceOf[GenericRecord],

    (userPurchase: GenericRecord, userInfo: GenericRecord) => createSaleDescription(userPurchase , userInfo)
  )

  userPurchasesEnrichedJoin.to("generic-avro-purchases-join-scala")

  val streams = new KafkaStreams(builder.build(), config)

  streams.cleanUp() // only do this in dev - not in prod
  streams.start()

  // print the topology
  logger info builder.build().describe().toString

  // shutdown hook to correctly close the streams application
  sys.addShutdownHook {
    streams.close()
  }

  def createSaleDescription(key: GenericRecord, value: GenericRecord): GenericRecord = {
    val record = new GenericData.Record(SalesDescriptionSchema)

    record.put("game", value.get("game"))
    record.put("is_two_player", value.get("is_two_player"))
    record.put("first_name", key.get("first_name"))
    record.put("last_name", key.get("last_name"))
    record.put("email", key.get("email"))
    record.put("login", key.get("login"))

    record
  }
}
