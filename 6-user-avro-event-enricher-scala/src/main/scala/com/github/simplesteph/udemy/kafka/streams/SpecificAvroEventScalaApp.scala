package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import com.github.simplesteph._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.{GenericAvroSerde, SpecificAvroSerde}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.GlobalKTable
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._


object SpecificAvroEventScalaApp extends App {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val userSerde = new SpecificAvroSerde[User]()
  val userKeySerde = new SpecificAvroSerde[UserKey]()
  val purchaseSerde = new SpecificAvroSerde[Purchase]()
  val purchaseKeySerde = new SpecificAvroSerde[PurchaseKey]()

  // output schema
  val salesDescriptionSerde = new SpecificAvroSerde[SalesDescription]()

  userKeySerde :: purchaseKeySerde :: Nil foreach {
    _.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081").asJava, true)
  }
  userSerde :: purchaseSerde :: salesDescriptionSerde :: Nil foreach {
    _.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://localhost:8081").asJava, false)
  }

  implicit val userConsumed: Consumed[UserKey, User] = Consumed.`with`(userKeySerde, userSerde)
  implicit val purchaseConsumed: Consumed[PurchaseKey, Purchase] = Consumed.`with`(purchaseKeySerde, purchaseSerde)

  implicit val produces: Produced[PurchaseKey, SalesDescription] =
    Produced.`with`(purchaseKeySerde, salesDescriptionSerde)

  val config: Properties = new Properties
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "specific-avro-app-scala")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")

  val builder = new StreamsBuilder()

  // we get a global table out of Kafka. This table will be replicated on each Kafka Streams application
  // the key of our globalKTable is the user ID
  val usersGlobalTable: GlobalKTable[UserKey, User] = builder.globalTable("user-avro-table")

  // we get a stream of user purchases
  val userPurchases: KStream[PurchaseKey, Purchase] = builder.stream("user-avro-purchases")

  // we want to enrich that stream

  val userPurchasesEnrichedJoin = userPurchases.join(usersGlobalTable)(

    // map from the (key, value) of this stream to the key of the GlobalKTable
    (key: PurchaseKey, _: Purchase) => key.getClientId,

    (userPurchase: Purchase, userInfo: User) => {
      new SalesDescription(
        userPurchase.getGame,
        userPurchase.getTwoPlayer,
        userInfo.getLogin,
        userInfo.getFirstName,
        userInfo.getLastName,
        userInfo.getEmail
      )
    }
  )

  userPurchasesEnrichedJoin.to("user-purchases-avro-inner-join-scala")

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


