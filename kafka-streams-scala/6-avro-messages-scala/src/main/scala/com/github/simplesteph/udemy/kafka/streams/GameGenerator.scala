package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import com.github.simplesteph._
import com.github.simplesteph.udemy.kafka.streams.internal.Generator
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object GameGenerator extends App with Generator {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  val properties = new Properties
  // kafka bootstrap server
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  // producer acks
  properties.setProperty(ProducerConfig.ACKS_CONFIG, "all") // strongest producing guarantee

  properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3")
  properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
  // leverage idempotent producer from Kafka 0.11 !
  properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true") // ensure we don't push duplicates

  val userSerde = new SpecificAvroSerde[User]()
  val userKeySerde = new SpecificAvroSerde[UserKey]()
  val purchaseSerde = new SpecificAvroSerde[Purchase]()
  val purchaseKeySerde = new SpecificAvroSerde[PurchaseKey]()

  userKeySerde :: purchaseKeySerde :: Nil foreach {
    _.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://127.0.0.1:8081").asJava, true)
  }
  userSerde :: purchaseSerde :: Nil foreach {
    _.configure(Map(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG -> "http://127.0.0.1:8081").asJava, false)
  }

  val userProducer = new KafkaProducer[UserKey, User](
    properties,
    userKeySerde.serializer(),
    userSerde.serializer()
  )

  val purchaseProducer = new KafkaProducer[PurchaseKey, Purchase](
    properties,
    purchaseKeySerde.serializer(),
    purchaseSerde.serializer()
  )

  val users = Vector[User](
    User("jdoe", "john", Some("Doe"), Some("john.doe@gmail.com")),
    User("jojo", "Johnny", Some("Doe"), Some("johnny.doe@gmail.com")),
    User("simplesteph", "Stephane", Some("Maarek")),
    User("alice", "Alice"),
    User("will01", "Will", Some("Byers"), Some("will@st.com")),
    User("kali02", "Kali", Some("Eight"), Some("kali@st.com")),
    User("lucas03", "Lucas", Some("Sinclair"), Some("lucas@st.com")),
    User("jon04", "Jonathan", Some("Byers"), Some("jonathan@st.com")),
    User("dustin05", "Dustin", Some("Henderson"), Some("dustin@st.com")),
    User("mike06", "Mike", Some("Wheeler"), Some("mike@st.com"))
  )

  /* produce all users in a compacted topic (to build a ktable)  */
  users
    .map(user => new ProducerRecord[UserKey, User]("avro-user-table", UserKey(user.login), user))
    .foreach(userProducer.send)

  userProducer.flush()

  var i = 0

  /* produce purchase in topic (to build a kstream) */
  while (true) {

    Thread.sleep(1000)

    if (i % 10 == 0) logger info s"Generating the ${i}th Purchase"

    for {
      game <- nextGame
      user <- nexUser(users)
      id <- nextPurchaseId.orElse(Some("X"))
      isTwoPlayer <- nextIsTwoPlayer.orElse(Some(true))
    } yield {

      val key: PurchaseKey = new PurchaseKey(id, new UserKey(user.login))

      val value: Purchase = new Purchase(user.login, game, isTwoPlayer)

      if (i % 10 == 0) logger info s"Sending the ${i}th Producer Record"

      val record = new ProducerRecord[PurchaseKey, Purchase]("avro-user-purchases", key, value)

      purchaseProducer.send(record)
    }

    i += 1
  }

}
