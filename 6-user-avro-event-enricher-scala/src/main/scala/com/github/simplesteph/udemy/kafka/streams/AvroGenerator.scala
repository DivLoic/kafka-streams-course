package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import com.github.simplesteph._
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalacheck.Gen
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object AvroGenerator extends App {

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

  val clients = Vector[User](
    User("jdoe", "john", Some("Doe"), Some("john.doe@gmail.com")),
    User("jojo", "Johnny", Some("Doe"), Some("johnny.doe@gmail.com")),
    User("simplesteph", "Stephane", Some("Maarek")),
    User("alice", "Alice"),
    User("will01St", "Will", Some("Byers"), Some("will@st.com")),
    User("kali02St", "Kali", Some("Eight"), Some("kali@st.com")),
    User("lucas03St", "Lucas", Some("Sinclair"), Some("lucas@st.com")),
    User("jon04St", "Jonathan", Some("Byers"), Some("jonathan@st.com")),
    User("dustin05St", "Dustin", Some("Henderson"), Some("dustin@st.com")),
    User("mike0St", "Mike", Some("Wheeler"), Some("mike@st.com"))
  )

  /* produce all users in a compacted topic (to build a ktable)  */
  clients
    .map(user => new ProducerRecord[UserKey, User]("avro-user-table", UserKey(user.login), user))
    .foreach(userProducer.send)

  /* produce all users in  */
  val keyValuePurchaseGen = for {
    uuid <- Gen.uuid.map(_.toString.take(8))

    game <- Gen.oneOf(Game.values.toSeq)

    knownClient <- Gen.oneOf(clients)
    unknownClient <- Gen.chooseNum(0, 999).map(i => User(s"Unknown$i", ""))

    client <- Gen.frequency((3, knownClient), (4, unknownClient))
    emailOrLogin = client.email.getOrElse("<email-not-available>")

    twoPlayerMode <- Gen.frequency((1, true), (1, false))

  } yield (PurchaseKey(uuid, UserKey(client.login)), Purchase(emailOrLogin, game, twoPlayerMode))

  userProducer.flush()

  var i = 0

  /* produce purchase in topic (to build a kstream) */
  while (true) {

    Thread.sleep(1000)

    if (i % 10 == 0) logger info s"Generating the ${i}th Purchase"

    keyValuePurchaseGen.sample.foreach { case (keyPurchase, valuePurchase) =>

      if (i % 10 == 0) logger info s"Sending the ${i}th Producer Record"

      val record = new ProducerRecord[PurchaseKey, Purchase]("avro-user-purchases", keyPurchase, valuePurchase)

      purchaseProducer.send(record)
    }
    i += 1
  }

}
