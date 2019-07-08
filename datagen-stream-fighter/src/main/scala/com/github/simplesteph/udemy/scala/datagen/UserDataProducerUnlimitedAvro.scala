package com.github.simplesteph.udemy.scala.datagen

import java.util.Properties

import com.github.simplesteph.udemy.scala.datagen.Dataset.{Purchase, User, UserKey}
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalacheck.Gen
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object UserDataProducerUnlimitedAvro extends App {

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

  val avroKeySerde = new GenericAvroSerde()
  val avroValueSerde = new GenericAvroSerde()

  avroKeySerde.configure(Map("schema.registry.url" -> "http://localhost:8081").asJava, true)
  avroValueSerde.configure(Map("schema.registry.url" -> "http://localhost:8081").asJava, false)

  val producer = new KafkaProducer[GenericRecord, GenericRecord](
    properties,
    avroKeySerde.serializer(),
    avroValueSerde.serializer()
  )

  val userFormatter = RecordFormat[User]
  val userKeyFormatter = RecordFormat[UserKey]

  val purchaseFormatter = RecordFormat[Purchase]
  val purchaseKeyFormatter = RecordFormat[Purchase]

  val clients = Vector[User](
    User("jdoe", "john", "Doe", "john.doe@gmail.com"),
    User("jojo", "Johnny", "Doe", "johnny.doe@gmail.com"),
    User("simplesteph", "Stephane", "Maarek"),
    User("alice", "Alice"),

    User("will01St","Will", "Byers", "will@st.com"),
    User("kali02St","Kali", "Eight", "kali@st.com"),
    User("lucas03St","Lucas", "Sinclair", "lucas@st.com"),
    User("jon04St","Jonathan", "Byers", "jonathan@st.com"),
    User("dustin05St","Dustin", "Henderson", "dustin@st.com"),
    User("mike0St","Mike", "Wheeler", "mike@st.com")
  )

  clients.map { client =>
    val avroKey = userKeyFormatter.to(UserKey(client.login))
    val avroValue = userKeyFormatter.to(UserKey(client.login))

    new ProducerRecord[GenericRecord, GenericRecord]("user-table", avroKey, avroValue)

  }.foreach(producer.send)

  val purchaseGen: Gen[Purchase] = for {

    knownClient <- Gen.oneOf(clients)
    unknownClient <- Gen.chooseNum(0, 999).map(i => User(s"Unknown$i", ""))

    client <-  Gen.frequency((3, knownClient), (4, unknownClient))

    game <- Gen.oneOf(Dataset.GameCollection)

    twoPlayerMode <- Gen.frequency((1, true), (1, false))

  } yield Purchase(client.login, game, twoPlayerMode)

  producer.flush()

  var i = 0

  while (true) {

    Thread.sleep(1000)

    if(i % 10 == 0) logger info s"Generating the ${i}th Purchase"

    purchaseGen.sample.foreach { purchase =>

      if(i % 10 == 0) logger info s"Sending the ${i}th Producer Record"

      val avroKey: GenericRecord = ??? //= userKeyFormatter.to(PurchaseKey())
      val avroValue: GenericRecord = ??? //= purchaseFormatter.to(purchase)

      val record = new ProducerRecord[GenericRecord, GenericRecord]("user-purchases", avroKey, avroValue)

      producer.send(record)
    }
    i += 1
  }

}
