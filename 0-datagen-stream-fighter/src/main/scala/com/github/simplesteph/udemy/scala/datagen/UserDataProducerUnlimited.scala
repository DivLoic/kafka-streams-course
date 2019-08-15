package com.github.simplesteph.udemy.scala.datagen

import java.util.Properties

import com.github.simplesteph.udemy.scala.datagen.Dataset.{Purchase, User}
import io.circe.{Encoder, Json}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalacheck.Gen
import io.circe.generic.auto._
import io.circe.syntax._
import org.slf4j.{Logger, LoggerFactory}


object UserDataProducerUnlimited extends App {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val gameEncoder: Encoder[Game] = (a: Game) => Json.fromString(a.toString)

  val properties = new Properties
  // kafka bootstrap server
  properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  // producer acks
  properties.setProperty(ProducerConfig.ACKS_CONFIG, "all") // strongest producing guarantee

  properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3")
  properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1")
  // leverage idempotent producer from Kafka 0.11 !
  properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true") // ensure we don't push duplicates

  val producer = new KafkaProducer[String, String](properties)

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

  clients
    .map(c => new ProducerRecord[String, String]("user-table", c.login, c.asJson.noSpaces))
    .foreach(producer.send)

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

      val record = new ProducerRecord[String, String]("user-purchases", purchase.user, purchase.asJson.noSpaces)

      producer.send(record)
    }
    i += 1
  }
}
