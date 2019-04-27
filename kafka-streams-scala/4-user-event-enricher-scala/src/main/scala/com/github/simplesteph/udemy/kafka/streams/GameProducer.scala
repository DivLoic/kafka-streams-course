package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import com.github.simplesteph.udemy.kafka.streams.internal.Generator
import io.circe.generic.auto._
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}

// Run the Kafka Streams application before running the producer.
// This will be best for your learning
object GameProducer extends App with Generator {

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

  // FYI - We do .get() to ensure the writes to the topics are sequential, for the sake of the teaching exercise
  // DO NOT DO THIS IN PRODUCTION OR IN ANY PRODUCER. BLOCKING A FUTURE IS BAD!

  // we are going to test different scenarios to illustrate the join

  // 1 - we create a new user, then we send some data to Kafka

  logger info "Example 1 - new user"
  producer.send(userRecord(User("jdoe", "john", "Doe", "john.doe@gmail.com"))).get
  producer.send(purchaseRecord(Purchase("jdoe", StreetFighter, istwoPlayer = false))).get

  Thread.sleep(10000)

  // 2 - we receive user purchase, but it doesn't exist in Kafka
  logger info "Example 2 - non existing user"
  producer.send(purchaseRecord(Purchase("bob", Takken, istwoPlayer = false))).get

  Thread.sleep(10000)

  // 3 - we update user "john", and send a new transaction
  logger info "Example 3 - update to user"
  producer.send(userRecord(User("jojo", "Johnny", "Doe", "johnny.doe@gmail.com"))).get
  producer.send(purchaseRecord(Purchase("jojo", KingOfFighters, istwoPlayer = false))).get

  Thread.sleep(10000)

  // 4 - we send a user purchase for stephane, but it exists in Kafka later
  logger info "Example 4 - non existing user then user"
  producer.send(purchaseRecord(Purchase("simplesteph", StreetFighter, istwoPlayer = false))).get
  producer.send(userRecord(User("simplesteph", "Stephane", "Maarek"))).get
  producer.send(purchaseRecord(Purchase("simplesteph", StreetFighter, istwoPlayer = false))).get
  producer.send(userDeletionRecord("simplesteph")).get // delete for cleanup

  Thread.sleep(10000)

  // 5 - we create a user, but it gets deleted before any purchase comes through
  logger info "Example 5 - user then delete then data"
  producer.send(userRecord(User("alice", "Alice"))).get
  producer.send(userDeletionRecord("alice")).get // that's the delete record

  producer.send(purchaseRecord(Purchase("alice", SoulCalibur, istwoPlayer = true))).get

  Thread.sleep(10000)
  logger info "End of demo"
  producer.close()

  private def userRecord(user: User) =
    new ProducerRecord[String, String]("user-table", user.login, user.asJson.noSpaces)

  private def userDeletionRecord(login: String) =
    new ProducerRecord[String, String]("user-table", login, null)

  private def purchaseRecord(purchase: Purchase) =
    new ProducerRecord[String, String]("user-purchases", purchase.user, purchase.asJson.noSpaces)
}
