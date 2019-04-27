package com.github.simplesteph.udemy.kafka.streams

import java.nio.ByteBuffer
import java.time.Instant
import java.util.Properties

import com.github.simplesteph.udemy.scala.datagen.Dataset.ExactlyOnceExercise.Hit
import io.circe.Json
import io.circe.generic.auto._
import io.circe.jawn.parseByteBuffer
import io.circe.syntax._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Grouped, Produced}
import org.apache.kafka.streams.scala.kstream.{Consumed, Materialized}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

object ArcadeContestExactlyOnceAppScala extends App {

  // create the initial json object for the player life points
  case class ChallengerPoints(`hit-count`: Int = 0,
                              `life-points`: Int = 100,
                              time: Instant = Instant.now())

  private def updateLifePoints(jsonHit: Json, jsonPoints: Json): Json = {

    val challenger = jsonPoints.as[ChallengerPoints].toOption.get
    val hit = jsonHit.as[Hit].toOption.get

    if (challenger.`life-points` == 0)
      updateLifePoints(hit.asJson, ChallengerPoints().asJson)

    else
      ChallengerPoints(
        `hit-count` = challenger.`hit-count` + 1,
        `life-points` = Math.max(challenger.`life-points` - hit.damage, 0),
        time = Instant.ofEpochMilli(Math.max(challenger.time.toEpochMilli, hit.time.toEpochMilli))
      ).asJson
  }

  private val logger = LoggerFactory.getLogger(getClass)

  val config: Properties = new Properties

  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "arcade-contest-application-scala")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  // for a immediate feed back when using the generator
  config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")

  // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
  config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")

  // Exactly once processing!!
  config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE)

  // json Serde
  val jsonSerde: Serde[Json] = Serdes.fromFn[Json](
    (_: String, json: Json) => json.noSpaces.getBytes("UTF8"),
    (_: String, bytes: Array[Byte]) => parseByteBuffer(ByteBuffer.wrap(bytes)).toOption
  )

  val builder = new StreamsBuilder

  val impactStream = builder.stream("arcade-contest")(Consumed.`with`(Serdes.String, jsonSerde))

  val impactGrouped = impactStream.groupByKey(Grouped.`with`(Serdes.String, jsonSerde))

  def initializer = ChallengerPoints().asJson

  def aggregator(key: String, jsonHit: Json, jsonPoints: Json) = updateLifePoints(jsonHit, jsonPoints)

  val challengersLifePoints = impactGrouped.aggregate(initializer)(aggregator)(
    Materialized.as("hit-impact-agg-scala")(Serdes.String, jsonSerde)
  )

  challengersLifePoints.toStream.to("arcade-contest-exactly-once")(Produced.`with`(Serdes.String, jsonSerde))

  val streams = new KafkaStreams(builder.build, config)
  streams.cleanUp()
  streams.start()

  // log the topology
  logger info streams.toString

  // shutdown hook to correctly close the streams application
  sys.addShutdownHook {
    streams.close()
  }

}
