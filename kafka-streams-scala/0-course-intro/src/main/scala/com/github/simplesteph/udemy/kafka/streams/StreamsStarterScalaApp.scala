package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Produced}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.slf4j.{Logger, LoggerFactory}

object StreamsStarterScalaApp extends App {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  val properties: Properties = new Properties()

  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter-app")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

  val builder: StreamsBuilder = new StreamsBuilder

  val kStream: KStream[String, String] = builder
    .stream("input-topic-name-scala")(Consumed.`with`(Serdes.String, Serdes.String))

  // do stuff
  kStream.to("word-count-output-scala")(Produced.`with`(Serdes.String, Serdes.String))

  val streams: KafkaStreams = new KafkaStreams(builder.build(), properties)
  streams.cleanUp() // only do this in dev - not in prod
  streams.start()

  // print the topology
  logger.info(builder.build().describe().toString)

  // shutdown hook to correctly close the streams application
  sys.addShutdownHook {
    streams.close()
  }

}
