package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.slf4j.LoggerFactory

object WordCountScalaApp extends App {

  val logger = LoggerFactory.getLogger(getClass)

  val config = new Properties
  config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-application")
  config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass)
  config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass)

  val builder: StreamsBuilder = new StreamsBuilder()
  // 1 - stream from Kafka

  val textLines: KStream[String, String] = builder
    .stream[String, String]("word-count-input")(Consumed.`with`(Serdes.String, Serdes.String))

  val wordCounts = textLines
    // 2 - map values to lowercase
    .mapValues(_.toLowerCase)
    // 3 - flatmap values split by space
    .flatMapValues(_.split("\\W+"))
    // 4 - select key to apply a key (we discard the old key)
    .selectKey((_, word) => word)
    // 5 - group by key before aggregation
    .groupByKey(Grouped.`with`(Serdes.String, Serdes.String))
    // 6 - count occurrences
    .count()(Materialized.as("Counts")(Serdes.String, Serdes.Long))

  // 7 - to in order to write the results back to kafka
  wordCounts.toStream.to("word-count-output-scala")(Produced.`with`(Serdes.String, Serdes.Long))

  val streams = new KafkaStreams(builder.build(), config)

  streams.start()

  logger.info(builder.build().describe().toString)

  // shutdown hook to correctly close the streams application
  sys.addShutdownHook {
    streams.close()
  }
}
