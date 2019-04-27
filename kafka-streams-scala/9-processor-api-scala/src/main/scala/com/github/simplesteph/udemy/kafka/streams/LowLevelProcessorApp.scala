package com.github.simplesteph.udemy.kafka.streams

import java.util.Properties

import com.github.simplesteph.{MachineId, Reward, RewardedVictory, Victory}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG
import io.confluent.kafka.streams.serdes.avro.{GenericAvroSerde, SpecificAvroSerde}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.kstream.{TransformerSupplier, ValueTransformerSupplier}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{Consumed, KStream, Materialized, Produced}
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

object LowLevelProcessorApp extends App {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  object VictoryError extends Victory()

  object RewardedVictoryError extends RewardedVictory()

  val properties: Properties = new Properties()

  val machineIdSerde: SpecificAvroSerde[MachineId] = new SpecificAvroSerde()

  val victorySerde: SpecificAvroSerde[Victory] = new SpecificAvroSerde()

  val rewardSerde: SpecificAvroSerde[Reward] = new SpecificAvroSerde()

  val rewardedVictorySerde: SpecificAvroSerde[RewardedVictory] = new SpecificAvroSerde()

  properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "warning-signal-monitoring-scala-app")
  properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092")
  properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[GenericAvroSerde])
  properties.put(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081")

  val registryConf = Map(SCHEMA_REGISTRY_URL_CONFIG -> "http://127.0.0.1:8081")

  machineIdSerde.configure(registryConf.asJava, true)

  victorySerde.configure(registryConf.asJava, false)

  rewardSerde.configure(registryConf.asJava, false)

  rewardedVictorySerde.configure(registryConf.asJava, false)

  val builder: StreamsBuilder = new StreamsBuilder()

  val consumed: Consumed[MachineId, Victory] = Consumed.`with`(machineIdSerde, victorySerde)

  // create a KTable to hold all the coupons in a state store
  builder.table[MachineId, Reward](
    "processor-rewards-scala",
    Materialized.as(Stores.persistentKeyValueStore("reward-store-name-scala"))(machineIdSerde, rewardSerde)
  )(Consumed.`with`(machineIdSerde, rewardSerde))

  val victories: KStream[MachineId, Victory] = builder.stream("processor-victories")(consumed)

  val statusFilterSupplier: ValueTransformerSupplier[Victory, Victory] =

    () => new StatusFilterTransformer

  val rewardTransSupplier: TransformerSupplier[MachineId, Victory, KeyValue[MachineId, RewardedVictory]] =

    () => new RewardTransformer()

  victories

    .transformValues(statusFilterSupplier)

    .filter((_, value) => VictoryError != value)

    // access to the state store linked to the KTable
    .transform(rewardTransSupplier, "reward-store-name-scala")

    .filter((_, value) => RewardedVictoryError != value)

    .to("processor-rewarded-victories-scala")(Produced.`with`(machineIdSerde, rewardedVictorySerde))

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
