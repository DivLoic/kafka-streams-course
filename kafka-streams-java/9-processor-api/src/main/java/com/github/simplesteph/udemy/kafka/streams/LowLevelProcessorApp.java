package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.MachineId;
import com.github.simplesteph.Reward;
import com.github.simplesteph.RewardedVictory;
import com.github.simplesteph.Victory;
import com.github.simplesteph.udemy.kafka.streams.internal.Generator.RewardedVictoryError;
import com.github.simplesteph.udemy.kafka.streams.internal.Generator.VictoryError;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;


public class LowLevelProcessorApp {

    private static final Logger logger = LoggerFactory.getLogger(LowLevelProcessorApp.class);

    static final VictoryError ERROR = new VictoryError();
    static final RewardedVictoryError NO_REWARD_LEFT = new RewardedVictoryError();

    public static void main(String[] args) {

        Properties properties = new Properties();

        SpecificAvroSerde<MachineId> machineIdSerde = new SpecificAvroSerde<>();

        SpecificAvroSerde<Victory> victorySerde = new SpecificAvroSerde<>();

        SpecificAvroSerde<Reward> rewardSerde = new SpecificAvroSerde<>();

        SpecificAvroSerde<RewardedVictory> rewardedVictorySerde = new SpecificAvroSerde<>();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "warning-signal-monitoring-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        Map<String, Object> registryConf = singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        machineIdSerde.configure(registryConf, true);

        victorySerde.configure(registryConf, false);

        rewardSerde.configure(registryConf, false);

        rewardedVictorySerde.configure(registryConf, false);

        StreamsBuilder builder = new StreamsBuilder();

        Consumed<MachineId, Victory> consumed = Consumed.with(machineIdSerde, victorySerde);

        // create a KTable to hold all the coupons in a state store
        builder.table(
                "rewards",
                Consumed.with(machineIdSerde, rewardSerde),
                Materialized.as(Stores.persistentKeyValueStore("reward-store-name"))
        );

        KStream<MachineId, Victory> victories = builder.stream("victories", consumed);

        victories

                .transformValues(StatusFilterTransformer::new)

                .filter((key, value) -> ERROR != value)

                // access to the state store linked to the KTable
                .transform(RewardTransformer::new, "reward-store-name")

                .filter((key, value) -> NO_REWARD_LEFT != value)

                .to("rewarded-victories", Produced.with(machineIdSerde, rewardedVictorySerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        logger.info(builder.build().describe().toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
