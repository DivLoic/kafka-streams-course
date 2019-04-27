package com.github.simplesteph.udemy.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class FavouritePlayerApp {

    private static Logger logger = LoggerFactory.getLogger(FavouritePlayerApp.class);

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-player");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // for a immediate feed back when using the generator
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: We create the topic of terminals keys to players
        KStream<String, String> textLines = builder.stream("favourite-player-input");

        KStream<String, String> terminalsAndPlayers = textLines
                // 1 - we ensure that a comma is here as we will split on it
                .filter((key, value) -> value.contains(","))
                // 2 - we select a key that will be the terminal id (lowercase for safety)
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // 3 - we get the player from the value (lowercase for safety)
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // 4 - we filter undesired players (could be a data sanitization step
                .filter((terminal, player) -> Arrays.asList("ryu", "ken", "chunli").contains(player));

        terminalsAndPlayers.to("terminal-keys-and-players");

        // step 2 - we read that topic as a KTable so that updates are read correctly
        KTable<String, String> terminalsAndPlayersTable = builder.table("terminal-keys-and-players");

        // step 3 - we count the occurrences of players
        KTable<String, Long> favouritePlayers = terminalsAndPlayersTable
                // 5 - we group by player within the KTable
                .groupBy((terminal, player) -> new KeyValue<>(player, player))
                .count(Materialized.as("CountsByPlayers"));

        // 6 - we output the results to a Kafka Topic - don't forget the serializers
        favouritePlayers.toStream().to("favourite-player-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        // only do this in dev - not in prod
        streams.cleanUp();
        streams.start();

        // print the topology
        logger.info(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
