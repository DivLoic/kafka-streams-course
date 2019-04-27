package com.github.simplesteph.udemy.kafka.streams;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

public class ArcadeContestExactlyOnceApp {

    private static Logger logger = LoggerFactory.getLogger(ArcadeContestExactlyOnceApp.class);

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "arcade-contest-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // for a immediate feed back when using the generator
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

        // we disable the cache to demonstrate all the "steps" involved in the transformation - not recommended in prod
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // json Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, JsonNode> impactStream =
                builder.stream("arcade-contest", Consumed.with(Serdes.String(), jsonSerde));

        KTable<String, JsonNode> challengersLifePoints = impactStream
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(
                        ArcadeContestExactlyOnceApp::intiLifePoints,
                        (key, hit, lifePoints) -> updateLifePoints(hit, lifePoints),
                        Materialized.<String, JsonNode, KeyValueStore<Bytes, byte[]>>as("hit-impact-agg").withValueSerde(jsonSerde)
                );

        challengersLifePoints.toStream().to("arcade-contest-exactly-once", Produced.with(Serdes.String(), jsonSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        // log the topology
        logger.info(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // create the initial json object for the player life points
    private static JsonNode intiLifePoints() {
        String datetime = Instant.now().toString();
        ObjectNode lifePoint = JsonNodeFactory.instance.objectNode();
        lifePoint.put("hit-count", 0);
        lifePoint.put("life-points", 100);
        lifePoint.put("game-start", datetime);
        lifePoint.put("last-impact", datetime);
        return lifePoint;
    }

    private static JsonNode restartPoints(String startDateTime) {
        return ((ObjectNode) intiLifePoints()).put("game-start", startDateTime);
    }

    private static JsonNode updateLifePoints(JsonNode hit, JsonNode lifePoint) {
        // create a new game json object
        ObjectNode newLifePoint = JsonNodeFactory.instance.objectNode();

        if (lifePoint.get("life-points").asInt() == 0) {
            return updateLifePoints(hit, restartPoints(hit.get("time").asText()));
        } else {
            newLifePoint.put("game-start", lifePoint.get("game-start").asText());
            newLifePoint.put("last-impact", hit.get("time").asText());

            newLifePoint.put("hit-count", lifePoint.get("hit-count").asInt() + 1);
            newLifePoint.put(
                    "life-points",
                    Math.max(lifePoint.get("life-points").asInt() - hit.get("damage").asInt(), 0)
            );
            return newLifePoint;
        }
    }
}
