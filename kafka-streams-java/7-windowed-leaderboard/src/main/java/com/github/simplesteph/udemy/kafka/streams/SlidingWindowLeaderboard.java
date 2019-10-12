package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.Victories;
import com.github.simplesteph.Victory;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public class SlidingWindowLeaderboard {

    private static final Logger logger = LoggerFactory.getLogger(SlidingWindowLeaderboard.class);

    private static final Initializer<Victories> initializer = () -> new Victories(0L, Instant.ofEpochMilli(-1L));

    private static final Aggregator<String, Victory, Victories> aggregator =
            (s, victory, victories) -> new Victories(victories.getCount() + 1, victory.getDatetime());

    public static void main(String[] args) {

        /*Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "sliding-window-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        StreamsBuilder builder = new StreamsBuilder();

        SpecificAvroSerde<Victory> victorySerde = new SpecificAvroSerde<>();

        SpecificAvroSerde<Victories> victoriesSerde = new SpecificAvroSerde<>();

        Consumed<String, Victory> consumed = Consumed.with(Serdes.String(), victorySerde);

        KStream<String, Victory> victories = builder.stream("victories", consumed);

        Duration windowSize = Duration.ofSeconds(10L);

        Duration windowStep = Duration.ofSeconds(10L);

        TimeWindows windows = TimeWindows

                .of(windowSize)

                .advanceBy(windowStep);

        Serde<Windowed<String>> windowedStringSerde =
                WindowedSerdes.timeWindowedSerdeFrom(String.class, windowSize.toMillis());

        victories

                .selectKey((key, value) -> value.getCharacter().getName())

                .groupByKey(Grouped.with(Serdes.String(), victorySerde))

                .windowedBy(windows)

                .aggregate(initializer, aggregator, Materialized.with(Serdes.String(), victoriesSerde))

        .toStream().to("tumbling-window-victories", Produced.with(windowedStringSerde, victoriesSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        logger.info(builder.build().describe().toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));*/
    }
}
