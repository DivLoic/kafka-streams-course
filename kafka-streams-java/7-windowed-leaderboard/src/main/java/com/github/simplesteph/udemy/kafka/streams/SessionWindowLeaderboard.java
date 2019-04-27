package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.*;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;

public class SessionWindowLeaderboard {

    private static final Logger logger = LoggerFactory.getLogger(TumblingWindowLeaderboard.class);

    private static final DateTimeFormatter fmt = DateTimeFormatter
            .ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private static final Initializer<Victories> initializer =
            () -> Victories.newBuilder().setCount(0L).build();

    private static final Aggregator<MachineId, Victory, Victories> aggregator =
            (s, victory, victories) -> Victories
                    .newBuilder()
                    .setCount(victories.getCount() + 1)
                    .setLatest(printTimeFromInstant(victory.getTimestamp()))
                    .build();

    private static final Merger<MachineId, Victories> merger = (aggKey, aggOne, aggTwo) -> {
        LocalTime instantOne = timestampFromOptionalTime(aggOne).orElse(LocalTime.MIN);
        LocalTime instantTow = timestampFromOptionalTime(aggTwo).orElse(LocalTime.MIN);
        String latest = instantOne.isAfter(instantTow) ? aggOne.getLatest() : aggTwo.getLatest();

        return Victories.newBuilder()
                .setCount(aggOne.getCount() + aggTwo.getCount())
                .setLatest(latest)
                .build();
    };

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "session-window-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        Map<String, Object> registryConf = singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        SpecificAvroSerde<Victory> victorySerde = new SpecificAvroSerde<>();

        SpecificAvroSerde<Victories> victoriesSerde = new SpecificAvroSerde<>();

        SpecificAvroSerde<MachineId> machineIdSerde = new SpecificAvroSerde<>();

        SpecificAvroSerde<CharacterKey> characterKeySerde = new SpecificAvroSerde<>();

        victorySerde.configure(registryConf, false);

        victoriesSerde.configure(registryConf, false);

        machineIdSerde.configure(registryConf, true);

        characterKeySerde.configure(registryConf, true);

        Consumed<MachineId, Victory> consumed = Consumed
                .with(machineIdSerde, victorySerde)
                .withTimestampExtractor(new VictoryTimestampKeyExtractor());

        StreamsBuilder builder = new StreamsBuilder();

        Duration inactivityGap = Duration.ofSeconds(30L);

        SessionWindows windows = SessionWindows.with(inactivityGap);

        Serde<Windowed<MachineId>> windowedSerde = new WindowedSerdes.TimeWindowedSerde<>(machineIdSerde);

        KStream<MachineId, Victory> victories = builder.stream("windowed-victories", consumed);

        victories

                .filter((key, value) -> value.getGame() == Game.StreetFighter)

                .groupByKey(Grouped.with(machineIdSerde, victorySerde))

                .windowedBy(windows)

                .aggregate(initializer, aggregator, merger, Materialized.with(machineIdSerde, victoriesSerde))

                .toStream()

                // we add the starting bound of the window in the value
                .mapValues(SessionWindowLeaderboard::moveWindowStartToValue)

                .to("session-window-victories", Produced.with(windowedSerde, victoriesSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        logger.info(builder.build().describe().toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Victories moveWindowStartToValue(
            Windowed<MachineId> key, Victories value) {

        Victories newValue = Optional
                .ofNullable(value)
                .map((victories) ->
                        Victories
                                .newBuilder(victories)
                                .setWindowStart(printTimeFromInstant(key.window().startTime()))
                                .build())
                .orElse(null);

        return newValue;
    }

    private static Optional<LocalTime> timestampFromOptionalTime(Victories aggOne) {
        return Optional.ofNullable(aggOne.getLatest()).map((time) -> LocalTime.parse(time, fmt));
    }

    private static String printTimeFromInstant(Instant instant) {
        return fmt.format(instant);
    }
}
