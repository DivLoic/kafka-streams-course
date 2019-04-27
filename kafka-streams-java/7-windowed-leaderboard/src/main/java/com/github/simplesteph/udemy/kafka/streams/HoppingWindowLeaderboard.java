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
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;

public class HoppingWindowLeaderboard {

    private static final Logger logger = LoggerFactory.getLogger(TumblingWindowLeaderboard.class);

    private static final DateTimeFormatter fmt = DateTimeFormatter
            .ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private static final Initializer<Victories> initializer =
            () -> Victories.newBuilder().setCount(0L).build();

    private static final Aggregator<CharacterKey, Victory, Victories> aggregator =
            (s, victory, victories) -> Victories
                    .newBuilder()
                    .setCount(victories.getCount() + 1)
                    .setLatest(printTimeFromInstant(victory.getTimestamp()))
                    .build();

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "hopping-window-app");
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

        Duration windowSize = Duration.ofSeconds(10L);

        Duration windowStep = Duration.ofSeconds(1L);

        TimeWindows windows = TimeWindows

                .of(windowSize)

                .advanceBy(windowStep);

        Serde<Windowed<CharacterKey>> windowedSerde =
                new WindowedSerdes.TimeWindowedSerde<>(characterKeySerde, windowSize.toMillis());

        KStream<MachineId, Victory> victories = builder.stream("windowed-victories", consumed);

        victories

                .filter((key, value) -> value.getGame() == Game.StreetFighter)

                .selectKey((key, value) -> new CharacterKey(value.getCharacter().getName()))

                .groupByKey(Grouped.with(characterKeySerde, victorySerde))

                .windowedBy(windows)

                .aggregate(initializer, aggregator, Materialized.with(characterKeySerde, victoriesSerde))

                .toStream()

                // we add the starting bound of the window in the value
                .mapValues(HoppingWindowLeaderboard::moveWindowStartToValue)

                .to("hopping-window-victories", Produced.with(windowedSerde, victoriesSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        logger.info(builder.build().describe().toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static Victories moveWindowStartToValue(
            Windowed<CharacterKey> key, Victories value){
        value.setWindowStart(printTimeFromInstant(key.window().startTime()));
        return value;
    }

    private static String printTimeFromInstant(Instant instant) {
        return fmt.format(instant);
    }
}
