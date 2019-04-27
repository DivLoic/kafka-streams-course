package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.ErrorEvents;
import com.github.simplesteph.Game;
import com.github.simplesteph.MachineId;
import com.github.simplesteph.Victory;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.WindowedSerdes.TimeWindowedSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;

public class FailureMonitoring {

    private static final Logger logger = LoggerFactory.getLogger(FailureMonitoring.class);

    private static final Initializer<ErrorEvents> initializer = () -> new ErrorEvents(0L, "");

    private static final Aggregator<MachineId, Victory, ErrorEvents> aggregator =
            (s, victory, victories) -> new ErrorEvents(victories.getCount() + 1, victories.getWindowStart());

    private static final DateTimeFormatter fmt = DateTimeFormatter
            .ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    public static void main(String[] args) {

        Properties properties = new Properties();

        SpecificAvroSerde<Victory> victorySerde = new SpecificAvroSerde<>();

        SpecificAvroSerde<MachineId> machineIdSerde = new SpecificAvroSerde<>();

        SpecificAvroSerde<ErrorEvents> errorEventSerde = new SpecificAvroSerde<>();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "warning-signal-monitoring-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        Map<String, Object> registryConf = singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        machineIdSerde.configure(registryConf, true);

        victorySerde.configure(registryConf, false);

        errorEventSerde.configure(registryConf, false);

        StreamsBuilder builder = new StreamsBuilder();

        Consumed<MachineId, Victory> consumed = Consumed.with(machineIdSerde, victorySerde)
                .withTimestampExtractor(new VictoryTimestampKeyExtractor());

        KStream<MachineId, Victory> victories = builder.stream("suppress-victories", consumed);

        Duration windowSize = Duration.ofSeconds(10L);

        Duration windowStep = Duration.ofSeconds(10L);

        Duration gracePeriod = Duration.ofSeconds(10L);

        TimeWindows windows = TimeWindows

                .of(windowSize)

                .advanceBy(windowStep)

                .grace(gracePeriod);

        Serde<Windowed<MachineId>> windowedMachineIdSerde =
                new TimeWindowedSerde<>(machineIdSerde, windowSize.toMillis());

        victories

                // victory without a correct game name is an error
                .filter((key, value) -> value.getGame() == Game.None)

                // we count ...
                .groupByKey(Grouped.with(machineIdSerde, victorySerde))

                // over a window ...
                .windowedBy(windows)

                // the number of errors
                .aggregate(initializer, aggregator, Materialized.with(machineIdSerde, errorEventSerde))

                // we suppress old versions of the windows
                .suppress(Suppressed.untilWindowCloses(unbounded()))

                .toStream()

                // we add the starting bound of the window in the value
                .mapValues(FailureMonitoring::moveWindowStartToValue)

                .to("suppress-errors-count", Produced.with(windowedMachineIdSerde, errorEventSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);

        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        logger.info(builder.build().describe().toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static ErrorEvents moveWindowStartToValue(
            Windowed<MachineId> key, ErrorEvents value) {
        value.setWindowStart(printTimeFromInstant(key.window().startTime()));
        return value;
    }

    private static String printTimeFromInstant(Instant instant) {
        return fmt.format(instant);
    }
}
