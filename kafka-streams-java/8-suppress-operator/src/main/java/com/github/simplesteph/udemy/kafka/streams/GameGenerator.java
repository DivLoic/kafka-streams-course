package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.MachineId;
import com.github.simplesteph.Victory;
import com.github.simplesteph.udemy.kafka.streams.internal.Generator;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Collections.singletonMap;

public class GameGenerator extends Generator {

    private static Logger logger = LoggerFactory.getLogger(GameGenerator.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        SpecificAvroSerde<MachineId> machineIdSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<Victory> victorySerde = new SpecificAvroSerde<>();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Map<String, Object> registryConf = singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        machineIdSerde.configure(registryConf, true);

        victorySerde.configure(registryConf, false);

        Producer<MachineId, Victory> victoryProducer = new KafkaProducer<>(
                properties,
                machineIdSerde.serializer(),
                victorySerde.serializer()
        );

        int i = 0;
        while (true) {
            Thread.sleep(1000L);

            final int y = i;
            nextVictory().ifPresent(victory -> {

                ProducerRecord<MachineId, Victory> record =
                        new ProducerRecord<>("suppress-victories", new MachineId(victory.getTerminal()), victory);

                victoryProducer.send(record);

                if (y % 10 == 0) logger.info(String.format("Sending the %sth victory from the game terminals", y));
            });

            i += 1;
        }
    }
}
