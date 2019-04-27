package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.MachineId;
import com.github.simplesteph.Reward;
import com.github.simplesteph.Victory;
import com.github.simplesteph.udemy.kafka.streams.internal.Generator;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
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
        SpecificAvroSerde<Reward> rewardSerde = new SpecificAvroSerde<>();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Map<String, Object> registryConf = singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        machineIdSerde.configure(registryConf, true);
        victorySerde.configure(registryConf, false);
        rewardSerde.configure(registryConf, false);

        Producer<MachineId, Victory> victoryProducer = new KafkaProducer<>(
                properties,
                machineIdSerde.serializer(),
                victorySerde.serializer()
        );

        Producer<MachineId, Reward> rewardProducer = new KafkaProducer<>(
                properties,
                machineIdSerde.serializer(),
                rewardSerde.serializer()
        );

        int termNumber = 10;

        List<Reward> rewards = nextRewards(termNumber);

        rewards.stream()
                .map((reward) -> new ProducerRecord<>("processor-rewards", new MachineId(reward.getIdTerm()), reward))
                .forEachOrdered(rewardProducer::send);

        rewardProducer.flush();

        int i = 0;
        while (true){
            Thread.sleep(1000L);

            int id = nextTerminalId(termNumber);
            final int y = i;
            nextVictory(id).ifPresent(victory -> {

                ProducerRecord<MachineId, Victory> record =
                        new ProducerRecord<>("processor-victories", new MachineId(Integer.toString(id)), victory);

                if (y % 3 == 0) {
                    victoryProducer.send(addRandomValidationStatus(record));
                } else {
                    victoryProducer.send(record);
                }

                if (y % 10 == 0) {
                    logger.info(String.format("Sending the %sth victory from the game terminals", y));
                }
            });
            
            i += 1;
        }
    }

    private static ProducerRecord<MachineId, Victory> addRandomValidationStatus(
            ProducerRecord<MachineId, Victory> record
    ) {
        if(!nextValidStatus()) {
            record.value().setTerminal("XXX");
            record.headers().add(new RecordHeader("status", "invalid".getBytes(StandardCharsets.UTF_8)));
        } else {
            record.headers().add(new RecordHeader("status", "valid".getBytes(StandardCharsets.UTF_8)));
        }

        return record;
    }


}
