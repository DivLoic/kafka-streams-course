package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.udemy.kafka.streams.internal.Generator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class GameGenerator extends Generator {

    public static void main(String[] args) throws InterruptedException {
        Logger logger = LoggerFactory.getLogger(GameGenerator.class);

        Properties properties = new Properties();

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.ACKS_CONFIG, "0");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.warn(String.format("Shunting down the generator: %s", GameGenerator.class));
            producer.flush();
            producer.close();
        }
        ));

        logger.info(String.format("Starting the generator: %s", GameGenerator.class));

        while (true) {
            Thread.sleep(Duration.ofSeconds(1).toMillis());
            producer.send(new ProducerRecord<>("word-count-input", nextGame()));
        }
    }
}
