package com.github.simplesteph.udemy.kafka.streams;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.simplesteph.udemy.kafka.streams.internal.Generator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class GameGenerator extends Generator {

    private static Logger logger = LoggerFactory.getLogger(GameGenerator.class);

    private static final Character KEN = new Character("Ken", "US"){};
    private static final Character RYU = new Character("Ryu", "Japan"){};
    public static final Character GEKI = new Character("Geki", "Japan"){};
    private static final Character CHUNLI = new Character("ChunLi", "China"){};

    private static String randomKey() {
        int i = new Random().nextInt(2);
        return Arrays.asList("X", "O").get(i);
    }

    protected static ProducerRecord<String, String> newRandomImpact(Challenger challenger) {
        // creates an empty json {}
        ObjectNode hit = JsonNodeFactory.instance.objectNode();

        // { "damage" : 16 } (16 is a random number between 5 and 25 excluded)
        Integer damage = ThreadLocalRandom.current().nextInt(0, 20) + 5;

        // Instant.now() is to get the current time using Java 8
        Instant now = Instant.now();

        // we write the data to the json document
        hit.put("key", randomKey());
        hit.set("challenger", challenger.toJson());

        hit.put("damage", damage);
        hit.put("time", now.toString());

        // format the hit as json {"key": "X", challenger: {"name": "Leo", ...}, ...}
        return new ProducerRecord<>("arcade-contest", challenger.getLogin(), hit.toString());
    }

    public static void main(String[] args) {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        Producer<String, String> producer = new KafkaProducer<>(properties);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.flush();
            producer.close();
        }));

        List<Challenger> challengers = Arrays.asList(
                new Challenger("john", RYU),
                new Challenger("steph", CHUNLI),
                new Challenger("alice", KEN)
        );

        int i = 0;
        while (true){
            if(i % 10 == 0) logger.info(String.format("Producing batch: %d", i));

            challengers.forEach(challenger -> {
                producer.send(newRandomImpact(challenger));
                try {
                    Thread.sleep(new Random().nextInt(2000) + 500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            });
            i += 1;
        }
    }
}
