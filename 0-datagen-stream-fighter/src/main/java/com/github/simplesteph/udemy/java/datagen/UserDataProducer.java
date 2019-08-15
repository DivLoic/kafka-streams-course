package com.github.simplesteph.udemy.java.datagen;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.github.simplesteph.udemy.java.datagen.Game.*;

// Run the Kafka Streams application before running the producer.
// This will be best for your learning
public class UserDataProducer {

    private static ObjectMapper mapper = new ObjectMapper();

    private static Logger logger = LoggerFactory.getLogger(UserDataProducer.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
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

        // FYI - We do .get() to ensure the writes to the topics are sequential, for the sake of the teaching exercise
        // DO NOT DO THIS IN PRODUCTION OR IN ANY PRODUCER. BLOCKING A FUTURE IS BAD!

        // we are going to test different scenarios to illustrate the join

        // 1 - we create a new user, then we send some data to Kafka
        logger.info("Example 1 - new user");
        producer.send(userRecord(new User("jdoe", "john", "Doe", "john.doe@gmail.com"))).get();
        producer.send(purchaseRecord(new Purchase("jdoe", StreetFighter, false))).get();

        Thread.sleep(10000);

        // 2 - we receive user purchase, but it doesn't exist in Kafka
        logger.info("Example 2 - non existing user");
        producer.send(purchaseRecord(new Purchase("bob", Takken, false))).get();

        Thread.sleep(10000);

        // 3 - we update user "john", and send a new transaction
        logger.info("Example 3 - update to user");
        producer.send(userRecord(new User("jojo", "Johnny", "Doe", "johnny.doe@gmail.com"))).get();
        producer.send(purchaseRecord(new Purchase("jojo", KingOfFighters, true))).get();

        Thread.sleep(10000);

        // 4 - we send a user purchase for stephane, but it exists in Kafka later
        logger.info("Example 4 - non existing user then user");
        producer.send(purchaseRecord(new Purchase("simplesteph", StreetFighter, false))).get();
        producer.send(userRecord(new User("simplesteph", "Stephane", "Maarek"))).get();
        producer.send(purchaseRecord(new Purchase("simplesteph", StreetFighter, false))).get();
        producer.send(userDeletionRecord("simplesteph")).get(); // delete for cleanup

        Thread.sleep(10000);

        // 5 - we create a user, but it gets deleted before any purchase comes through
        logger.info("Example 5 - user then delete then data");
        producer.send(userRecord(new User("alice", "Alice"))).get();
        producer.send(userDeletionRecord("alice")).get(); // that's the delete record
        producer.send(purchaseRecord(new Purchase("alice", SoulCalibur, true))).get();

        Thread.sleep(10000);

        logger.info("End of demo");
        producer.close();
    }

    private static ProducerRecord<String, String> userRecord(User user){
        return new ProducerRecord<>("user-table", user.getLogin(), writeJson(user));
    }

    private static ProducerRecord<String, String> userDeletionRecord(String login){
        return new ProducerRecord<>("user-table", login, null);
    }

    private static ProducerRecord<String, String> purchaseRecord(Purchase purchase){
        return new ProducerRecord<>("user-purchases", purchase.getUser(), writeJson(purchase));
    }

    private static String writeJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return "{}";
        }
    }
}
