package com.github.simplesteph.udemy.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.simplesteph.udemy.kafka.streams.internal.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GameGenerator extends Generator {

    private static ObjectMapper mapper = new ObjectMapper();

    private static Logger logger = LoggerFactory.getLogger(GameGenerator.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String, String> producer =
                new KafkaProducer<>(properties, Serdes.String().serializer(), Serdes.String().serializer());

        /* add a user to this list to see him in the list of purchases */
        ArrayList<User> users = new ArrayList<>( Arrays.asList(
                new User("jdoe", "john", "Doe", "john.doe@gmail.com"),
                new User("jojo", "Johnny", "Doe", "johnny.doe@gmail.com"),
                new User("simplesteph", "Stephane", "Maarek", null),
                new User("alice", "Alice", null, null),
                new User("will00", "Will", "Byers", "will@st.com"),
                new User("kali01", "Kali", "Eight", "kali@st.com"),
                new User("lucas03", "Lucas", "Sinclair", "lucas@st.com"),
                new User("jon04", "Jonathan", "Byers", "jonathan@st.com"),
                new User("dustin05", "Dustin", "Henderson", "dustin@st.com"),
                new User("mike06", "Mike", "Wheeler", "mike@st.com")
        ));

        users
                .stream()
                .map(GameGenerator::userRecord)
                .forEach(producer::send);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            producer.flush();
            producer.close();
        }));

        producer.flush();

        int i = 0;
        while (true){
            Thread.sleep(1000L);

            if (i % 10 == 0) logger.info(String.format("Generating the %sth Purchase", i));

            // produce purchase
            Optional<ProducerRecord<String, String>> maybeRecord =

                    Optional.ofNullable(nextUser(users))
                            .map((user) -> {

                                Purchase value = nextPurchase(user);

                                return purchaseRecord(value);
                            }
                    );

            if (i % 10 == 0) logger.info(String.format("Sending the %sth Producer Record", i));

            maybeRecord.ifPresent(producer::send);

            i += 1;
        }
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
