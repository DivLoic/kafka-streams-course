package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.*;
import com.github.simplesteph.udemy.kafka.streams.internal.Generator;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AvroGenerator extends Generator {

    private static Logger logger = LoggerFactory.getLogger(AvroGenerator.class);

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();

        // kafka bootstrap server
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        // producer acks
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // strongest producing guarantee

        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        // leverage idempotent producer from Kafka 0.11 !
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // ensure we don't push duplicates

        SpecificAvroSerde<User> userSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<UserKey> userKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<Purchase> purchaseSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<PurchaseKey> purchaseKeySerde = new SpecificAvroSerde<>();

        userSerde
                .configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081"), false);
        userKeySerde
                .configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081"), true);
        purchaseSerde
                .configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081"), false);
        purchaseKeySerde
                .configure(Collections.singletonMap(SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081"), true);

        Producer<UserKey, User> userProducer = new KafkaProducer<>(
                properties,
                userKeySerde.serializer(),
                userSerde.serializer()
        );

        Producer<PurchaseKey, Purchase> purchaseProducer = new KafkaProducer<>(
                properties,
                purchaseKeySerde.serializer(),
                purchaseSerde.serializer()
        );

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

        users.stream()
                .map((user) -> new ProducerRecord<>("avro-user-table", new UserKey(user.getLogin()), user))
                .forEach(userProducer::send);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            purchaseProducer.flush();
            purchaseProducer.close();
        }));

        userProducer.flush();

        int i = 0;
        while (true){
            Thread.sleep(1000L);

            if (i % 10 == 0) logger.info(String.format("Generating the %sth Purchase", i));

            // produce puchase
            Optional<ProducerRecord<PurchaseKey, Purchase>> maybeRecord =

                    Optional.ofNullable(Generator.nextUser(users)).map(
                            (user) -> {

                                PurchaseKey key = new PurchaseKey(nextPurchaseId(), new UserKey(user.getLogin()));

                                Purchase value = new Purchase(user.getLogin(), nextGame(), nextIsTwoPlayer());

                                return new ProducerRecord<>("avro-user-purchases", key, value);
                            }
                    );

            if (i % 10 == 0) logger.info(String.format("Sending the %sth Producer Record", i));

            maybeRecord.ifPresent(purchaseProducer::send);

            i += 1;
        }
    }
}
