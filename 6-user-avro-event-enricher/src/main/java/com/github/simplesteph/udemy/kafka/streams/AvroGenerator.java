package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.Purchase;
import com.github.simplesteph.PurchaseKey;
import com.github.simplesteph.User;
import com.github.simplesteph.UserKey;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.Stream;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

public class AvroGenerator {

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

        Stream<User> users = Stream.of(
                    new User("jdoe", "john", "Doe", "john.doe@gmail.com"),
                    new User("jojo", "Johnny", "Doe", "johnny.doe@gmail.com"),
                    new User("simplesteph", "Stephane", "Maarek", null),
                    new User("alice", "Alice", null, null),
                    new User("will01St", "Will", "Byers", "will@st.com"),
                    new User("kali02St", "Kali", "Eight", "kali@st.com"),
                    new User("lucas03St", "Lucas", "Sinclair", "lucas@st.com"),
                    new User("jon04St", "Jonathan", "Byers", "jonathan@st.com"),
                    new User("dustin05St", "Dustin", "Henderson", "dustin@st.com"),
                    new User("mike0St", "Mike", "Wheeler", "mike@st.com")
        );

        users
                .map((user) -> new ProducerRecord<UserKey, User>("user-table-avro", new UserKey(user.getLogin()), user))
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

            i += 1;
        }
    }
}
