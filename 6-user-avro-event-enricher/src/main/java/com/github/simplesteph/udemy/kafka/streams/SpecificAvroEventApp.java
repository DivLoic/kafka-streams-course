package com.github.simplesteph.udemy.kafka.streams;

import com.github.simplesteph.*;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SpecificAvroEventApp {

    private static Logger logger = LoggerFactory.getLogger(GenericAvroEventApp.class);

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "generic-avro-scala-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        SpecificAvroSerde<User> userSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<UserKey> userKeySerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<Purchase> purchaseSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<PurchaseKey> purchaseKeySerde = new SpecificAvroSerde<>();

        Map<String, String> schemaRegistryConf =
                Collections.singletonMap(
                        AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                        "http://localhost:8081"
                );

        userSerde.configure(schemaRegistryConf, false);
        userKeySerde.configure(schemaRegistryConf, true);
        purchaseSerde.configure(schemaRegistryConf, false);
        purchaseKeySerde.configure(schemaRegistryConf, true);

        StreamsBuilder builder = new StreamsBuilder();

        Consumed<UserKey, User> userConsumed = Consumed.with(userKeySerde, userSerde);
        Consumed<PurchaseKey, Purchase> purchaseConsumed = Consumed.with(purchaseKeySerde, purchaseSerde);

        GlobalKTable<UserKey, User> usersGlobalTable = builder.globalTable("user-avro-table", userConsumed);

        KStream<PurchaseKey, Purchase> userPurchases = builder.stream("user-avro-purchases", purchaseConsumed);

        KStream<PurchaseKey, SalesDescription> userPurchasesEnrichedJoin = userPurchases.join(
                usersGlobalTable,
                (key, purchase) -> key.getUserKey(),
                (userPurchase, userInfo) -> new SalesDescription(
                        userPurchase.getGame(),
                        userPurchase.getIsTwoPlayer(),
                        userInfo.getLogin(),
                        userInfo.getFirstName(),
                        userInfo.getLastName(),
                        userInfo.getEmail()
                )
        );

        userPurchasesEnrichedJoin.to("specific-avro-purchases-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        logger.info(builder.build().describe().toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
