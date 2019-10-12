package com.github.simplesteph.udemy.kafka.streams;


import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class GenericAvroEventApp {

    private static Logger logger = LoggerFactory.getLogger(GenericAvroEventApp.class);

    private static Schema salesDescriptionSchema() {
        Schema schema = null;
        try {
            String path = "/avro/sales-description.avsc";
            File resource = new File(GenericAvroEventApp.class.getResource(path).getPath());
            schema = new Schema.Parser().parse(resource);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "generic-avro-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");


        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<GenericRecord, GenericRecord> usersGlobalTable = builder.globalTable("avro-user-table");

        KStream<GenericRecord, GenericRecord> userPurchases = builder.stream("avro-user-purchases");

        KStream<GenericRecord, GenericRecord> userPurchasesEnrichedJoin = userPurchases.join(
                usersGlobalTable,
                (key, value) -> (GenericRecord) key.get("user_key"),
                GenericAvroEventApp::createSaleDescription
        );

        userPurchasesEnrichedJoin.to("generic-avro-purchases");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        logger.info(builder.build().describe().toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static GenericRecord createSaleDescription(GenericRecord purchase, GenericRecord user) {
        GenericRecord record = new GenericData.Record(salesDescriptionSchema());

        record.put("game", purchase.get("game"));
        record.put("is_two_player", purchase.get("is_two_player"));
        record.put("first_name", user.get("first_name"));
        record.put("last_name", user.get("last_name"));
        record.put("email", user.get("email"));
        record.put("login", user.get("login"));

        return record;
    }
}
