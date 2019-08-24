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

import java.util.Properties;

public class GenericAvroEventApp {

    private static Logger logger = LoggerFactory.getLogger(GenericAvroEventApp.class);

    private static Schema SalesDescriptionSchema = new Schema.Parser()
            .parse(GenericAvroEventApp.class.getResource("avro/sales-description.avsc").getPath());

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "generic-avro-scala-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://127.0.0.1:8081");

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<GenericRecord, GenericRecord> usersGlobalTable = builder.globalTable("user-table-avro");

        KStream<GenericRecord, GenericRecord> userPurchases = builder.stream("user-purchases-avro");

        KStream<GenericRecord, GenericRecord> userPurchasesEnrichedJoin = userPurchases.join(
                usersGlobalTable,
                (key, value) -> (GenericRecord) key.get("client_id"),
                GenericAvroEventApp::createSaleDescription
        );

        userPurchasesEnrichedJoin.to("generic-avro-purchases-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        streams.cleanUp(); // only do this in dev - not in prod
        streams.start();

        // print the topology
        logger.info(builder.build().describe().toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static GenericRecord createSaleDescription(GenericRecord key, GenericRecord value) {
        GenericRecord record = new GenericData.Record(SalesDescriptionSchema);

        record.put("game", value.get("game"));
        record.put("is_two_player", value.get("is_two_player"));
        record.put("first_name", key.get("first_name"));
        record.put("last_name", key.get("last_name"));
        record.put("email", key.get("email"));
        record.put("login", key.get("login"));

        return record;
    }
}
