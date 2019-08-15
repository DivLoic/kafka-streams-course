package com.github.simplesteph.udemy.kafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertNull;

public class SimpleProcessingStepTest {

    private Properties config;

    private StreamsBuilder builder;

    private TopologyTestDriver testDriver;

    private Serializer<String> stringSerializer = Serdes.String().serializer();

    ConsumerRecordFactory<String, String> recordFactory =
            new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

    @Before
    public void setUpTopologyTestDriver(){
        builder = new StreamsBuilder();
        config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    @After
    public void closeTestDriver(){
        testDriver.close();
    }

    @Test
    public void formatKeyValueTest(){
        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .map(SimpleProcessingStep.formatKeyValue)
                .to("output-topic", Produced.with(Serdes.Integer(), Serdes.String()));

        testDriver = new TopologyTestDriver(builder.build(), config);

        testDriver.pipeInput(recordFactory.create("input-topic", "0000101", "terminal-8,ryu"));

        ProducerRecord<Integer, String> output = testDriver
                .readOutput("output-topic", Serdes.Integer().deserializer(), Serdes.String().deserializer());

        OutputVerifier.compareKeyValue(output, 101, "ryu");
    }

    @Test
    public void getMachineIdTest(){
        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(SimpleProcessingStep.getMachineId)
                .to("output-topic");

        testDriver = new TopologyTestDriver(builder.build(), config);

        testDriver.pipeInput(recordFactory.create("input-topic", "notused", "terminal-9,ryu"));

        ProducerRecord<String, String> output = testDriver
                .readOutput("output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

        OutputVerifier.compareKeyValue(output, "notused", "9");
    }

    @Test
    public void isTopCharacterTest(){
        builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()))
                .filter(SimpleProcessingStep.isTopCharacter)
                .to("output-topic");

        testDriver = new TopologyTestDriver(builder.build(), config);

        testDriver.pipeInput(recordFactory.create("input-topic", "0000101", "ryu"));
        testDriver.pipeInput(recordFactory.create("input-topic", "0000102", "jin"));
        testDriver.pipeInput(recordFactory.create("input-topic", "0000103", "blair"));
        testDriver.pipeInput(recordFactory.create("input-topic", "0000104", "chunli"));

        ProducerRecord<String, String> output1 = testDriver
                .readOutput("output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());
        ProducerRecord<String, String> output2 = testDriver
                .readOutput("output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());
        ProducerRecord<String, String> output3 = testDriver
                .readOutput("output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());
        ProducerRecord<String, String> output4 = testDriver
                .readOutput("output-topic", Serdes.String().deserializer(), Serdes.String().deserializer());

        OutputVerifier.compareKeyValue(output1, "0000101", "ryu");
        OutputVerifier.compareKeyValue(output2, "0000104", "chunli");

        assertNull(output3);
        assertNull(output4);
    }
}
