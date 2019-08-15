package com.github.simplesteph.udemy.kafka.streams;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.internals.KeyValueStoreFacade;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CompleteTopologyTest {

    Properties config;

    StreamsBuilder builder;

    TopologyTestDriver testDriver;

    Serializer<String> stringSerializer = Serdes.String().serializer();

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
    public void characterCountOutputTest(){
        testDriver = new TopologyTestDriver(CompleteTopology.createTopology(), config);

        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000101", "terminal-8,Ryu"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000102", "terminal-5,Jin"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000103", "terminal-1,Ryu"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000104", "terminal-3,Ken"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000105", "terminal-7,Ryu"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000106", "terminal-2,Chunli"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000107", "terminal-2,Alen"));


        ProducerRecord<String, Long> output1 = testDriver
                .readOutput("favourite-player-output", Serdes.String().deserializer(), Serdes.Long().deserializer());
        ProducerRecord<String, Long> output2 = testDriver
                .readOutput("favourite-player-output", Serdes.String().deserializer(), Serdes.Long().deserializer());
        ProducerRecord<String, Long> output3 = testDriver
                .readOutput("favourite-player-output", Serdes.String().deserializer(), Serdes.Long().deserializer());
        ProducerRecord<String, Long> output4 = testDriver
                .readOutput("favourite-player-output", Serdes.String().deserializer(), Serdes.Long().deserializer());
        ProducerRecord<String, Long> output5 = testDriver
                .readOutput("favourite-player-output", Serdes.String().deserializer(), Serdes.Long().deserializer());

        ProducerRecord<String, Long> output6 = testDriver
                .readOutput("favourite-player-output", Serdes.String().deserializer(), Serdes.Long().deserializer());

        OutputVerifier.compareKeyValue(output1, "ryu", 1L);
        OutputVerifier.compareKeyValue(output2, "ryu", 2L);
        OutputVerifier.compareKeyValue(output3, "ken", 1L);
        OutputVerifier.compareKeyValue(output4, "ryu", 3L);
        OutputVerifier.compareKeyValue(output5, "chunli", 1L);

        assertNull(output6);
    }

    @Test
    public void characterCountStateTest(){
        testDriver = new TopologyTestDriver(CompleteTopology.createTopology(), config);

        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000101", "terminal-8,Ryu"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000102", "terminal-5,Jin"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000103", "terminal-1,Ryu"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000104", "terminal-3,Ken"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000105", "terminal-7,Ryu"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000106", "terminal-2,Chunli"));
        testDriver.pipeInput(recordFactory.create("favourite-player-input", "0000107", "terminal-2,Alen"));

        ReadOnlyKeyValueStore<String, Long> counts = testDriver.getKeyValueStore("CountsByPlayers");

        assertEquals(3L, counts.get("ryu").longValue());

        assertEquals(1L, counts.get("ken").longValue());

        assertEquals(1L, counts.get("chunli").longValue());

        assertNull(counts.get("alen"));

        assertNull(counts.get("jin"));
    }
}
