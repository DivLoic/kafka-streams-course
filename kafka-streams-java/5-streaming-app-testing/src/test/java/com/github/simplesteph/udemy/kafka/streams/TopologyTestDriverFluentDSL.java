package com.github.simplesteph.udemy.kafka.streams;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

public class TopologyTestDriverFluentDSL {

    Properties config;

    StreamsBuilder builder;

    @Before
    public void setUpTopologyTestDriver(){
        builder = new StreamsBuilder();
        config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    @Test
    public void characterCountOutputTest(){
    }

}
